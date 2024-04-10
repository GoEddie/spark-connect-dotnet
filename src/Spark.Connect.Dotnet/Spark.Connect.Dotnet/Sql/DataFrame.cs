using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql;

public class DataFrame
{
    private readonly SparkSession _session;
    protected internal readonly Relation Relation;
    private readonly DataType? _schema;

    public DataFrame(SparkSession session, Relation relation, DataType? schema)
    {
        _session = session;
        Relation = relation;
        _schema = schema;
    }
    
    protected internal DataFrame(SparkSession session, Relation relation)
    {
        _session = session;
        Relation = relation;
    }

    /// <summary>
    /// Returns the `SparkColumn` denoted by name.
    /// </summary>
    /// <param name="name"></param>
    public SparkColumn this[string name] => new(name);

    /// <summary>
    /// Returns a new `DataFrame` by taking the first n rows.
    /// </summary>
    /// <param name="num">The number of rows to limit the `DataFrame` to.</param>
    /// <returns>`DataFrame` truncated to the number of rows specified in `num`.</returns>
    public DataFrame Limit(int num)
    {
        var limitPlan = new Plan
        {
            Root = new Relation
            {
                Limit = new Limit
                {
                    Limit_ = num, Input = Relation
                }
            }, 
        };

        return new DataFrame(_session, limitPlan.Root, _schema);
    }

    /// <summary>
    /// Prints the first `numberOfRows` rows to the console.
    /// </summary>
    /// <param name="numberOfRows">The number of rows to show</param>
    /// <param name="truncate">If set greater than one, truncates long strings to length truncate and align cells right.</param>
    /// <param name="vertical">Print output rows vertically (one line per column value).</param>
    public void Show(int numberOfRows = 10, int truncate = 20, bool vertical = false)
    {
        var result = Task.Run(() => ShowAsync(Relation, numberOfRows, truncate, vertical, _session));
        Wait(result);
    }
    
    /// <summary>
    /// Prints the first `numberOfRows` rows to the console.
    /// </summary>
    /// <param name="input">`DataFrame` to show</param>
    /// <param name="numberOfRows">The number of rows to show</param>
    /// <param name="truncate">If set greater than one, truncates long strings to length truncate and align cells right.</param>
    /// <param name="vertical">Print output rows vertically (one line per column value).</param>
    /// <param name="sessionId">SessionId to make the call on</param>
    /// <param name="client">`SparkConnectServiceClient` client, must already be connected</param>
    public static async Task ShowAsync(Relation input, int numberOfRows, int truncate, bool vertical, SparkSession session)
    {
        var showStringPlan = new Plan
        {
            Root = new Relation
            {
                ShowString = new ShowString
                {
                    Truncate = truncate, Input = input, NumRows = numberOfRows, Vertical = vertical
                }
            }
        };
        
        await GrpcInternal.Exec(session.Client, session.Host, session.SessionId, showStringPlan, session.Headers, session.UserContext, session.ClientType);
    }

    /// <summary>
    /// Projects a set of expressions and returns a new `DataFrame`.
    /// </summary>
    /// <param name="columns">Column Expressions (Column). If one of the column names is ‘*’, that column is expanded to include all columns in the current DataFrame.</param>
    /// <returns></returns>
    public DataFrame Select(params SparkColumn[] columns)
    {
        var relation = new Relation()
        {
            Project = new Project()
            {
                Input = Relation, Expressions =
                {
                    columns.Select(p => p.Expression)
                }
            }
        };

        return new DataFrame(_session, relation, _schema);
    }
    
    public DataFrame Select(params Expression[] columns)
    {
        var relation = new Relation()
        {
            Project = new Project()
            {
                Input = Relation, Expressions =
                {
                    columns.Select(p => p)
                }
            }
        };

        return new DataFrame(_session, relation, _schema);
    }

    /// <summary>
    /// Projects a set of expressions and returns a new `DataFrame`.
    /// </summary>
    /// <param name="columns">If one of the column names is ‘*’, that column is expanded to include all columns in the current DataFrame.</param>
    /// <returns></returns>
    public DataFrame Select(params string[] columns)
    {
        var relation = new Relation()
        {
            Project = new Project()
            {
                Input = Relation, Expressions =
                {
                    columns.Select(p => new Expression()
                    {
                        Literal = new Expression.Types.Literal()
                        {
                            String = p
                        }
                    })
                }
            }
        };

        return new DataFrame(_session, relation, _schema);
    }

    /// <summary>
    /// Returns a new `DataFrame` by adding a column or replacing the existing column that has the same name.
    /// The column expression must be an expression over this `DataFrame`; attempting to add a column from some other `DataFrame` will raise an error.
    /// </summary>
    /// <param name="columnName">string, name of the new column.</param>
    /// <param name="column">a `SparkColumn` expression for the new column.</param>
    /// <returns></returns>
    public DataFrame WithColumn(string columnName, SparkColumn column)
    {
        var alias = new Expression.Types.Alias()
        {
            Expr = column.Expression, Name = { columnName }
        };
        
        var relation = new Relation
        {
            WithColumns = new WithColumns()
        };
        
        relation.WithColumns.Aliases.Add(alias);
        relation.WithColumns.Input = Relation;
            
        return new DataFrame(_session, relation, _schema);
    }

    public DataFrame WithColumn(string columnName, Expression column)
    {
        var alias = new Expression.Types.Alias()
        {
            Expr = column, Name = { columnName }
        };
        
        var relation = new Relation
        {
            WithColumns = new WithColumns()
        };
        
        relation.WithColumns.Aliases.Add(alias);
        relation.WithColumns.Input = Relation;
            
        return new DataFrame(_session, relation, _schema);
    }

    public DataFrameWriter Write() => new(_session, this);

    public List<object[]> Collect()
    {
        var task = Task.Run(() => CollectAsync());
        Wait(task);
        return task.Result;
    }

    public async Task<List<object[]>> CollectAsync()
    {
        var plan = new Plan()
        {
            Root = Relation
        };
        
        var (batches, schema) =  await GrpcInternal.ExecArrowResponse(_session.Client, _session.SessionId, plan, _session.Headers, _session.UserContext, _session.ClientType);
        var rows = new List<object[]>();
        
        foreach (var batch in batches)
        {
            rows.AddRange(await ArrowBatchToList(batch));
        }
        
        return rows;
    }

    private static async Task<List<object[]>> ArrowBatchToList(ExecutePlanResponse.Types.ArrowBatch batch)
    {
        var rows = new List<object[]>();
        
        var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(batch.Data.ToByteArray()));
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        
        Logger.WriteLine("arrow: Read record batch with {0} column(s)", recordBatch.ColumnCount);

        for (int i = 0; i < recordBatch.Column(0).Length; i++)
        {
            rows.Add(new object[reader.Schema.FieldsList.Count]);
        }

        for (var i = 0; i < recordBatch.ColumnCount; i++)
        {
            switch (reader.Schema.FieldsList[i].DataType)
            {
                case BinaryType binaryType:
                    break;
                case BooleanType booleanType:
                    var b = (BooleanArray)recordBatch.Column(i);
                    for (int ii = 0; ii < b.Length; ii++)
                    {
                        rows[ii][i] = b.GetValue(ii)!;
                    }

                    break;
                case Date32Type date32Type:
                    break;
                case Date64Type date64Type:
                    break;
                case DateType dateType:
                    break;
                case Decimal128Type decimal128Type:
                    break;
                case Decimal256Type decimal256Type:
                    break;
                case DictionaryType dictionaryType:
                    break;
                case DoubleType doubleType:
                    var d = (DoubleArray)recordBatch.Column(i);
                    for (int ii = 0; ii < d.Length; ii++)
                    {
                        rows[ii][i] = d.GetValue(ii)!;
                    }

                    break;
                case FixedSizeBinaryType fixedSizeBinaryType:
                    break;
                case FloatType floatType:
                    break;
                case HalfFloatType halfFloatType:
                    break;
                case FloatingPointType floatingPointType:
                    break;
                case Int16Type int16Type:
                    break;
                case Int32Type int32Type:
                    break;
                case Int64Type int64Type:
                    var i64 = (Int64Array)recordBatch.Column(i);
                    for (int ii = 0; ii < i64.Length; ii++)
                    {
                        rows[ii][i] = i64.GetValue(ii)!;
                    }

                    break;
                case Int8Type int8Type:
                    break;
                case UInt16Type uInt16Type:
                    break;
                case UInt32Type uInt32Type:
                    break;
                case UInt64Type uInt64Type:
                    break;
                case UInt8Type uInt8Type:
                    break;
                case IntegerType integerType:
                    break;
                case IntervalType intervalType:
                    break;
                case NumberType numberType:
                    break;
                case Time32Type time32Type:
                    break;
                case Time64Type time64Type:
                    break;
                case TimestampType timestampType:
                    break;
                case TimeType timeType:
                    break;
                case FixedWidthType fixedWidthType:
                    break;
                case ListType listType:
                    break;
                case StructType structType:
                    break;
                case NestedType nestedType:
                    break;
                case NullType nullType:
                    break;
                case StringType stringType:
                    var s = (StringArray)recordBatch.Column(i);
                    for (int ii = 0; ii < s.Length; ii++)
                    {
                        rows[ii][i] = s.GetString(i);
                    }

                    break;
                case UnionType unionType:
                    break;
                case ArrowType arrowType:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        return rows;
    }

    public void CreateOrReplaceTempView(string name)
    {
        var task = Task.Run(async () => await CreateDataFrameViewCommand(name, true, false));
        Wait(task);
    }
    
    public void CreateTempView(string name)
    {
        var task = Task.Run(async () => await CreateDataFrameViewCommand(name, false, false));
        Wait(task);
    }
    
    public void CreateOrReplaceGlobalTempView(string name)
    {
        var task = Task.Run(async () => await CreateDataFrameViewCommand(name, true, true));
        Wait(task);
    }
    
    public void CreateGlobalTempView(string name)
    {
        var task = Task.Run(async () => await CreateDataFrameViewCommand(name, false, true));
        Wait(task);
    }
    
    private async Task CreateDataFrameViewCommand(string name, bool replace, bool global)
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                CreateDataframeView = new CreateDataFrameViewCommand()
                {
                    Input = Relation,
                    Name = name,
                    Replace = replace,
                    IsGlobal = global
                }
            }
        };
        
        await GrpcInternal.Exec(_session.Client,  _session.Host, _session.SessionId, plan, _session.Headers, _session.UserContext, _session.ClientType);
    }

    public DataFrame Union(DataFrame other)
    {
        var relation = new Relation()
        {
            SetOp = new SetOperation()
            {
                AllowMissingColumns = false,
                ByName = false,
                IsAll = false,
                LeftInput = Relation,
                RightInput = other.Relation,
                SetOpType = SetOperation.Types.SetOpType.Union
            }
        };

        return new DataFrame(_session, relation);
    }
    
    public DataFrame UnionAll(DataFrame other)
    {
        var relation = new Relation()
        {
            SetOp = new SetOperation()
            {
                AllowMissingColumns = false,
                ByName = false,
                IsAll = true,
                LeftInput = Relation,
                RightInput = other.Relation,
                SetOpType = SetOperation.Types.SetOpType.Union
            }
        };

        return new DataFrame(_session, relation);
    }
    
    public DataFrame UnionByName(DataFrame other, bool allowMissingColumns)
    {
        var relation = new Relation()
        {
            SetOp = new SetOperation()
            {
                AllowMissingColumns = allowMissingColumns,
                ByName = true,
                IsAll = false,
                LeftInput = Relation,
                RightInput = other.Relation,
                SetOpType = SetOperation.Types.SetOpType.Union
            }
        };

        return new DataFrame(_session, relation);
    }

    public GroupedData GroupBy(params SparkColumn[] cols)
    {
        var relation = new Relation()
        {
            GroupMap = new GroupMap()
            {
                Input = Relation,
                GroupingExpressions = { cols.Select(p => p.Expression) }
            }
        };

        return new GroupedData(_session, relation);
    }

    public DataFrame Agg(SparkColumn exprs)
    {
        var relation = new Relation()
        {
            Aggregate = new Aggregate()
            {
                Input = Relation,
                GroupType = Aggregate.Types.GroupType.Groupby,
                GroupingExpressions = { exprs.Expression },
                AggregateExpressions = {  }
            }
        };

        return new DataFrame(_session, relation);
    }
    
    public long Count()
    {
        var result = this.Select(Functions.FunctionCall("count", Functions.Lit(1))).Collect();
        return (long)result[0][0];
    }

    public DataFrame OrderBy(params SparkColumn[] columns)
    {
        var sortColumns = new List<Expression.Types.SortOrder>();
        
        foreach (var column in columns)
        {
            switch (column.Expression.UnresolvedFunction.FunctionName)
            {
                case "asc":
                    sortColumns.Add(new Expression.Types.SortOrder()
                    {
                        Child = column.Expression.UnresolvedFunction.Arguments.First(),
                        Direction = Expression.Types.SortOrder.Types.SortDirection.Ascending,
                        NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsUnspecified
                    });
                    break;
                case "desc":
                    sortColumns.Add(new Expression.Types.SortOrder()
                    {
                        Child = column.Expression.UnresolvedFunction.Arguments.First(),
                        Direction = Expression.Types.SortOrder.Types.SortDirection.Descending,
                        NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsUnspecified
                    });
                    break;
                case "asc_nulls_last":
                    sortColumns.Add(new Expression.Types.SortOrder()
                    {
                        Child = column.Expression.UnresolvedFunction.Arguments.First(),
                        Direction = Expression.Types.SortOrder.Types.SortDirection.Ascending,
                        NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsLast
                    });
                    break;
                case "asc_nulls_first":
                    sortColumns.Add(new Expression.Types.SortOrder()
                    {
                        Child = column.Expression.UnresolvedFunction.Arguments.First(),
                        Direction = Expression.Types.SortOrder.Types.SortDirection.Ascending,
                        NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsFirst
                    });
                    break;
                case "desc_nulls_last":
                    sortColumns.Add(new Expression.Types.SortOrder()
                    {
                        Child = column.Expression.UnresolvedFunction.Arguments.First(),
                        Direction = Expression.Types.SortOrder.Types.SortDirection.Descending,
                        NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsLast
                    });
                    break;
                case "desc_nulls_first":
                    sortColumns.Add(new Expression.Types.SortOrder()
                    {
                        Child = column.Expression.UnresolvedFunction.Arguments.First(),
                        Direction = Expression.Types.SortOrder.Types.SortDirection.Descending,
                        NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsFirst
                    });
                    break;
            }
        }
        var relation = new Relation()
        {
            Sort = new Sort()
            {
                Input = Relation,
                Order = { sortColumns },
                IsGlobal = false
            }
        };

        return new DataFrame(_session, relation);
    }
    public DataFrame OrderBy(List<SparkColumn> columns)
    {
        return OrderBy(columns.ToArray());
    }

    public string Explain(bool extended = false, string mode = null, bool outputToConsole = true)
    {
        var plan = new Plan()
        {
            Root = Relation
        };
        var output = GrpcInternal.Explain(_session.Client, _session.SessionId, plan, _session.Headers, _session.UserContext, _session.ClientType, extended, mode);

        if (outputToConsole)
        {
            Console.WriteLine(output);
        }

        return output;
    }

    private static Task Wait(Task on)
    {
        try
        {
            on.Wait();
            return on;
        }
        catch (AggregateException aggregate)
        {
            throw SparkExceptionFactory.GetExceptionFromRpcException(aggregate);
        }
    }
}