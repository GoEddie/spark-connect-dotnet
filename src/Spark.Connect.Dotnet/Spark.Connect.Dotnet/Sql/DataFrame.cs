using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Grpc.SparkExceptions;
using Spark.Connect.Dotnet.Sql.Streaming;
using StructType = Spark.Connect.Dotnet.Sql.Types.StructType;
using static Spark.Connect.Dotnet.Sql.Functions;
using TransformFunction = System.Linq.Expressions.Expression<System.Func<Spark.Connect.Dotnet.Sql.DataFrame, Spark.Connect.Dotnet.Sql.DataFrame>>;

namespace Spark.Connect.Dotnet.Sql;

public class DataFrame
{
    private readonly DataType? _schema;
    public readonly Relation Relation;

    private readonly Dictionary<SparkStorageLevel, StorageLevel> _storageLevels =
        new()
        {
            {
                SparkStorageLevel.None, new StorageLevel
                {
                    UseDisk = false, Deserialized = false, UseMemory = false, UseOffHeap = false
                }
            }
            ,
            {
                SparkStorageLevel.DISK_ONLY, new StorageLevel
                {
                    UseDisk = true, Deserialized = false, UseMemory = false, UseOffHeap = false
                }
            }
            ,
            {
                SparkStorageLevel.DISK_ONLY_2, new StorageLevel
                {
                    UseDisk = true, Deserialized = false, UseMemory = false, UseOffHeap = false, Replication = 2
                }
            }
            ,
            {
                SparkStorageLevel.DISK_ONLY_3, new StorageLevel
                {
                    UseDisk = true, Deserialized = false, UseMemory = false, UseOffHeap = false, Replication = 3
                }
            }
            ,
            {
                SparkStorageLevel.MEMORY_ONLY, new StorageLevel
                {
                    UseDisk = false, Deserialized = false, UseMemory = true, UseOffHeap = false
                }
            }
            ,
            {
                SparkStorageLevel.MEMORY_ONLY_2, new StorageLevel
                {
                    UseDisk = false, Deserialized = false, UseMemory = true, UseOffHeap = false, Replication = 2
                }
            }
            ,
            {
                SparkStorageLevel.MEMORY_AND_DISK, new StorageLevel
                {
                    UseDisk = true, Deserialized = false, UseMemory = true, UseOffHeap = false
                }
            }
            ,
            {
                SparkStorageLevel.MEMORY_AND_DISK_2, new StorageLevel
                {
                    UseDisk = true, Deserialized = false, UseMemory = true, UseOffHeap = false, Replication = 2
                }
            }
            ,
            {
                SparkStorageLevel.OFF_HEAP, new StorageLevel
                {
                    UseDisk = true, Deserialized = false, UseMemory = true, UseOffHeap = true
                }
            }
            ,
            {
                SparkStorageLevel.MEMORY_AND_DISK_DESER, new StorageLevel
                {
                    UseDisk = true, Deserialized = true, UseMemory = true, UseOffHeap = false
                }
            }
        };

    public DataFrame(SparkSession session, Relation relation, DataType? schema)
    {
        SparkSession = session;
        Relation = relation;

        if (Relation.Common == null)
        {
            Relation.Common = new RelationCommon
            {
                PlanId = SparkSession.GetPlanId()
            };
        }

        _schema = schema;
    }

    public DataFrame(SparkSession session, Relation relation)
    {
        SparkSession = session;
        Relation = relation;

        if (Relation.Common == null)
        {
            Relation.Common = new RelationCommon
            {
                PlanId = SparkSession.GetPlanId()
            };
        }
    }

    /// <summary>
    ///     Returns the `Column` denoted by name.
    /// </summary>
    /// <param name="name"></param>
    public Column this[string name]
    {
        get
        {
            if (!ValidateThisCallColumnName &&
                !SparkSession.Conf.IsTrue(RuntimeConf.SparkDotnetConfigKey + "validatethiscallcolumnname"))
            {
                return new Column(name, this);
            }

            var schema = Schema;
            if (schema.Fields.All(p => !string.Equals(p.Name, name, StringComparison.InvariantCultureIgnoreCase)))
            {
                throw new SparkException(
                    $"The field '{name}' was not found in the schema: '{schema.SimpleString()}', DataFrame[\"name\"] failed validation");
            }

            return new Column(name, this);
        }
    }

    public bool ValidateThisCallColumnName { get; set; }

    public StructType Schema
    {
        get
        {
            var plan = new Plan
            {
                Root = Relation
            };

            if (plan.Root.Common == null)
            {
                plan.Root.Common = new RelationCommon()
                {
                    PlanId = SparkSession.GetPlanId()
                };
            }

            var explain = GrpcInternal.Schema(SparkSession.GrpcClient, SparkSession.SessionId, plan,
                SparkSession.Headers,
                SparkSession.UserContext, SparkSession.ClientType, false, "");
            var structType = new StructType(explain.Struct);
            return structType;
        }
    }

    public IEnumerable<string> Columns => Schema.FieldNames();

    public IEnumerable<(string Name, string Type)> Dtypes =>
        Schema.Fields.Select(p => (p.Name, p.DataType.SimpleString()));

    public DataFrameNaFunctions Na => new(this);

    public SparkSession SparkSession { get; }

    /// <summary>
    ///     Returns a new `DataFrame` by taking the first n rows.
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
            }
        };

        if (limitPlan.Root.Common == null)
        {
            limitPlan.Root.Common = new RelationCommon()
            {
                PlanId = SparkSession.GetPlanId()
            };
        }

        return new DataFrame(SparkSession, limitPlan.Root, _schema);
    }

    /// <summary>
    ///     Prints the first `numberOfRows` rows to the console.
    /// </summary>
    /// <param name="numberOfRows">The number of rows to show</param>
    /// <param name="truncate">If set greater than one, truncates long strings to length truncate and align cells right.</param>
    /// <param name="vertical">Print output rows vertically (one line per column value).</param>
    public void Show(int numberOfRows = 10, int truncate = 20, bool vertical = false)
    {
        var result = Task.Run(() => ShowAsync(Relation, numberOfRows, truncate, vertical, SparkSession));
        Wait(result);
    }

    /// <summary>
    ///     Prints the first `numberOfRows` rows to the console.
    /// </summary>
    /// <param name="input">`DataFrame` to show</param>
    /// <param name="numberOfRows">The number of rows to show</param>
    /// <param name="truncate">If set greater than one, truncates long strings to length truncate and align cells right.</param>
    /// <param name="vertical">Print output rows vertically (one line per column value).</param>
    /// <param name="session">Spark Session to make the call on</param>
    public static async Task ShowAsync(Relation input, int numberOfRows, int truncate, bool vertical, SparkSession session)
    {
        var showStringPlan = new Plan
        {
            Root = new Relation
            {
                Common = new RelationCommon()
                {
                    PlanId = session.GetPlanId()
                },
                ShowString = new ShowString
                {
                    Truncate = truncate, Input = input, NumRows = numberOfRows, Vertical = vertical
                }
            },
        };
        
        var executor = new RequestExecutor(session, showStringPlan);
        await executor.ExecAsync();

        var rows = executor.GetData();
        session.Console.WriteLine(rows[0].Data[0] as string ?? "No data in row");
    }

    public void PrintSchema(int? level = null)
    {
        SparkSession.Console.WriteLine(GrpcInternal.TreeString(SparkSession, Relation));
    }

    /// <summary>
    ///     Projects a set of expressions and returns a new `DataFrame`.
    /// </summary>
    /// <param name="columns">
    ///     Column Expressions (Column). If one of the column names is ‘*’, that column is expanded to
    ///     include all columns in the current DataFrame.
    /// </param>
    /// <returns></returns>
    public DataFrame Select(params Column[] columns)
    {
        var relation = new Relation
        {
            Project = new Project
            {
                Input = Relation, Expressions =
                {
                    columns.Select(p => p.Expression)
                }
            }
        };

        RunAnalyze(relation);

        return new DataFrame(SparkSession, relation, _schema);
    }

    private void RunAnalyze(Relation relation)
    {
        var plan = new Plan
        {
            Root = relation,
        };

        if (plan.Root.Common == null)
        {
            plan.Root.Common = new RelationCommon()
            {
                PlanId = SparkSession.GetPlanId()
            };
        } 

        GrpcInternal.Schema(SparkSession.GrpcClient, SparkSession.SessionId, plan, SparkSession.Headers, SparkSession.UserContext, SparkSession.ClientType, false, "");
    }

    public DataFrame Select(params Expression[] columns)
    {
        var relation = new Relation
        {
            Project = new Project
            {
                Input = Relation, Expressions =
                {
                    columns.Select(p => p)
                }
            }
        };

        RunAnalyze(relation);

        return new DataFrame(SparkSession, relation, _schema);
    }

    /// <summary>
    ///     Projects a set of expressions and returns a new `DataFrame`.
    /// </summary>
    /// <param name="columns">
    ///     If one of the column names is ‘*’, that column is expanded to include all columns in the current
    ///     DataFrame.
    /// </param>
    /// <returns></returns>
    public DataFrame Select(params string[] columns)
    {
        Expression ColumnNameToExpression(string p)
        {
            if (p == "*")
            {
                return new Expression
                {
                    UnresolvedStar = new Expression.Types.UnresolvedStar()
                };
            }

            return new Expression
            {
                UnresolvedAttribute = new Expression.Types.UnresolvedAttribute
                {
                    UnparsedIdentifier = p
                }
            };
        }

        var relation = new Relation
        {
            Project = new Project
            {
                Input = Relation, Expressions =
                {
                    columns.Select(p => ColumnNameToExpression(p))
                }
            }
        };

        RunAnalyze(relation);

        return new DataFrame(SparkSession, relation, _schema);
    }

    public DataFrame Alias(string alias)
    {
        var newRelation = new Relation
        {
            SubqueryAlias = new SubqueryAlias
            {
                Input = Relation, Alias = alias
            }
        };

        return new DataFrame(SparkSession, newRelation);
    }

    public DataFrame Cache()
    {
        return new DataFrame(SparkSession,
            GrpcInternal.Persist(SparkSession, Relation, new StorageLevel
            {
                UseMemory = true, UseDisk = true, UseOffHeap = false, Deserialized = true, Replication = 1
            }));
    }

    public DataFrame Persist()
    {
        return Persist(SparkStorageLevel.MEMORY_AND_DISK);
    }

    public DataFrame Persist(SparkStorageLevel storageLevel)
    {
        return new DataFrame(SparkSession, GrpcInternal.Persist(SparkSession, Relation, _storageLevels[storageLevel]));
    }

    public DataFrame Checkpoint()
    {
        throw new NotImplementedException("Not yet implemented in Apache Spark Connect");
    }

    public DataFrame Describe(params string[] cols)
    {
        var relation = new Relation
            {
                Describe = new StatDescribe
                {
                    Input = Relation, Cols = { cols }
                }
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame Distinct(bool withinWatermark = false)
    {
        var relation = new Relation
            {
                Deduplicate = new Deduplicate
                {
                    Input = Relation, AllColumnsAsKeys = true, WithinWatermark = withinWatermark
                }
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame Drop(params string[] cols)
    {
       var relation = new Relation
            {
                Drop = new Drop
                {
                    Input = Relation, ColumnNames = { cols }
                }
            };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame DropDuplicates(params string[] subset)
    {
        if (subset.Length == 0)
        {
            return Distinct();
        }

        var relation = new Relation
            {
                Deduplicate = new Deduplicate
                {
                    Input = Relation, ColumnNames = { subset }
                }
            
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame DropDuplicatesWithinWatermark(params string[] subset)
    {
        if (subset.Length == 0)
        {
            return Distinct(true);
        }

        var relation = new Relation
            {
                Deduplicate = new Deduplicate
                {
                    Input = Relation, ColumnNames = { subset }, WithinWatermark = true
                }
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame Drop(params Column[] cols)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Drop = new Drop
                {
                    Input = Relation, Columns = { cols.Select(p => p.Expression) }
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame DropNa(string how, int? thresh, params string[] subset)
    {
        var naDrop = new NADrop
        {
            Input = Relation, Cols = { subset }
        };

        if (how == "all")
        {
            naDrop.MinNonNulls = 1;
        }

        if (thresh.HasValue)
        {
            naDrop.MinNonNulls = thresh.Value;
        }

        var relation = new Relation
        {
                DropNa = naDrop
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame Coalesce()
    {
        return Coalesce(1);
    }

    public DataFrame Coalesce(int numPartitions)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Repartition = new Repartition
                {
                    Input = Relation, NumPartitions = numPartitions
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Repartition(int numPartitions, params Column[] cols)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                RepartitionByExpression = new RepartitionByExpression
                {
                    Input = Relation, NumPartitions = numPartitions, PartitionExprs =
                    {
                        cols.Select(p => p.Expression)
                    }
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Repartition(params Column[] cols)
    {
        return Repartition(1, cols);
    }

    public DataFrame RepartitionByRange(int numPartitions, params Column[] cols)
    {
        Column WrapSortOrderCols(Column column)
        {
            // if (column.Expression.SortOrder == null)
            // {
            //     column.Expression.SortOrder = new Expression.Types.SortOrder
            //     {
            //         // Child = column.Expression, 
            //         Direction = Expression.Types.SortOrder.Types.SortDirection.Unspecified, 
            //         NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsUnspecified
            //     };
            // }
            
            return column;
        }

        var sort = cols.Select(WrapSortOrderCols);
        var relation = new Relation
            {
                RepartitionByExpression = new RepartitionByExpression
                {
                    NumPartitions = numPartitions, Input = Relation, PartitionExprs = { sort.Select(p => p.Expression) }
                }
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame SortWithinPartitions(params string[] cols)
    {
        var relation = new Relation
            {
                Sort = new Sort
                {
                    IsGlobal = false, Input = Relation, Order = { ColumnsToSortOrder(cols.Select(Col).ToArray()) }
                }
        };

        return new DataFrame(SparkSession, relation);
    }

    public Column ColRegex(string regex)
    {
        var expression = new Expression
        {
            UnresolvedRegex = new Expression.Types.UnresolvedRegex
            {
                ColName = regex
            }
        };

        return new Column(expression);
    }


    /// <summary>
    ///     Returns a new `DataFrame` by adding a column or replacing the existing column that has the same name.
    ///     The column expression must be an expression over this `DataFrame`; attempting to add a column from some other
    ///     `DataFrame` will raise an error.
    /// </summary>
    /// <param name="columnName">string, name of the new column.</param>
    /// <param name="column">a `Column` expression for the new column.</param>
    /// <returns></returns>
    public DataFrame WithColumn(string columnName, Column column)
    {
        var alias = new Expression.Types.Alias
        {
            Expr = column.Expression, Name = { columnName }
        };

        var relation = new Relation
        {
            WithColumns = new WithColumns()
        };

        relation.WithColumns.Aliases.Add(alias);
        relation.WithColumns.Input = Relation;

        return new DataFrame(SparkSession, relation, _schema);
    }

    public DataFrame WithColumn(string columnName, Expression column)
    {
        var alias = new Expression.Types.Alias
        {
            Expr = column, Name = { columnName }
        };

        var relation = new Relation
        {
            WithColumns = new WithColumns()
        };

        relation.WithColumns.Aliases.Add(alias);
        relation.WithColumns.Input = Relation;

        return new DataFrame(SparkSession, relation, _schema);
    }

    public DataFrame WithColumns(IDictionary<string, Column> colsMap)
    {
        var df = this;
        foreach (var colMap in colsMap.Keys)
        {
            df = df.WithColumn(colMap, colsMap[colMap]);
        }

        return df;
    }

    public DataFrame WithColumnsRenamed(IDictionary<string, string> colsMap)
    {
        var df = this;
        foreach (var colMap in colsMap.Keys)
        {
            df = df.WithColumnRenamed(colMap, colsMap[colMap]);
        }

        return df;
    }

    public DataFrame WithColumnRenamed(string existing, string newName)
    {
        var  relation = new Relation
            {
                WithColumnsRenamed = new WithColumnsRenamed
                {
                    Input = Relation, RenameColumnsMap = { { existing, newName } }
                }
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrameWriter Write()
    {
        return new DataFrameWriter(SparkSession, this);
    }

    /// <summary>
    ///     Collect the data back to .NET as .NET objects, doesn't currently support List[type] or Struct[type].
    /// </summary>
    /// <returns></returns>
    public IList<Row> Collect()
    {
        var task = Task.Run(CollectAsync);
        Wait(task);
        return task.Result;
    }

    public async Task<IList<Row>> CollectAsync()
    {
        var plan = new Plan
        {
            Root = Relation
        };
        
        var executor = new RequestExecutor(SparkSession, plan);
        await executor.ExecAsync();
        return executor.GetData();
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
        var plan = new Plan
        {
            Command = new Command
            {
                CreateDataframeView = new CreateDataFrameViewCommand
                {
                    Input = Relation, Name = name, Replace = replace, IsGlobal = global
                }
            }
        };

        var executor = new RequestExecutor(SparkSession, plan);
        await executor.ExecAsync();
    }

    public DataFrame Union(DataFrame other)
    {
        var relation = new Relation
        {
            SetOp = new SetOperation
            {
                AllowMissingColumns = false, ByName = false, IsAll = false, LeftInput = Relation, RightInput = other.Relation, SetOpType = SetOperation.Types.SetOpType.Union
            }
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame UnionAll(DataFrame other)
    {
        var relation = new Relation
        {
            SetOp = new SetOperation
            {
                AllowMissingColumns = false, ByName = false, IsAll = true, LeftInput = Relation, RightInput = other.Relation, SetOpType = SetOperation.Types.SetOpType.Union
            }
        };

        return new DataFrame(SparkSession, relation);
    }

    public DataFrame UnionByName(DataFrame other, bool allowMissingColumns)
    {
        var relation = new Relation
        {
            SetOp = new SetOperation
            {
                AllowMissingColumns = allowMissingColumns, ByName = true, IsAll = false, LeftInput = Relation, RightInput = other.Relation
                , SetOpType = SetOperation.Types.SetOpType.Union
            }
        };

        return new DataFrame(SparkSession, relation);
    }

    public GroupedData GroupBy(params Column[] cols)
    {
        return new GroupedData(SparkSession, Relation, cols.Select(p => p.Expression),
            Aggregate.Types.GroupType.Groupby);
    }

    public GroupedData GroupBy(params string[] cols)
    {
        return new GroupedData(SparkSession, Relation, cols.Select(p => Col(p).Expression).ToArray(),
            Aggregate.Types.GroupType.Groupby);
    }

    public DataFrame Agg(params Column[] exprs)
    {
        var relation = new Relation
        {
            Aggregate = new Aggregate
            {
                Input = Relation, GroupType = Aggregate.Types.GroupType.Groupby, AggregateExpressions = { exprs.Select(p => p.Expression) }
            }
        };

        return new DataFrame(SparkSession, relation);
    }

    public long Count()
    {
        var result = Select(FunctionsInternal.FunctionCall("count", Lit(1))).Collect();
        return (long)result[0][0];
    }

    public DataFrame Sort(params Column[] columns)
    {
        return OrderBy(columns);
    }

    public DataFrame Sort(IEnumerable<Column> columns)
    {
        return OrderBy(columns.ToArray());
    }

    public DataFrame OrderBy(params string[] columns)
    {
        return OrderBy(columns.Select(Col).ToArray());
    }

    public DataFrame OrderBy(params Column[] columns)
    {
        var sortColumns = ColumnsToSortOrder(columns);

        var relation = new Relation
        {
            Sort = new Sort
            {
                Input = Relation, Order = { sortColumns }, IsGlobal = true
            }
        };

        return new DataFrame(SparkSession, relation);
    }

    private static IEnumerable<Expression.Types.SortOrder> ColumnsToSortOrder(Column[] columns)
    {
        var sortColumns = new List<Expression.Types.SortOrder>();

        foreach (var column in columns)
        {
            if (column.Expression.SortOrder != null)
            {
                sortColumns.Add(column.Expression.SortOrder);
            }

            if (column.Expression.UnresolvedAttribute != null)
            {
                sortColumns.Add(new Expression.Types.SortOrder
                {
                    Child = column.Expression, Direction = Expression.Types.SortOrder.Types.SortDirection.Ascending
                    , NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsLast
                });
            }

            if (column.Expression.UnresolvedFunction != null)
            {
                switch (column.Expression.UnresolvedFunction.FunctionName)
                {
                    case "asc":
                        sortColumns.Add(new Expression.Types.SortOrder
                        {
                            Child = column.Expression.UnresolvedFunction.Arguments.First(), Direction = Expression.Types.SortOrder.Types.SortDirection.Ascending
                            , NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsUnspecified
                        });
                        break;
                    case "desc":
                        sortColumns.Add(new Expression.Types.SortOrder
                        {
                            Child = column.Expression.UnresolvedFunction.Arguments.First(), Direction = Expression.Types.SortOrder.Types.SortDirection.Descending
                            , NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsUnspecified
                        });
                        break;
                    case "asc_nulls_last":
                        sortColumns.Add(new Expression.Types.SortOrder
                        {
                            Child = column.Expression.UnresolvedFunction.Arguments.First(), Direction = Expression.Types.SortOrder.Types.SortDirection.Ascending
                            , NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsLast
                        });
                        break;
                    case "asc_nulls_first":
                        sortColumns.Add(new Expression.Types.SortOrder
                        {
                            Child = column.Expression.UnresolvedFunction.Arguments.First(), Direction = Expression.Types.SortOrder.Types.SortDirection.Ascending
                            , NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsFirst
                        });
                        break;
                    case "desc_nulls_last":
                        sortColumns.Add(new Expression.Types.SortOrder
                        {
                            Child = column.Expression.UnresolvedFunction.Arguments.First(), Direction = Expression.Types.SortOrder.Types.SortDirection.Descending
                            , NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsLast
                        });
                        break;
                    case "desc_nulls_first":
                        sortColumns.Add(new Expression.Types.SortOrder
                        {
                            Child = column.Expression.UnresolvedFunction.Arguments.First(), Direction = Expression.Types.SortOrder.Types.SortDirection.Descending
                            , NullOrdering = Expression.Types.SortOrder.Types.NullOrdering.SortNullsFirst
                        });
                        break;
                }
            }
        }

        return sortColumns;
    }

    public DataFrame OrderBy(List<Column> columns)
    {
        return OrderBy(columns.ToArray());
    }

    public DataFrameWriterV2 WriteTo(string table)
    {
        return new DataFrameWriterV2(table, SparkSession, this);
    }

    public string Explain(bool extended = false, string? mode = null, bool outputToConsole = true)
    {
        var plan = new Plan
        {
            Root = Relation
        };

        var output = GrpcInternal.Explain(SparkSession.GrpcClient, SparkSession.SessionId, plan, SparkSession.Headers, SparkSession.UserContext, SparkSession.ClientType, extended, mode);

        if (outputToConsole)
        {
            SparkSession.Console.WriteLine(output);
        }

        return output;
    }

    private static void Wait(Task on)
    {
        try
        {
            on.Wait();
        }
        catch (AggregateException aggregate)
        {
            throw SparkExceptionFactory.GetExceptionFromRpcException(aggregate);
        }
    }

    public double Corr(string col1, string col2)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Corr = new StatCorr
                {
                    Input = Relation, Col1 = col1, Col2 = col2, Method = "pearson"
                }
            }
        };

        var response = new DataFrame(SparkSession, plan.Root).Collect();
        return (double)response[0][0];
    }

    public double Cov(string col1, string col2)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Cov = new StatCov
                {
                    Input = Relation, Col1 = col1, Col2 = col2
                }
            }
        };

        var response = new DataFrame(SparkSession, plan.Root).Collect();
        return (double)response[0][0];
    }

    public DataFrame CrossJoin(DataFrame other)
    {
        return Join(other, new List<string>(), JoinType.Cross);
    }

    public DataFrame Join(DataFrame other, IEnumerable<string> on, string how)
    {
        return Join(other, on, ToJoinType(how));
    }

    public DataFrame Join(DataFrame other, IEnumerable<string> on, JoinType how = JoinType.Inner)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Join = new Join
                {
                    JoinType = (Join.Types.JoinType)(int)how, Left = Relation, Right = other.Relation, UsingColumns = { on }
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Join(DataFrame other, Column on, JoinType how = JoinType.Inner)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Join = new Join
                {
                    JoinType = (Join.Types.JoinType)(int)how, Left = Relation, Right = other.Relation, JoinCondition = on.Expression
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    private JoinType ToJoinType(string type)
    {
        if (Enum.TryParse(type, true, out JoinType jt))
        {
            return jt;
        }

        throw new SparkException($"Unrecognised join type '{type}'");
    }

    public DataFrame CrossTab(string col1, string col2)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Crosstab = new StatCrosstab
                {
                    Input = Relation, Col1 = col1, Col2 = col2
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public GroupedData Cube(IEnumerable<string> cols)
    {
        return Cube(cols.Select(Col).ToList());
    }

    public GroupedData Cube(params string[] cols)
    {
        return Cube(cols.Select(Col).ToList());
    }

    public GroupedData Cube(IEnumerable<Column> cols)
    {
        var relation = new Relation
        {
            GroupMap = new GroupMap
            {
                Input = Relation, GroupingExpressions = { cols.Select(p => p.Expression) }
            }
        };
        //TODO - is this working?
        return new GroupedData(SparkSession, Relation, cols.Select(p => p.Expression), Aggregate.Types.GroupType.Cube);
    }

    public DataStreamWriter WriteStream()
    {
        return new DataStreamWriter(SparkSession, Relation);
    }

    public DataFrame ExceptAll(DataFrame other)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                SetOp = new SetOperation
                {
                    ByName = false, SetOpType = SetOperation.Types.SetOpType.Except, LeftInput = Relation, RightInput = other.Relation, IsAll = true, AllowMissingColumns = false
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    /// <summary>
    ///     Replace null values. Note if you pass an int, Spark may be expecting a long, it is pretty specific about this.
    /// </summary>
    /// <param name="value">Has to be an expression generated by Lit(value)</param>
    /// <param name="subset"></param>
    /// <returns></returns>
    public DataFrame FillNa(Column value, params string[] subset)
    {
        if (value.Expression.Literal == null)
        {
            throw new SparkException("The Expression value must be Lit(value)");
        }

        var plan = new Plan
        {
            Root = new Relation
            {
                FillNa = new NAFill
                {
                    Input = Relation, Values = { value.Expression.Literal }, Cols = { subset }
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Where(Column condition)
    {
        return Filter(condition);
    }

    public DataFrame Filter(Column condition)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Filter = new Filter
                {
                    Condition = condition.Expression, Input = Relation
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Filter(string condition)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Filter = new Filter
                {
                    Condition = Expr(condition).Expression, Input = Relation
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public Row First()
    {
        return Take(1).FirstOrDefault();
    }

    public IEnumerable<Row> Head(int rows = 1)
    {
        return Take(rows);
    }

    public IEnumerable<Row> Take(int limit)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Limit = new Limit
                {
                    Input = Relation, Limit_ = limit
                }
            }
        };
        var dataFrame = new DataFrame(SparkSession, plan.Root);
        var schema = dataFrame.Schema;

        return dataFrame.Collect();
    }

    /// <summary>
    ///     Finding frequent items for columns, possibly with false positives. Using the frequent element count algorithm
    ///     described in “https://doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou”.
    /// </summary>
    /// <param name="cols"></param>
    /// <returns></returns>
    public DataFrame FreqItems(params string[] cols)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                FreqItems = new StatFreqItems
                {
                    Input = Relation, Cols = { cols }
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    /// <summary>
    ///     Finding frequent items for columns, possibly with false positives. Using the frequent element count algorithm
    ///     described in “https://doi.org/10.1145/762471.762473, proposed by Karp, Schenker, and Papadimitriou”.
    /// </summary>
    /// <param name="cols"></param>
    /// <param name="support">optional, use FreqItems(cols) if you want to use the default 1%</param>
    /// <returns></returns>
    public DataFrame FreqItems(double support, params string[] cols)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                FreqItems = new StatFreqItems
                {
                    Input = Relation, Cols = { cols }, Support = support
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Hint(string hint, params Column[] values)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Hint = new Hint
                {
                    Input = Relation, Name = hint, Parameters = { values.Select(p => p.Expression) }
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public IEnumerable<string> InputFiles()
    {
        var plan = new Plan
        {
            Root = Relation
        };

        return GrpcInternal.InputFiles(SparkSession, plan);
    }

    /// <summary>
    ///     Return a new DataFrame containing rows only in both this DataFrame and another DataFrame. Note that any duplicates
    ///     are removed. To preserve duplicates use IntersectAll().
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public DataFrame Intersect(DataFrame other)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                SetOp = new SetOperation
                {
                    SetOpType = SetOperation.Types.SetOpType.Intersect, IsAll = false, ByName = false, LeftInput = Relation, RightInput = other.Relation, AllowMissingColumns = false
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    /// <summary>
    ///     Return a new DataFrame containing rows only in both this DataFrame and another DataFrame. Preserving duplicates.
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public DataFrame IntersectAll(DataFrame other)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                SetOp = new SetOperation
                {
                    SetOpType = SetOperation.Types.SetOpType.Intersect, IsAll = true, ByName = false, LeftInput = Relation, RightInput = other.Relation, AllowMissingColumns = false
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }


    private Plan Plan()
    {
        return new Plan
        {
            Root = Relation
        };
    }

    public bool IsEmpty()
    {
        return Count() == 0;
    }

    public bool IsLocal()
    {
        return GrpcInternal.IsLocal(SparkSession, Plan());
    }

    public bool IsStreaming()
    {
        return GrpcInternal.IsStreaming(SparkSession, Plan());
    }

    public DataFrame Unpivot(Column[] ids, Column[] values, string? variableColumnName = null,
        string? valueColumnName = null)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Unpivot = new Unpivot
                {
                    Input = Relation, Ids = { ids.Select(p => p.Expression) }, Values = new Unpivot.Types.Values
                    {
                        Values_ = { values.Select(p => p.Expression) }
                    }
                }
            }
        };

        if (!string.IsNullOrEmpty(variableColumnName))
        {
            plan.Root.Unpivot.VariableColumnName = variableColumnName;
        }

        if (!string.IsNullOrEmpty(valueColumnName))
        {
            plan.Root.Unpivot.ValueColumnName = valueColumnName;
        }

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Melt(Column[] ids, Column[] values, string? variableColumnName = null,
        string? valueColumnName = null)
    {
        return Unpivot(ids, values, variableColumnName, valueColumnName);
    }

    public DataFrame Offset(int num)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Offset = new Offset
                {
                    Input = Relation, Offset_ = num
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame RandomSplit(int seed, List<double> weights)
    {
        throw new NotImplementedException("PySpark does some work to flip this into Sample");
    }

    public DataFrame RandomSplit(List<double> weights)
    {
        throw new NotImplementedException("PySpark does some work to flip this into Sample");
    }

    public DataFrame Sample(bool? withReplacement = null, float? fraction = null, long? seed = null)
    {
        var plan = new Plan();

        plan.Root = new Relation
        {
            Sample = new Sample
            {
                Input = Relation, DeterministicOrder = true, LowerBound = 0.0
            }
        };

        if (withReplacement.HasValue)
        {
            plan.Root.Sample.WithReplacement = withReplacement.Value;
        }

        if (fraction.HasValue)
        {
            plan.Root.Sample.UpperBound = fraction.Value;
        }
        else
        {
            plan.Root.Sample.UpperBound = 1.0;
        }

        if (seed.HasValue)
        {
            plan.Root.Sample.Seed = seed.Value;
        }

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame SampleBy(Column col, IDictionary<int, double> fractions, long? seed = null)
    {
        RepeatedField<StatSampleBy.Types.Fraction> DictionaryToFractions(IDictionary<int, double> dictionary)
        {
            var fractions = new RepeatedField<StatSampleBy.Types.Fraction>();
            foreach (var key in dictionary.Keys.Order())
            {
                fractions.Add(new StatSampleBy.Types.Fraction
                {
                    Fraction_ = dictionary[key], Stratum = Lit(key).Expression.Literal
                });
            }

            return fractions;
        }

        var plan = new Plan
        {
            Root = new Relation
            {
                SampleBy = new StatSampleBy
                {
                    Input = Relation, Col = col.Expression, Fractions = { DictionaryToFractions(fractions) }
                }
            }
        };

        if (seed.HasValue)
        {
            plan.Root.SampleBy.Seed = seed.Value;
        }

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Replace(Column to_replace, Column value, params string[] subset)
    {
        if (to_replace.Expression?.Literal == null)
        {
            throw new SparkException("to_replace must have been created using Lit");
        }

        if (value.Expression?.Literal == null)
        {
            throw new SparkException("value must have been created using Lit");
        }

        var plan = new Plan
        {
            Root = new Relation
            {
                Replace = new NAReplace
                {
                    Input = Relation, Replacements =
                    {
                        new NAReplace.Types.Replacement
                        {
                            OldValue = to_replace.Expression.Literal, NewValue = value.Expression.Literal
                        }
                    }
                }
            }
        };

        if (subset != null && subset.Any())
        {
            plan.Root.Replace.Cols.Add(subset);
        }

        return new DataFrame(SparkSession, plan.Root);
    }

    public GroupedData Rollup(params string[] cols)
    {
        return Rollup(cols.Select(Col).ToArray());
    }

    public GroupedData Rollup(params Column[] cols)
    {
        return new GroupedData(SparkSession, Relation, cols.Select(p => p.Expression),
            Aggregate.Types.GroupType.Rollup);
    }

    public bool SameSemantics(DataFrame other)
    {
        return GrpcInternal.SameSemantics(SparkSession, Relation, other.Relation);
    }

    public DataFrame SelectExpr(params string[] expr)
    {
        if (expr.Length == 0)
        {
            throw new SparkException("At least one expression is required when calling SelectExpr");
        }

        var plan = new Plan
        {
            Root = new Relation
            {
                Project = new Project
                {
                    Input = Relation, Expressions = { expr.Select(p => Expr(p).Expression) }
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public int SemanticHash()
    {
        return GrpcInternal.SemanticHash(SparkSession, Relation);
    }

    public SparkStorageLevelRecord StorageLevel()
    {
        var internalStorageLevel = GrpcInternal.StorageLevel(SparkSession, Relation);

        return new SparkStorageLevelRecord(internalStorageLevel.UseDisk, internalStorageLevel.UseMemory,
            internalStorageLevel.UseOffHeap, internalStorageLevel.Deserialized, internalStorageLevel.Replication);
    }

    public DataFrame Subtract(DataFrame other)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                SetOp = new SetOperation
                {
                    ByName = false, SetOpType = SetOperation.Types.SetOpType.Except, LeftInput = Relation, RightInput = other.Relation, IsAll = false, AllowMissingColumns = false
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Except(DataFrame other)
    {
        return Subtract(other);
    }

    public DataFrame Summary(params string[] statistics)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Summary = new StatSummary
                {
                    Input = Relation
                }
            }
        };

        if (statistics != null && statistics.Any())
        {
            plan.Root.Summary.Statistics.Add(statistics);
        }

        return new DataFrame(SparkSession, plan.Root);
    }

    public IEnumerable<Row> Tail(int num)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Tail = new Tail
                {
                    Input = Relation, Limit = num
                }
            }
        };
        
        var executor = new RequestExecutor(SparkSession, plan);
        
        var task = Task.Run(() => executor.ExecAsync());
        task.Wait();

        return executor.GetData();
    }

    public DataFrame To(StructType schema)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                ToSchema = new ToSchema
                {
                    Input = Relation, Schema = schema.ToDataType()
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame ToDf(params string[] cols)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                ToDf = new ToDF
                {
                    Input = Relation, ColumnNames = { cols }
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame WithWatermark(string eventTime, string delayThreshold)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                WithWatermark = new WithWatermark
                {
                    Input = Relation, EventTime = eventTime, DelayThreshold = delayThreshold
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public DataFrame Hint(string what)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Hint = new Hint
                {
                    Input = Relation, Name = what
                }
            }
        };

        return new DataFrame(SparkSession, plan.Root);
    }

    public static IEnumerable<IEnumerable<object>> ToRows(params object[] objects)
    {
        //don't do new on List<>(){} otherwise the child objects get flattened
        return objects.Cast<IEnumerable<object>>().ToList();
    }

    public static IEnumerable<object> ToRow(params object[] items)
    {
        return items.ToList();
    }

    public DataFrame Transform(TransformFunction function)
    {
        var callable = function.Compile();
        var response = callable(this);

        return response;
    }
}

public enum JoinType
{
    Unspecified = 0
    , Inner = 1
    , FullOuter = 2
    , LeftOuter = 3
    , RightOuter = 4
    , LeftAnti = 5
    , LeftSemi = 6
    , Cross = 7
}

public enum SparkStorageLevel
{
    None
    , DISK_ONLY
    , DISK_ONLY_2
    , DISK_ONLY_3
    , MEMORY_ONLY
    , MEMORY_ONLY_2
    , MEMORY_AND_DISK
    , MEMORY_AND_DISK_2
    , OFF_HEAP
    , MEMORY_AND_DISK_DESER
}

//Too many types with the same name in the generated code.
public record SparkStorageLevelRecord(
    bool useDisk
    , bool useMemory
    , bool useOffHeap
    , bool deserialised
    , int replication = 1);