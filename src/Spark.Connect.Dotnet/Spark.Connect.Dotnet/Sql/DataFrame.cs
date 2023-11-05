using System.Text;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql;

public class DataFrame
{
    private readonly SparkSession _session;
    protected internal readonly Relation Relation;

    protected internal DataFrame(SparkSession session, Relation relation)
    {
        _session = session;
        Relation = relation;
    }
    
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

        return new DataFrame(_session, limitPlan.Root);
    }
    
    /// <summary>
    /// Prints the first `numberOfRows` rows to the console.
    /// </summary>
    /// <param name="numberOfRows">The number of rows to show</param>
    /// <param name="truncate">If set greater than one, truncates long strings to length truncate and align cells right.</param>
    /// <param name="vertical">Print output rows vertically (one line per column value).</param>
    public void Show(int numberOfRows = 10, int truncate = 20, bool vertical = false)
    {
        var result = Task.Run(() => ShowAsync(Relation, numberOfRows, truncate, vertical, _session.SessionId, _session.Client));
        result.Wait();
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
    public static async Task ShowAsync(Relation input, int numberOfRows, int truncate, bool vertical, string sessionId, SparkConnectService.SparkConnectServiceClient client)
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
        
        await GrpcInternal.Exec(client, sessionId, showStringPlan);
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

        return new DataFrame(_session, relation);
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

        return new DataFrame(_session, relation);
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
            
        return new DataFrame(_session, relation);
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
            
        return new DataFrame(_session, relation);
    }

    public DataFrameWriter Write() => new(_session, this);

    public async Task<List<object[]>> CollectAsync()
    {
        var plan = new Plan()
        {
            Root = Relation
        };
        
       var batch =  await GrpcInternal.ExecArrowResponse(_session.Client, _session.SessionId, plan);
       var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(batch.Data.ToByteArray()));
       Logger.WriteLine($"arrow schema: {reader.Schema}");
       var schema = reader.Schema;
       var recordBatch = await reader.ReadNextRecordBatchAsync();
       Logger.WriteLine("arrow: Read record batch with {0} column(s)", recordBatch.ColumnCount);
       
       var rows = new List<object[]>();
       var currentArray = 0;

       for (int i = 0; i < recordBatch.Column(0).Length; i++)
       {
           rows.Add(new object[reader.Schema.FieldsList.Count]);
       }
       
       for (var i = 0; i < recordBatch.ColumnCount; i++)
       {
           var column = recordBatch.Column(i);
           
           switch (reader.Schema.FieldsList[i].DataType)
           {
               case BinaryType binaryType:
                   break;
               case BooleanType booleanType:
                   var b = (BooleanArray)recordBatch.Column(i);
                   for (int ii = 0; ii < b.Length; ii++)
                   {
                       rows[ii][i] = b.GetValue(ii);
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
                       rows[ii][i] = d.GetValue(ii);
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
                       rows[ii][i] = i64.GetValue(ii);
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
    /// <summary>
    /// Returns the `SparkColumn` denoted by name.
    /// </summary>
    /// <param name="name"></param>
    public SparkColumn this[string name] => new(name);
}