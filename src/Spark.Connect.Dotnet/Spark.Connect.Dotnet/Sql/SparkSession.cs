using System.Collections;
using System.Security.Cryptography;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Spark.Connect.Dotnet.Databricks;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql.Streaming;
using Spark.Connect.Dotnet.Sql.Types;
using BinaryType = Apache.Arrow.Types.BinaryType;
using BooleanType = Apache.Arrow.Types.BooleanType;
using DoubleType = Apache.Arrow.Types.DoubleType;
using StringType = Apache.Arrow.Types.StringType;
using StructType = Spark.Connect.Dotnet.Sql.Types.StructType;

namespace Spark.Connect.Dotnet.Sql;

public class SparkSession
{
    protected internal readonly SparkConnectService.SparkConnectServiceClient Client;

    protected internal readonly string SessionId;
    
    private readonly DatabricksConnectionVerification _databricksConnectionVerification;

    private int _planId;

    /// <summary>
    ///     Creates a new `SparkSession` the normal pattern is to use the `SparkSessionBuilder`.
    /// </summary>
    /// <param name="sessionId"></param>
    /// <param name="url"></param>
    /// <param name="headers"></param>
    /// <param name="userContext"></param>
    /// <param name="clientType"></param>
    /// <param name="databricksConnectionVerification"></param>
    /// <param name="databricksConnectionMaxVerificationTime"></param>
    public SparkSession(string sessionId, string url, Metadata headers, UserContext userContext, string clientType,
        DatabricksConnectionVerification databricksConnectionVerification, TimeSpan databricksConnectionMaxVerificationTime)
    {
        Headers = headers;
        UserContext = userContext;
        SessionId = sessionId;
        _databricksConnectionVerification = databricksConnectionVerification;
        ClientType = clientType;

        Host = url.Replace("sc://", "http://");
        var channel = GrpcChannel.ForAddress(Host, new GrpcChannelOptions());

        Task.Run(() => channel.ConnectAsync()).Wait();
        
        Client = new SparkConnectService.SparkConnectServiceClient(channel);
        VerifyDatabricksClusterRunning(databricksConnectionMaxVerificationTime);
        
    }

    private void VerifyDatabricksClusterRunning(TimeSpan databricksConnectionMaxVerificationTime)
    {
        // var timer = new Timer(databricksConnectionMaxVerificationTime, () => throw new SparkException($"The databricks cluster was not ready before the timeout"));
        bool IsPending(string message)
        {
            if(message.Contains("state=PENDING", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if(message.Contains("PENDING", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (message.Contains("is not usable", StringComparison.OrdinalIgnoreCase))
            {
                return true;    //If terminating then we get this
            }
            return false;

        }
        
        if (IsDatabricks.Url(Host) && _databricksConnectionVerification == DatabricksConnectionVerification.WaitForCluster)
        {
            while (true)
            {
                try
                {
                    Console.WriteLine($"Trying Databricks Query at {DateTime.Now}");
                    Sql("SELECT 'spark-connect-dotnet' as client").Collect();
                    Console.WriteLine($"Completed Databricks Query at {DateTime.Now}");
                    return;
                }
                catch (Exception ex)
                {
                    if (ex is RpcException r)
                    {
                        if (IsPending(r.Status.Detail))
                        {
                            Thread.Sleep(1000* 10);
                            continue;
                        }
                    }

                    if (ex is AggregateException a)
                    {
                        if(a.InnerExceptions.Any(p => IsPending(p.Message)))
                        {
                            Thread.Sleep(1000* 10);
                            continue;
                        }
                    }

                    if (IsPending(ex.Message))
                    {
                        Thread.Sleep(1000* 10);
                        continue;
                    }
                    
                    var inner = ex.InnerException;
                    
                    while (inner != null)
                    {
                        if (IsPending(inner.Message))
                        {
                            Thread.Sleep(1000* 10);
                            continue;
                        }

                        inner = inner.InnerException;
                    }

                    throw;
                }
            }
        }
    }

    /// <summary>
    ///     Mockable `SparkSession` constructor. If you would like to write unit tests using `SparkSession` and would like to
    ///     mock it then
    ///     you can use this constructor. It is not to be used in the actual code, if you try to then any call is guaranteed to
    ///     fail.
    /// </summary>
#pragma warning disable CS8618
    protected internal SparkSession()
    {
    }
#pragma warning restore CS8618
    public Metadata Headers { get; }
    public UserContext UserContext { get; }

    public string Host { get; }

    public string ClientType { get; }

    /// <summary>
    ///     Returns a new `SparkSessionBuilder`.
    /// </summary>
    public static SparkSessionBuilder Builder => new();

    /// <summary>
    ///     Returns a `DataFrameReader` which is used to read from any supported datasource.
    /// </summary>
    public DataFrameReader Read => new(this);

    public StreamingQueryManager Streams { get; } = new();

    //TODO: Should we be setting plan id everytime we send a new plan?
    //      - should we be caching existing plans?
    public int GetPlanId()
    {
        return _planId++;
    }

    /// <summary>
    ///     Creates a new `DataFrame` which has one column called `id` and is from 0 to `end` rows long.
    /// </summary>
    /// <param name="end">What number should the `id` end at?</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Range(long end)
    {
        return new DataFrame(this, RangeInternal(end));
    }

    /// <summary>
    ///     Creates a new `DataFrame` which has one column called `id` and is from `start` to `end` rows long.
    /// </summary>
    /// <param name="start">What number should the `id` start at?</param>
    /// <param name="end">What number should the `id` end at?</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Range(long start, long end)
    {
        return new DataFrame(this, RangeInternal(end, start));
    }

    /// <summary>
    ///     Creates a new `DataFrame` which has one column called `id` and is from `start` to `end` rows long and id jumps in
    ///     `step` increments.
    /// </summary>
    /// <param name="start">What number should the `id` start at?</param>
    /// <param name="end">What number should the `id` end at?</param>
    /// <param name="step">The `id` increment.</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Range(long start, long end, long step)
    {
        return new DataFrame(this, RangeInternal(end, start, step));
    }

    /// <summary>
    ///     Creates a new `DataFrame` which has one column called `id` and is from `start` to `end` rows long and id jumps in
    ///     `step` increments.
    /// </summary>
    /// <param name="start">What number should the `id` start at?</param>
    /// <param name="end">What number should the `id` end at?</param>
    /// <param name="step">The `id` increment.</param>
    /// <param name="numPartitions">How many partitions do you want to create in the `DataFrame`?</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Range(long start, long end, long step, int numPartitions)
    {
        return new DataFrame(this, RangeInternal(end, start, step, numPartitions));
    }

    /// <summary>
    ///     Used to make the Range Relation - note end and start and the wrong way around as end is the only parameter that is
    ///     never defaulted.
    /// </summary>
    /// <param name="end">What number should the `id` end at?</param>
    /// <param name="start">What number should the `id` start at?</param>
    /// <param name="step">The `id` increment.</param>
    /// <param name="numPartitions">How many partitions do you want to create in the `DataFrame`?</param>
    /// <returns></returns>
    protected internal Relation RangeInternal(long end, long start = 0, long step = 1, int numPartitions = 1)
    {
        return new Relation
        {
            Range = new Range
            {
                Start = start,
                End = end,
                Step = step,
                NumPartitions = numPartitions
            }
        };
    }

    /// <summary>
    ///     Creates a `DataFrame` that is the result of the SPARK SQL query that is passed as a string.
    /// </summary>
    /// <param name="sql">The SPARK SQL Query to execute.</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Sql(string sql)
    {
        var task = Task.Run(() => SqlAsync(sql));
        task.Wait();
        return task.Result;
    }

    /// <summary>
    ///     Async Version of `Sql`. Creates a `DataFrame` that is the result of the SPARK SQL query that is passed as a string.
    /// </summary>
    /// <param name="sql">The SPARK SQL Query to execute.</param>
    /// <returns>`DataFrame`</returns>
    public async Task<DataFrame> SqlAsync(string sql)
    {
        var plan = new Plan
        {
            Command = new Command
            {
                SqlCommand = new SqlCommand
                {
                    Sql = sql
                }
            }
        };

        var (relation, schema, output) = await GrpcInternal.Exec(Client, Host, SessionId, plan, Headers, UserContext, ClientType);
        return new DataFrame(this, relation, schema);
    }

    public DataFrame CreateDataFrame(List<(object, object)> rows)
    {
        if (rows.Count == 0)
        {
            throw new SparkException("Cannot CreateDataFrame with no rows");
        }

        var first = rows.First();

        var fields = new List<StructField>();
        fields.Add(new StructField("_1", SparkDataType.FromString(first.Item1.GetType().Name), true));
        fields.Add(new StructField("_2", SparkDataType.FromString(first.Item2.GetType().Name), true));
        var schema = new StructType(fields.ToArray());

        var data = new List<IList<object>>();
        foreach (var row in rows)
        {
            data.Add(new List<object> { row.Item1, row.Item2 });
        }

        return CreateDataFrame(data, schema);
    }

    public DataFrame CreateDataFrame(List<Dictionary<string, object>> rows)
    {   
        //TODO: Need to be able to handle null values
        if (rows.Count == 0)
        {
            throw new SparkException("Cannot CreateDataFrame with no rows");
        }

        var first = rows.First();
        var fields = new List<StructField>();
        foreach (var key in first.Keys)
        {
            fields.Add(new StructField(key, SparkDataType.FromString(first[key].GetType().Name), true));
        }

        var schema = new StructType(fields.ToArray());
        var data = new List<IList<object>>();
        foreach (var row in rows)
        {
            var newRow = new List<object>();

            foreach (var key in first.Keys)
            {
                newRow.Add(row[key]);
            }

            data.Add(newRow);
        }

        return CreateDataFrame(data, schema);
    }

    public DataFrame CreateDataFrame(IList<IList<object>> data, StructType schema)
    {
        var columns = DataToColumns(data);
        var schemaFields = schema.Fields
            .Select(field => new Field(field.Name, field.DataType.ToArrowType(), field.Nullable)).ToList();
        var arrowSchema = new Schema(schemaFields, new List<KeyValuePair<string, string>>());
        var stream = new MemoryStream();
        var writer = new ArrowStreamWriter(stream, arrowSchema);

        var batchBuilder = new RecordBatch.Builder();
        var i = 0;
        foreach (var schemaCol in schemaFields)
        {
            var column = columns[i++];
            
            //TODO - if the wrong schema is passed in it creates an obscure message - write a nice error here like
            // "Data looks like a string but the schema specifies an int32 - is column x correct?"
            
            switch (schemaCol.DataType)
            {
                case StringType:
                    
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.String(builder => builder.AppendRange(column)));
                    break;
                case Int32Type:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Int32(builder => builder.AppendRange(column)));
                    break;
                case DoubleType:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Double(builder => builder.AppendRange(column)));
                    break;
                case Int8Type:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Int8(builder => builder.AppendRange(column)));
                    break;
                case Int64Type:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Int64(builder => builder.AppendRange(column)));
                    break;
                case BinaryType:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Binary(builder => builder.AppendRange(column)));
                    break;
                case BooleanType:
                    
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable, arrayBuilder => arrayBuilder.Boolean(builder => builder.AppendRange(column)));
                    break;

                default:
                    throw new SparkException($"Need Arrow Type for Builder: {schemaCol.DataType}");
            }
        }

        var batch = batchBuilder.Build();

        writer.WriteStart();
        writer.WriteRecordBatch(batch);
        writer.WriteEnd();

        stream.Position = 0;

        var createdRelation = new LocalRelation
        {
            Data = ByteString.FromStream(stream)
        };

        var plan = new Plan
        {
            Root = new Relation
            {
                LocalRelation = createdRelation
            }
        };

        var relation = GrpcInternal.Exec(this, plan);
        return new DataFrame(this, relation);
    }


    private dynamic DataToColumns(IList<IList<object>> data)
    {
        if (!data.Any())
        {
            throw new SparkException("Cannot create a dataframe without any rows");
        }

        var firstRow = data.First();
        var columns = new List<IList>();

        foreach (var column in firstRow)
        {
            var newColumn = CreateGenericList(column.GetType());
            columns.Add(newColumn);
        }

        foreach (var row in data)
        {
            for (var i = 0; i < columns.Count(); i++)
            {
                columns[i].Add(row[i]);
            }
        }

        return columns;
    }

    private static IList CreateGenericList(Type elementType)
    {
        //ChatGPT generated
        var listType = typeof(List<>);

        // Make the generic type by using the elementType
        var constructedListType = listType.MakeGenericType(elementType);

        // Create an instance of the list
        var instance = (IList)Activator.CreateInstance(constructedListType);

        return instance;
    }

    /// <summary>
    ///     Does nothing.
    /// </summary>
    public void Stop()
    {
    }

    public string Version()
    {
        return GrpcInternal.Version(this);
    }

    public DataStreamReader ReadStream()
    {
        return new DataStreamReader(this);
    }

    public DataFrame Table(string name)
    {
        return Read.Table(name);
    }

    private static SparkCatalog _catalog;

    public SparkCatalog Catalog
    {
        get
        {
            if (_catalog == null)
            {
                _catalog = new SparkCatalog(this);
            }

            return _catalog;
        }
    }

}