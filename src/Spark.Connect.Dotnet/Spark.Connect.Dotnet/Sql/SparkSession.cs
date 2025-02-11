using System.Collections;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Spark.Connect.Dotnet.Databricks;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql.Streaming;
using Spark.Connect.Dotnet.Sql.Types;
using BinaryType = Apache.Arrow.Types.BinaryType;
using BooleanType = Apache.Arrow.Types.BooleanType;
using DoubleType = Apache.Arrow.Types.DoubleType;
using FloatType = Apache.Arrow.Types.FloatType;
using MapType = Apache.Arrow.Types.MapType;
using StringType = Apache.Arrow.Types.StringType;
using StructType = Spark.Connect.Dotnet.Sql.Types.StructType;
using TimestampType = Apache.Arrow.Types.TimestampType;

namespace Spark.Connect.Dotnet.Sql;

public class SparkSession
{
    private readonly SparkCatalog _catalog;

    private readonly DatabricksConnectionVerification _databricksConnectionVerification;

    public readonly RuntimeConf Conf;
    public readonly SparkConnectService.SparkConnectServiceClient GrpcClient;

    public readonly string SessionId;

    private int _planId = 1;
    public LocalConsole Console;

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
    /// <param name="sparkConnectDotnetConf"></param>
    public SparkSession(string sessionId, string url, Metadata headers, UserContext userContext, string clientType,
        DatabricksConnectionVerification databricksConnectionVerification,
        TimeSpan databricksConnectionMaxVerificationTime, Dictionary<string, string> sparkConnectDotnetConf,
        LocalConsole? customConsole = null)
    {
        Headers = headers;
        UserContext = userContext;
        SessionId = sessionId;
        _databricksConnectionVerification = databricksConnectionVerification;
        Console = customConsole ?? new LocalConsole();
        
        Conf = new RuntimeConf(this, sparkConnectDotnetConf);
        ClientType = clientType;

        Host = url.Replace("sc://", "http://");
        var channel = GrpcChannel.ForAddress(Host, new GrpcChannelOptions()
        {
            ServiceConfig = new ServiceConfig()
            {
                MethodConfigs = { new MethodConfig()
                {
                    RetryPolicy = new RetryPolicy()
                    {
                        
                    }
                } }
            }
        });

        Task.Run(() => channel.ConnectAsync()).Wait();

        GrpcClient = new SparkConnectService.SparkConnectServiceClient(channel);
        VerifyDatabricksClusterRunning(databricksConnectionMaxVerificationTime);

        GrpcChannel = channel;
        _catalog = new SparkCatalog(this);
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

    public GrpcChannel GrpcChannel { get; }

    public SparkCatalog Catalog => _catalog;

    private void VerifyDatabricksClusterRunning(TimeSpan databricksConnectionMaxVerificationTime)
    {
        bool IsPending(string message)
        {
            if (message.Contains("state=PENDING", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (message.Contains("PENDING", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (message.Contains("is not usable", StringComparison.OrdinalIgnoreCase))
            {
                return true; //If terminating then we get this
            }

            return false;
        }

        if (IsDatabricks.Url(Host) && _databricksConnectionVerification == DatabricksConnectionVerification.WaitForCluster)
        {
            while (true)
            {
                try
                {   
                    Console.WriteLine(DateTime.Now + " :: Trying Connection");
                    Sql("SELECT 'spark-connect-dotnet' as client").CollectAsArrowBatch();
                    return;
                }
                catch (Exception ex)
                {
                    if (ex is SparkException s)
                    {
                        Console.WriteLine(DateTime.Now + " :: SparkException :: " + s.Message + " :: " +      s.InnerException?.Message);
                        if (IsPending(s.InnerException?.Message + s.Message))
                        {
                            Thread.Sleep(1000 * 10);
                            continue;
                        }
                    }
                    
                    if (ex is RpcException r)
                    {
                        Console.WriteLine(DateTime.Now + " :: RpcException :: " + r.Status.StatusCode);
                        if (IsPending(r.Status.Detail))
                        {
                            Thread.Sleep(1000 * 10);
                            continue;
                        }
                    }

                    if (ex is AggregateException a)
                    {
                        if (a.InnerExceptions.Any(p => IsPending(p.Message)))
                        {
                            Thread.Sleep(1000 * 10);
                            continue;
                        }
                    }

                    if (IsPending(ex.Message))
                    {
                        Thread.Sleep(1000 * 10);
                        continue;
                    }

                    var inner = ex.InnerException;

                    while (inner != null)
                    {
                        if (IsPending(inner.Message))
                        {
                            Thread.Sleep(1000 * 10);
                            continue;
                        }

                        inner = inner.InnerException;
                    }

                    throw;
                }
            }
        }
    }

    
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
                Start = start, End = end, Step = step, NumPartitions = numPartitions
            }
        };
    }

    /// <summary>
    ///     Async Version of `Sql`. Creates a `DataFrame` that is the result of the SPARK SQL query that is passed as a string.
    /// </summary>
    /// <param name="sql">The SPARK SQL Query to execute.</param>
    /// <returns>`DataFrame`</returns>
    /// TODO: Migrate to SqlFormatter - see https://github.com/apache/spark/commit/a100e11936bcd92ac091abe94221c1b669811efa#diff-5b26ee7d224ae355b252d713e570cb03eaecbf7f8adcdb6287dc40c370b71462
    public DataFrame Sql(string sql)
    {
        var plan = new Plan()
        {
            Root = new Relation() { Sql = new SQL() { Query = sql } }
        };

        var executor = new RequestExecutor(this, plan);
        executor.Exec();    //We have to exec because the sql could be something like 'create functions blah'
        
        return new DataFrame(this, executor.GetRelation());
    }

    /// <summary>
    ///     Creates a `DataFrame` that is the result of the SPARK SQL query that is passed as a string.
    ///     ///     Args is a list of DataFrames to wrap in CreateOrReplaceTempView, example (spark is a SparkSession):
    ///     ```csharp
    ///     var df = spark.Range(100);
    ///     var dict = new Dictionary
    ///     &lt;string, object&gt;
    ///         ();
    ///         dict["c"] = df["id"]; //could do Col("id") etc
    ///         dict["dataFramePassedIn"] = df;
    ///         dict["three"] = 3;
    ///         spark.Sql("SELECT {c} FROM {dataFramePassedIn} WHERE {c} = {three}", dict).Show();
    ///         ```
    /// </summary>
    /// <param name="sql">The SPARK SQL Query to execute.</param>
    /// <param name="args">
    ///     Args is a list of key names to replace in the Sql with values, can also include DataFrames which are
    ///     wrapped in CreateOrReplaceTempView
    /// </param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Sql(string sql, IDictionary<string, object> args)
    {
        var formattedSql = SqlFormatter.Format(sql, args);
        var expressionMap = args.ToDictionary(arg => arg.Key, arg => Functions.Lit(arg.Value).Expression);
        var plan = new Plan(){
            Root = new Relation()
        {
            Sql = new SQL()
            {
                Query = formattedSql, NamedArguments = { expressionMap }
            }
        }};
        var executor = new RequestExecutor(this, plan);
        executor.Exec();
        return new DataFrame(this, executor.GetRelation());
        
    }

    /// <summary>
    ///     Async Version of `Sql`. Creates a `DataFrame` that is the result of the SPARK SQL query that is passed as a string.
    ///     SqlAsync("SELECT * FROM {dataFrame1}", ("dataFrame1", SparkSession.Range(100)));
    /// </summary>
    /// <param name="sql">The SPARK SQL Query to execute.</param>
    /// <param name="args">
    ///     Array of tuples containing string name to replace in the SQL and the DataFrame to wrap in
    ///     createOrReplaceTempView
    /// </param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Sql(string sql, params (string, DataFrame)[] dataFrames)
    {
        var dict = new Dictionary<string, object>();
        foreach (var tuple in dataFrames)
        {
            dict[tuple.Item1] = tuple.Item2;
        }

        return Sql(sql, dict);
    }

    /// <summary>
    ///     Pass in a list of tuples, schema is guessed by the type of the first tuple's child types:
    ///     CreateDataFrame(new List
    ///     &lt;object, object&gt;
    ///         (){
    ///         ("tupple", 1), ("another", 2)
    ///         });
    /// </summary>
    /// <param name="data">List of tuples (2 values)</param>
    /// <param name="cola">The name of the first column</param>
    /// <param name="colb">The name of the second column</param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    /// <exception cref="SparkException"></exception>
    public DataFrame CreateDataFrame(IEnumerable<(object, object)> rows, string cola, string colb)
    {
        if (!rows.Any())
        {
            throw new SparkException("Cannot CreateDataFrame with no rows");
        }

        var first = rows.First();

        var fields = new List<StructField>
        {
            new(cola, SparkDataType.FromString(first.Item1.GetType().Name), true), new(colb, SparkDataType.FromString(first.Item2.GetType().Name), true)
        };
        var schema = new StructType(fields.ToArray());

        var data = new List<IList<object>>();
        foreach (var row in rows)
        {
            data.Add(new List<object> { row.Item1, row.Item2 });
        }

        return CreateDataFrame(data, schema);
    }

    /// <summary>
    ///     Pass in a list of tuples, schema is guessed by the type of the first tuple's child types:
    ///     CreateDataFrame(new List
    ///     &lt;object, object&gt;
    ///         (){
    ///         ("tupple", 1), ("another", 2)
    ///         });
    /// </summary>
    /// <param name="data"></param>
    /// <param name="schema"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    /// <exception cref="SparkException"></exception>
    public DataFrame CreateDataFrame(IEnumerable<(object, object)> rows)
    {
        if (!rows.Any())
        {
            throw new SparkException("Cannot CreateDataFrame with no rows");
        }

        var first = rows.First();

        var fields = new List<StructField>
        {
            new("_1", SparkDataType.FromString(first.Item1.GetType().Name), true), new("_2", SparkDataType.FromString(first.Item2.GetType().Name), true)
        };
        var schema = new StructType(fields.ToArray());

        var data = new List<IList<object>>();
        foreach (var row in rows)
        {
            data.Add(new List<object> { row.Item1, row.Item2 });
        }

        return CreateDataFrame(data, schema);
    }

    /// <summary>
    ///     Pass in a List of Dictionary
    ///     &lt;string, type&gt; - the key is used as the name of the field. The schema is guessed at using the first dictionary
    /// </summary>
    /// <param name="rows"></param>
    /// <returns></returns>
    /// <exception cref="SparkException"></exception>
    public DataFrame CreateDataFrame(IEnumerable<Dictionary<string, object>> rows)
    {
        if (!rows.Any())
        {
            throw new SparkException("Cannot CreateDataFrame with no rows");
        }

        var first = rows.First();

        var schema = ParseObjectsToCreateSchema(first.Values.ToList(), first.Keys.ToArray());
        return CreateDataFrame(rows, schema);
    }

    /// <summary>
    ///     Pass in a List of Dictionary
    ///     &lt;string, type&gt; - the key is used as the name of the field. The schema is passed in explicitly
    /// </summary>
    /// <param name="rows"></param>
    /// <param name="schema"></param>
    /// <returns></returns>
    /// <exception cref="SparkException"></exception>
    public DataFrame CreateDataFrame(IEnumerable<Dictionary<string, object>> rows, StructType schema)
    {
        if (!rows.Any())
        {
            throw new SparkException("Cannot CreateDataFrame with no rows");
        }

        var first = rows.First();
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

    private static StructType ParseObjectsToCreateSchema(IList row, params string[] colNames)
    {
        var fields = new List<StructField>();
        var usedColNumbers = 0;
        for (var i = 0; i < row.Count; i++)
        {
            var type = SparkDataType.FromDotNetType(row[i]);
            var colName = colNames.Length > i ? colNames[i] : $"_{usedColNumbers++}";

            fields.Add(new StructField(colName, type, true));
        }

        var schema = new StructType(fields);
        return schema;
    }

    /// <summary>
    ///     Pass in an IEnumerable of Ienumerable objects, the types are guessed using the first row and if you pass in column
    ///     names they are used
    ///     if colNames has fewer columns than the first row column names not in colNames are named _1, _2, etc
    /// </summary>
    /// <param name="data"></param>
    /// <param name="colNames"></param>
    /// <returns></returns>
    public DataFrame CreateDataFrame(IEnumerable<IEnumerable<object>> data, params string[] colNames)
    {
        var schema = ParseObjectsToCreateSchema(data.First().ToList(), colNames);
        return CreateDataFrame(data, schema);
    }


    /// <summary>
    ///     Pass in rows and an explicit schema:
    ///     CreateDataFrame(new List
    ///     &lt;object&gt;
    ///         (){
    ///         new List
    ///         &lt;object&gt;
    ///             (){"abc", 123, 100.123},
    ///             new List
    ///             &lt;object&gt;
    ///                 (){"def", 456, 200.456},
    ///                 new List
    ///                 &lt;object&gt;
    ///                     (){"xyz", 999, 999.456},
    ///                     }, new StructType(
    ///                     new StructField("col_a", StringType(), true),
    ///                     new StructField("col_b", IntType(), true),
    ///                     new StructField("col_c", DoubleType(), true),
    ///                     ));
    /// </summary>
    /// <param name="data"></param>
    /// <param name="schema"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    /// <exception cref="SparkException"></exception>
    public DataFrame CreateDataFrame(IEnumerable<IEnumerable<object>> data, StructType schema)
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
                        arrayBuilder => arrayBuilder.Int32(builder => AppendInt(column, builder)));
                    break;
                case DoubleType:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Double(builder => AppendDouble(column, builder)));
                    break;

                case FloatType:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Float(builder => AppendFloat(column, builder)));
                    break;
                case Int8Type:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Binary(builder => AppendByte(column, builder)));
                    break;
                case Int64Type:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Int64(builder => AppendLong(column, builder)));
                    break;
                case BinaryType:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Binary(builder => AppendByte(column, builder)));
                    break;
                case BooleanType:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Boolean(builder => AppendBool(column, builder)));
                    break;
                case Date32Type:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Date32(builder => AppendDateTime(column, builder)));
                    break;
                case Date64Type:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Date64(builder => AppendDate64(column, builder)));
                    break;
                case TimestampType:
                    batchBuilder = batchBuilder.Append(schemaCol.Name, schemaCol.IsNullable,
                        arrayBuilder => arrayBuilder.Int64(builder => AppendTimestamp(column, builder)));
                    break;

                case ListType:
                    throw new NotImplementedException(
                        "Currently you can't pass a complex type to CreateDataFrame - use Spark.Sql array, map, etc i.e. spark.Range(1).Select(Map(...)) will do the same thing as CreateDataFrame or WithColumn etc");

                case MapType:
                    throw new NotImplementedException(
                        "Currently you can't pass a complex type to CreateDataFrame - use Spark.Sql array, map, etc i.e. spark.Range(1).Select(Map(...)) will do the same thing as CreateDataFrame or WithColumn etc");

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

        var executor = new RequestExecutor(this, plan);
        Task.Run(() => executor.ExecAsync()).Wait();
        return new DataFrame(this, executor.GetRelation());
    }

    private static IEnumerable<Int32Array.Builder> AppendInt(dynamic? column, Int32Array.Builder builder)
    {
        var list = (List<int?>)column;
        var retList = new List<Int32Array.Builder>();
        foreach (var i in list)
        {
            retList.Add(builder.Append(i));
        }

        return retList;
    }

    private static IEnumerable<Int64Array.Builder> AppendLong(dynamic? column, Int64Array.Builder builder)
    {
        var list = (List<long?>)column;
        var retList = new List<Int64Array.Builder>();
        foreach (var i in list)
        {
            retList.Add(builder.Append(i));
        }

        return retList;
    }

    private static IEnumerable<BooleanArray.Builder> AppendBool(dynamic? column, BooleanArray.Builder builder)
    {
        var list = (List<bool?>)column;
        var retList = new List<BooleanArray.Builder>();
        foreach (var i in list)
        {
            if (i.HasValue)
            {
                retList.Add(builder.Append(i.Value));
            }
            else
            {
                retList.Add(builder.AppendNull());
            }
        }

        return retList;
    }

    private static IEnumerable<Int16Array.Builder> AppendShort(dynamic? column, Int16Array.Builder builder)
    {
        var list = (List<short?>)column;
        var retList = new List<Int16Array.Builder>();
        foreach (var i in list)
        {
            retList.Add(builder.Append(i));
        }

        return retList;
    }

    private static IEnumerable<BinaryArray.Builder> AppendByte(dynamic? column, BinaryArray.Builder builder)
    {
        var list = (List<byte?>)column;
        var retList = new List<BinaryArray.Builder>();
        foreach (var i in list)
        {
            if (i.HasValue)
            {
                retList.Add(builder.Append(i.Value));
            }

            else
            {
                retList.Add(builder.AppendNull());
            }
        }

        return retList;
    }

    private static IEnumerable<Date32Array.Builder> AppendDateTime(dynamic? column, Date32Array.Builder builder)
    {
        var list = (List<DateTime?>)column;
        var retList = new List<Date32Array.Builder>();
        foreach (var i in list)
        {
            if (i.HasValue)
            {
                retList.Add(builder.Append(i.Value));
            }

            else
            {
                retList.Add(builder.AppendNull());
            }
        }

        return retList;
    }

    private static IEnumerable<Date64Array.Builder> AppendDate64(dynamic? column, Date64Array.Builder builder)
    {
        var list = (List<DateTime?>)column;
        var retList = new List<Date64Array.Builder>();
        foreach (var i in list)
        {
            if (i.HasValue)
            {
                retList.Add(builder.Append(i.Value));
            }

            else
            {
                retList.Add(builder.AppendNull());
            }
        }

        return retList;
    }

    private static IEnumerable<Int64Array.Builder> AppendTimestamp(dynamic? column, Int64Array.Builder builder)
    {
        //Timestamp is actually long microseconds since unix epoch - don't use timestamp builder
        // the arrow schema type also needs to be set to unit=microsoeconds and timezone=utc

        var list = (List<DateTime?>)column;
        var retList = new List<Int64Array.Builder>();
        foreach (var i in list)
        {
            if (i.HasValue)
            {
                var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                retList.Add(builder.Append((long)(i.Value - unixEpoch).TotalMicroseconds));
            }
            else
            {
                retList.Add(builder.AppendNull());
            }
        }

        return retList;
    }

    private static IEnumerable<FloatArray.Builder> AppendFloat(dynamic? column, FloatArray.Builder builder)
    {
        var list = (List<float?>)column;
        var retList = new List<FloatArray.Builder>();
        foreach (var i in list)
        {
            retList.Add(builder.Append(i));
        }

        return retList;
    }

    private static IEnumerable<DoubleArray.Builder> AppendDouble(dynamic? column, DoubleArray.Builder builder)
    {
        var list = (List<double?>)column;
        var retList = new List<DoubleArray.Builder>();
        foreach (var i in list)
        {
            retList.Add(builder.Append(i));
        }

        return retList;
    }


    private dynamic DataToColumns(IEnumerable<IEnumerable<object>> data)
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

        foreach (var row in data.Select(d => d.ToList()))
        {
            for (var i = 0; i < columns.Count(); i++)
            {
                if (i < row.Count())
                {
                    columns[i].Add(row[i]);
                }
            }
        }

        return columns;
    }

    private static IList CreateGenericList(Type elementType)
    {
        if (elementType == typeof(IDictionary<string, object>) || elementType == typeof(Dictionary<string, object>) ||
            elementType == typeof(string) || elementType == typeof(string[]))
        {
            var listType = typeof(List<>);

            // Make the generic type by using the elementType
            var constructedListType = listType.MakeGenericType(elementType);

            // Create an instance of the list
            var instance = (IList)Activator.CreateInstance(constructedListType);

            return instance;
        }
        else
        {
            //ChatGPT generated
            var listType = typeof(List<>);

            var nullableType = typeof(Nullable<>).MakeGenericType(elementType);

            // Make the generic type by using the elementType
            var constructedListType = listType.MakeGenericType(nullableType);

            // Create an instance of the list
            var instance = (IList)Activator.CreateInstance(constructedListType);

            return instance;
        }
    }

    /// <summary>
    ///     Does nothing.
    /// </summary>
    public void Stop()
    {
    }

    /// <summary>
    /// Interrupt all operations of this session currently running on the connected server.
    /// </summary>
    public List<string> InterruptAll()
    {
        var task = Task.Run(()=>GrpcInternal.InterruptAll(this));
        task.Wait();

        return task.Result;
            
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
}