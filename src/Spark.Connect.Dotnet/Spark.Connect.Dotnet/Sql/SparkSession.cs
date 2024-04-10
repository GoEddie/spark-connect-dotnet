using Apache.Arrow;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Spark.Connect.Dotnet.Grpc;
using StringType = Apache.Arrow.Types.StringType;

namespace Spark.Connect.Dotnet.Sql;

public class SparkSession
{
    public Metadata Headers { get; }
    public UserContext UserContext { get; }

    public string Host { get; private set; }
    
    public string ClientType { get; }
    
    protected internal readonly string SessionId;
    
    protected internal readonly SparkConnectService.SparkConnectServiceClient Client;
    
    private int _planId = 0;

    //TODO: Should we be setting plan id everytime we send a new plan?
    //      - should we be caching existing plans?
    public int GetPlanId() => _planId++;

    /// <summary>
    /// Returns a new `SparkSessionBuilder`.
    /// </summary>
    public static SparkSessionBuilder Builder
    {
        get => new ();
    }

    /// <summary>
    /// Creates a new `SparkSession` the normal pattern is to use the `SparkSessionBuilder`.
    /// </summary>
    /// <param name="sessionId"></param>
    /// <param name="url"></param>
    /// <param name="headers"></param>
    /// <param name="userContext"></param>
    /// <param name="clientType"></param>
    public SparkSession(string sessionId, string url, Metadata headers, UserContext userContext, string clientType = "dotnet")
    {
        Headers = headers;
        UserContext = userContext;
        SessionId = sessionId;
        ClientType = clientType;
        
        Host = url.Replace("sc://", "http://");
        var channel = GrpcChannel.ForAddress(Host, new GrpcChannelOptions(){});
        
        Task.Run(() => channel.ConnectAsync());
        Client = new SparkConnectService.SparkConnectServiceClient(channel);
    }

    /// <summary>
    /// Mockable `SparkSession` constructor. If you would like to write unit tests using `SparkSession` and would like to mock it then
    ///  you can use this constructor. It is not to be used in the actual code, if you try to then any call is guaranteed to fail.
    /// </summary>
#pragma warning disable CS8618
    protected internal SparkSession()
    {
    }
#pragma warning restore CS8618

    /// <summary>
    /// Returns a `DataFrameReader` which is used to read from any supported datasource.
    /// </summary>
    public DataFrameReader Read => new(this);
    
    /// <summary>
    /// Creates a new `DataFrame` which has one column called `id` and is from 0 to `end` rows long.
    /// </summary>
    /// <param name="end">What number should the `id` end at?</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Range(long end)=> new (this, RangeInternal(end));
    
    /// <summary>
    /// Creates a new `DataFrame` which has one column called `id` and is from `start` to `end` rows long.
    /// </summary>
    /// <param name="start">What number should the `id` start at?</param>
    /// <param name="end">What number should the `id` end at?</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Range(long start, long end) => new (this, RangeInternal(end, start));
    
    /// <summary>
    /// Creates a new `DataFrame` which has one column called `id` and is from `start` to `end` rows long and id jumps in `step` increments.
    /// </summary>
    /// <param name="start">What number should the `id` start at?</param>
    /// <param name="end">What number should the `id` end at?</param>
    /// <param name="step">The `id` increment.</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Range(long start, long end, long step) => new (this, RangeInternal(end, start, step));

    /// <summary>
    /// Creates a new `DataFrame` which has one column called `id` and is from `start` to `end` rows long and id jumps in `step` increments.
    /// </summary>
    /// <param name="start">What number should the `id` start at?</param>
    /// <param name="end">What number should the `id` end at?</param>
    /// <param name="step">The `id` increment.</param>
    /// <param name="numPartitions">How many partitions do you want to create in the `DataFrame`?</param>
    /// <returns>`DataFrame`</returns>
    public DataFrame Range(long start, long end, long step, int numPartitions) => new (this, RangeInternal(end, start, step, numPartitions));
    
    /// <summary>
    /// Used to make the Range Relation - note end and start and the wrong way around as end is the only parameter that is never defaulted.
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
            Range = new global::Spark.Connect.Range
            {
                Start = start,
                End = end,
                Step = step,
                NumPartitions = numPartitions
            }
        };
    }

    /// <summary>
    /// Creates a `DataFrame` that is the result of the SPARK SQL query that is passed as a string.
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
    /// Async Version of `Sql`. Creates a `DataFrame` that is the result of the SPARK SQL query that is passed as a string.
    /// </summary>
    /// <param name="sql">The SPARK SQL Query to execute.</param>
    /// <returns>`DataFrame`</returns>
    public async Task<DataFrame> SqlAsync(string sql)
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                SqlCommand = new SqlCommand()
                {
                    Sql = sql
                }
            }
        };

        var (relation, schema) = await GrpcInternal.Exec(Client,  Host, SessionId, plan, Headers, UserContext, ClientType);
        return new DataFrame(this, relation, schema);
    }

    public async Task<DataFrame>  a()
    {
        var list = new List<List<object>>()
        {
            new List<object>() { "hello1", 100 },
            new List<object>() { "hello2", 200 },
            new List<object>() { "hello3", 300 },
        };
        
        var cola = new List<string>();
        foreach (var row in list)
        { 
            cola.Add(row[0] as string);
        }
        var colb = new List<int>();
        foreach (var row in list)
        {
            colb.Add((int)row[1]);
        }
        
        var stream = new MemoryStream();
        var arrowSchema = new Schema(new List<Field>()
        {
            new Field("col_a", new StringType(), false)
        },new List<KeyValuePair<string, string>>());
        
        var writer = new ArrowStreamWriter(stream, arrowSchema);

        var batch = new RecordBatch.Builder()
            .Append("cola", false, col => col.String(array => array.AppendRange(cola)))
            .Build();
        
        writer.WriteStart();
        writer.WriteRecordBatch(batch);
        writer.WriteEnd();

        stream.Position = 0;
        
        var createdRelation = new LocalRelation()
        {
            Data = ByteString.FromStream(stream)
        };
            
        var plan = new Plan()
        {
            Root = new Relation()
            {
                LocalRelation = createdRelation
            }
        };
        
        var (relation, schema) = await GrpcInternal.Exec(Client,  Host, SessionId, plan, Headers, UserContext, ClientType
        );
        return new DataFrame(this, relation, schema);
    }
}