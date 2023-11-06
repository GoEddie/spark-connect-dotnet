using Grpc.Net.Client;

namespace Spark.Connect.Dotnet.Sql;

public class SparkSessionBuilder
{
    private string _remote = String.Empty;
    private SparkSession? _session;
    private readonly int _planId = 0;
    public SparkSessionBuilder Remote(string address)
    {
        _remote = address;
        return this;
    }

    public SparkSession GetOrCreate()
    {
        if (_session != null)
        {
            return _session;
        }
        
        var channel = GrpcChannel.ForAddress(_remote.Replace("sc://", "http://"), new GrpcChannelOptions(){});
        Task.Run(() => channel.ConnectAsync());
        var client = new SparkConnectService.SparkConnectServiceClient(channel);

        _session = new SparkSession(Guid.NewGuid().ToString(), client);
        return _session;
    }
}

public class SparkSession
{
    protected internal readonly string SessionId;
    protected internal readonly SparkConnectService.SparkConnectServiceClient Client;
    private int _planId = 0;

    protected int GetPlanId() => _planId++;

    public static SparkSessionBuilder Builder
    {
        get => new ();
    }
    
    public SparkSession(string sessionId, SparkConnectService.SparkConnectServiceClient client)
    {
        SessionId = sessionId;
        Client = client;
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

    public DataFrameReader Read => new(this);
    
    public DataFrame Range(long end)
    {
        var result = Task.Run(() => RangeAsync(0, end, 1, 1));
        result.Wait();
        return new DataFrame(this, result.Result);
    }
    
    public DataFrame Range(long start, long end)
    {
        var result = Task.Run(() => RangeAsync(start, end, 1, 1));
        result.Wait();
        return new DataFrame(this, result.Result);
    }
    
    public DataFrame Range(long start, long end, long step)
    {
        var result = Task.Run(() => RangeAsync(start, end, step, 1));
        result.Wait();
        return new DataFrame(this, result.Result);
    }
    
    public DataFrame Range(long start, long end, long step, int numParitions)
    {
        var result = Task.Run(() => RangeAsync(start, end, step, numParitions));
        result.Wait();
        return new DataFrame(this, result.Result);
    }
    
    protected internal Relation RangeAsync(long start, long end, long step, int numPartitions)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Range = new global::Spark.Connect.Range
                {
                    Start = start,
                    End = end,
                    Step = step,
                    NumPartitions = numPartitions
                },
                Common = new RelationCommon
                {
                    SourceInfo = $"Range({start}, {end}, {step}, {numPartitions})", PlanId = GetPlanId()
                }
            }
        };

        return plan.Root;
    }

}