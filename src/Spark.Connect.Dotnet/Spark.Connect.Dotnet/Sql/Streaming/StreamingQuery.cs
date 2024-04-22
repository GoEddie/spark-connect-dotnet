using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql.Streaming;

public class StreamingQuery
{
    private readonly SparkSession _session;
    private readonly StreamingQueryInstanceId _queryId;
    private readonly string _queryName;

    public StreamingQuery(SparkSession session, StreamingQueryInstanceId queryId, string queryName)
    {
        _session = session;
        _queryId = queryId;
        _queryName = queryName;
    }

    public void Stop()
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                StreamingQueryCommand = new StreamingQueryCommand()
                {
                    Stop = true,
                    QueryId = _queryId
                }
            }
        };

        GrpcInternal.Exec(_session, plan);
    }

    public bool IsActive()
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                StreamingQueryCommand = new StreamingQueryCommand()
                {
                    Status = true,
                    QueryId = _queryId
                }
            }
        };

        var task = GrpcInternal.ExecStreamingQueryCommandResponse(_session, plan);
        task.Wait();
        return task.Result.Item2.IsActive;
    }
}