using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql.Streaming;

public class StreamingQuery
{
    private readonly StreamingQueryInstanceId _queryId;
    private readonly string _queryName;
    private readonly SparkSession _session;

    public StreamingQuery(SparkSession session, StreamingQueryInstanceId queryId, string queryName)
    {
        _session = session;
        _queryId = queryId;
        _queryName = queryName;
    }

    public string Id => _queryId.Id;

    public void Stop()
    {
        var plan = new Plan
        {
            Command = new Command
            {
                StreamingQueryCommand = new StreamingQueryCommand
                {
                    Stop = true, QueryId = _queryId
                }
            }
        };

        var executor = new RequestExecutor(_session, plan);
        executor.Exec();
        _session.Streams.Remove(this);
    }

    public bool IsActive()
    {
        var plan = new Plan
        {
            Command = new Command
            {
                StreamingQueryCommand = new StreamingQueryCommand
                {
                    Status = true, QueryId = _queryId
                }
            }
        };
        
        var executor = new RequestExecutor(_session, plan);
        executor.Exec();

        return executor.GetStreamingQueryCommandResult().IsActive;
    }

    public async Task<bool> AwaitTerminationAsync(int? timeout = null)
    {
        var command = new StreamingQueryCommand
        {
            QueryId = _queryId, AwaitTermination = new StreamingQueryCommand.Types.AwaitTerminationCommand()
        };

        if (timeout.HasValue)
        {
            command.AwaitTermination.TimeoutMs = timeout.Value * 1000;
        }

        var plan = new Plan
        {
            Command = new Command
            {
                StreamingQueryCommand = command
            }
        };

        var executor = new RequestExecutor(_session, plan);
        await executor.ExecAsync();

        return executor.GetStreamingQueryIsTerminated();
    }

    public bool AwaitTermination(int? timeout = null)
    {
        var command = new StreamingQueryCommand
        {
            QueryId = _queryId, AwaitTermination = new StreamingQueryCommand.Types.AwaitTerminationCommand()
        };

        if (timeout.HasValue)
        {
            command.AwaitTermination.TimeoutMs = timeout.Value * 1000;
        }

        var plan = new Plan
        {
            Command = new Command
            {
                StreamingQueryCommand = command
            }
        };

        var executor = new RequestExecutor(_session, plan);
        executor.Exec();

        return executor.GetStreamingQueryIsTerminated();
    }

    public StreamingQueryException Exception()
    {
        var command = new StreamingQueryCommand
        {
            QueryId = _queryId, Exception = true
        };


        var plan = new Plan
        {
            Command = new Command
            {
                StreamingQueryCommand = command
            }
        };

        var executor = new RequestExecutor(_session, plan);
        executor.Exec();

        var result = executor.GetStreamingException();
        
        if (result == null)
        {
            return null;
        }

        return new StreamingQueryException(result.ExceptionMessage);
    }

    public void ProcessAllAvailable()
    {
        var command = new StreamingQueryCommand
        {
            QueryId = _queryId, ProcessAllAvailable = true
        };

        var plan = new Plan
        {
            Command = new Command
            {
                StreamingQueryCommand = command
            }
        };

        var executor = new RequestExecutor(_session, plan);
        executor.Exec();
    }

    public IEnumerable<string> RecentProgress()
    {
        var command = new StreamingQueryCommand
        {
            QueryId = _queryId, RecentProgress = true
        };

        var plan = new Plan
        {
            Command = new Command
            {
                StreamingQueryCommand = command
            }
        };

        var executor = new RequestExecutor(_session, plan);
        executor.Exec();

        var response = executor.GetStreamingRecentProgress();
        return response.RecentProgressJson.Select(p => p);
    }
}