using System.Text;
using Apache.Arrow.Ipc;
using Grpc.Core;
using Spark.Connect.Dotnet.Grpc.SparkExceptions;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Grpc;

static class GrpcInternal
{
    static async Task  DumpArrowBatch(ExecutePlanResponse.Types.ArrowBatch batch)
    {
        var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(batch.Data.ToByteArray()));
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        
        foreach (var array in recordBatch.Arrays)
        {
            //TODO: should I be using these?
            // var offsetsBuffer = array.Data.Buffers[0];
            // var validityBuffer = array.Data.Buffers[1];

            if (array.Data.Buffers.Length > 2)
            {
                var dataBuffer = array.Data.Buffers[2];
                Console.WriteLine(Encoding.UTF8.GetString(dataBuffer.Span));    
            }
            
        }
    }

    public static string Explain(SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan, Metadata headers, UserContext userContext, string clientType, bool explainExtended, string mode)
    {
        var explainMode = explainExtended
            ? AnalyzePlanRequest.Types.Explain.Types.ExplainMode.Extended
            : AnalyzePlanRequest.Types.Explain.Types.ExplainMode.Simple;
        
        if (!string.IsNullOrEmpty(mode))
        {
            AnalyzePlanRequest.Types.Explain.Types.ExplainMode.TryParse(mode, out explainMode);
        }
    
        var analyzeRequest = new AnalyzePlanRequest()
        {
            Explain = new AnalyzePlanRequest.Types.Explain()
            {
                Plan = plan,
                ExplainMode = explainMode
            },
            SessionId = sessionId,
            UserContext = userContext,
            ClientType = clientType
        };
        
        var analyzeResponse = client.AnalyzePlan(analyzeRequest, headers);
        return analyzeResponse.Explain.ExplainString;
    }
    
    public static Relation Persist(SparkConnectService.SparkConnectServiceClient client, string sessionId, Relation relation, Metadata headers, UserContext userContext, string clientType, bool explainExtended, string mode)
    {
    
        var analyzeRequest = new AnalyzePlanRequest()
        {
            Persist = new AnalyzePlanRequest.Types.Persist()
            {
                Relation = relation,
                StorageLevel = new StorageLevel()
                {
                    UseMemory = true,
                    UseDisk = true,
                    UseOffHeap = false,
                    Deserialized = true,
                    Replication = 1
                }
            },
            SessionId = sessionId,
            UserContext = userContext,
            ClientType = clientType
        };
        
        var analyzeResponse = client.AnalyzePlan(analyzeRequest, headers);
        return relation;
    }

    public static DataType Schema(SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan, Metadata headers, UserContext userContext, string clientType, bool explainExtended, string mode)
    {
        var analyzeRequest = new AnalyzePlanRequest()
        {
            Schema = new AnalyzePlanRequest.Types.Schema()
            {
                Plan = plan
            },
            SessionId = sessionId,
            UserContext = userContext,
            ClientType = clientType
        };
        
        var analyzeResponse = client.AnalyzePlan(analyzeRequest, headers);
        return analyzeResponse.Schema.Schema_;
    }
    
    public static string Version(SparkSession session)
    {
        var analyzeRequest = new AnalyzePlanRequest()
        {
            SparkVersion = new AnalyzePlanRequest.Types.SparkVersion()
            {
                
            },
            SessionId = session.SessionId,
            UserContext = session.UserContext,
            ClientType = session.ClientType
        };
        
        var analyzeResponse = session.Client.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.SparkVersion.Version;
    }

    public static string LastPlan = "";

    public static Relation Exec(SparkSession session, Plan plan)
    {
        var task = Exec(session.Client, session.Host, session.SessionId, plan, session.Headers, session.UserContext, session.ClientType);
        task.Wait();
        return task.Result.Item1;
    }
    
    public static async Task<(Relation, DataType?)> Exec(SparkConnectService.SparkConnectServiceClient client, string host, string sessionId, Plan plan, Metadata headers, UserContext userContext, string clientType)
    {
        var executeRequest = new ExecutePlanRequest
        {
            Plan = plan, SessionId = sessionId, UserContext = userContext, ClientType = clientType
        };

        LastPlan = plan.ToString();

        AsyncServerStreamingCall<ExecutePlanResponse> Exec()
        {
            try
            {
                return client.ExecutePlan(executeRequest, headers);
            }
            catch (Exception exception)
            {
                if (exception is AggregateException aggregateException)
                {
                    throw SparkExceptionFactory.GetExceptionFromRpcException(aggregateException);
                }

                if (exception is RpcException rpcException)
                {
                    throw SparkExceptionFactory.GetExceptionFromRpcException(rpcException);
                }

                throw new SparkException(exception);
            }
        }

        var execResponse = Exec();
        await execResponse.ResponseStream.MoveNext(new CancellationToken());
        var current = execResponse.ResponseStream.Current;

        Relation? dataframe = null;
        DataType? schema = null;
        
        while (current != null)
        {
            if (current?.SqlCommandResult != null)
            {
                Logger.WriteLine($"SqlCommandResult: {current.SqlCommandResult.Relation}");
                dataframe = current.SqlCommandResult.Relation;
            }

            if (current?.Schema != null)
            {
                Logger.WriteLine($"schema: {current.Schema}");
                schema = current.Schema;
            }
            
            if (current?.ArrowBatch != null)
            {
                await DumpArrowBatch(current.ArrowBatch);
            }

            if (current?.WriteStreamOperationStartResult != null)
            {
                
            }
            
            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return (dataframe ?? plan.Root, schema);
    }
    
    public static async Task<(StreamingQueryInstanceId, string)> ExecStreamingResponse(SparkSession session, Plan plan)
    {
        var executeRequest = new ExecutePlanRequest
        {
            Plan = plan, SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        LastPlan = plan.ToString();

        AsyncServerStreamingCall<ExecutePlanResponse> Exec()
        {
            try
            {
                return session.Client.ExecutePlan(executeRequest, session.Headers);
            }
            catch (Exception exception)
            {
                if (exception is AggregateException aggregateException)
                {
                    throw SparkExceptionFactory.GetExceptionFromRpcException(aggregateException);
                }

                if (exception is RpcException rpcException)
                {
                    throw SparkExceptionFactory.GetExceptionFromRpcException(rpcException);
                }

                throw new SparkException(exception);
            }
        }

        var execResponse = Exec();
        await execResponse.ResponseStream.MoveNext(new CancellationToken());
        var current = execResponse.ResponseStream.Current;


        string queryName = null;
        StreamingQueryInstanceId queryId = null;
        
        while (current != null)
        {
            if (current?.WriteStreamOperationStartResult != null)
            {
                queryId = current.WriteStreamOperationStartResult.QueryId;
                queryName = current.WriteStreamOperationStartResult.Name;
            }
            
            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return (queryId, queryName);
    }
    
    public static async Task<(StreamingQueryInstanceId, StreamingQueryCommandResult.Types.StatusResult)> ExecStreamingQueryCommandResponse(SparkSession session, Plan plan)
    {
        var executeRequest = new ExecutePlanRequest
        {
            Plan = plan, SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        LastPlan = plan.ToString();

        AsyncServerStreamingCall<ExecutePlanResponse> Exec()
        {
            try
            {
                return session.Client.ExecutePlan(executeRequest, session.Headers);
            }
            catch (Exception exception)
            {
                if (exception is AggregateException aggregateException)
                {
                    throw SparkExceptionFactory.GetExceptionFromRpcException(aggregateException);
                }

                if (exception is RpcException rpcException)
                {
                    throw SparkExceptionFactory.GetExceptionFromRpcException(rpcException);
                }

                throw new SparkException(exception);
            }
        }

        var execResponse = Exec();
        await execResponse.ResponseStream.MoveNext(new CancellationToken());
        var current = execResponse.ResponseStream.Current;


        
        StreamingQueryCommandResult.Types.StatusResult result = null;
        StreamingQueryInstanceId queryId = null;
        
        while (current != null)
        {
            if (current?.StreamingQueryCommandResult != null)
            {
                queryId = current.StreamingQueryCommandResult.QueryId;
                result = current.StreamingQueryCommandResult.Status;
            }
            
            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return (queryId, result);
    }

    public static async Task<(List<ExecutePlanResponse.Types.ArrowBatch>, DataType?)> ExecArrowResponse(SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan, Metadata headers, UserContext userContext, string clientType)
    {
        var executeRequest = new ExecutePlanRequest
        {
            Plan = plan, SessionId = sessionId, UserContext = userContext, ClientType = clientType
        };

        AsyncServerStreamingCall<ExecutePlanResponse> Exec()
        {
            try
            {
                return client.ExecutePlan(executeRequest, headers);
            }
            catch (Exception exception)
            {
                if (exception is AggregateException aggregateException)
                {
                    throw SparkExceptionFactory.GetExceptionFromRpcException(aggregateException);
                }

                if (exception is RpcException rpcException)
                {
                    throw SparkExceptionFactory.GetExceptionFromRpcException(rpcException);
                }

                throw new SparkException(exception);
            }
        }

        var execResponse = Exec();
        
        await execResponse.ResponseStream.MoveNext(new CancellationToken());
        var current = execResponse.ResponseStream.Current;

        Relation? dataframe = null;
        DataType? schema = null;
        
        List<ExecutePlanResponse.Types.ArrowBatch> batches = new List<ExecutePlanResponse.Types.ArrowBatch>(); 

        while (current != null)
        {
            if (current?.SqlCommandResult != null)
            {
                dataframe = current.SqlCommandResult.Relation;
            }
            
            if (current?.Schema != null)
            {
                schema = current.Schema;
            }

            if (current?.ArrowBatch != null)
            {
                var batch = current.ArrowBatch;
                batches.Add(batch);
            }

            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        if (dataframe == null)
        {
            Logger.WriteLine("EXEC Relation is NULL");
        }

        return (batches, schema);
    }
}