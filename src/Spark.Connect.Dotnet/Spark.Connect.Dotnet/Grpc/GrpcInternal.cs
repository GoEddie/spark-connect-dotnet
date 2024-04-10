using System.Text;
using Apache.Arrow.Ipc;
using Grpc.Core;

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
            var dataBuffer = array.Data.Buffers[2];
            
            Console.WriteLine(Encoding.UTF8.GetString(dataBuffer.Span));
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
    
    public static async Task<(Relation, DataType?)> Exec(SparkConnectService.SparkConnectServiceClient client, string host, string sessionId, Plan plan, Metadata headers, UserContext userContext, string clientType)
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
            
            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return (dataframe ?? plan.Root, schema);
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