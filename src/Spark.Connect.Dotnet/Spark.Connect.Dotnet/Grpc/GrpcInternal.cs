using System.Text;
using Apache.Arrow.Ipc;
using Google.Protobuf.Collections;
using Grpc.Core;
using Spark.Connect.Dotnet.Grpc.SparkExceptions;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Grpc;

public static class GrpcInternal
{
    public static string LastPlan = "";

    private static async Task<string> DumpArrowBatch(ExecutePlanResponse.Types.ArrowBatch batch)
    {
        var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(batch.Data.ToByteArray()));
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        var stringBuilder = new StringBuilder();

        foreach (var array in recordBatch.Arrays)
            //TODO: should I be using these?
            // var offsetsBuffer = array.Data.Buffers[0];
            // var validityBuffer = array.Data.Buffers[1];
        {
            if (array.Data.Buffers.Length > 2)
            {
                var dataBuffer = array.Data.Buffers[2];
                stringBuilder.Append(Encoding.UTF8.GetString(dataBuffer.Span));
            }
        }

        return stringBuilder.ToString();
    }

    public static string Explain(SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan,
        Metadata headers, UserContext userContext, string clientType, bool explainExtended, string mode)
    {
        var explainMode = explainExtended
            ? AnalyzePlanRequest.Types.Explain.Types.ExplainMode.Extended
            : AnalyzePlanRequest.Types.Explain.Types.ExplainMode.Simple;

        if (!string.IsNullOrEmpty(mode))
        {
            Enum.TryParse(mode, out explainMode);
        }

        var analyzeRequest = new AnalyzePlanRequest
        {
            Explain = new AnalyzePlanRequest.Types.Explain
            {
                Plan = plan, ExplainMode = explainMode
            }
            , SessionId = sessionId, UserContext = userContext, ClientType = clientType
        };

        var analyzeResponse = client.AnalyzePlan(analyzeRequest, headers);
        return analyzeResponse.Explain.ExplainString;
    }

    public static Relation Persist(SparkSession session, Relation relation, StorageLevel storageLevel)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            Persist = new AnalyzePlanRequest.Types.Persist
            {
                Relation = relation, StorageLevel = storageLevel
            }
            , SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return relation;
    }

    public static DataType Schema(SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan,
        Metadata headers, UserContext userContext, string clientType, bool explainExtended, string mode)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            Schema = new AnalyzePlanRequest.Types.Schema
            {
                Plan = plan
            }
            , SessionId = sessionId, UserContext = userContext, ClientType = clientType
        };

        var analyzeResponse = client.AnalyzePlan(analyzeRequest, headers);
        return analyzeResponse.Schema.Schema_;
    }

    public static string Version(SparkSession session)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            SparkVersion = new AnalyzePlanRequest.Types.SparkVersion(), SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.SparkVersion.Version;
    }

    public static IEnumerable<string> InputFiles(SparkSession session, Plan plan)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            InputFiles = new AnalyzePlanRequest.Types.InputFiles
            {
                Plan = plan
            }
            , SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.InputFiles.Files.Select(p => p);
    }

    public static bool IsLocal(SparkSession session, Plan plan)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            IsLocal = new AnalyzePlanRequest.Types.IsLocal
            {
                Plan = plan
            }
            , SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.IsLocal.IsLocal_;
    }

    public static string TreeString(SparkSession session, Relation relation, int? level = null)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            TreeString = new AnalyzePlanRequest.Types.TreeString
            {
                Plan = new Plan
                {
                    Root = relation
                }
            }
            , SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        if (level.HasValue)
        {
            analyzeRequest.TreeString.Level = level.Value;
        }

        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.TreeString.TreeString_;
    }

    public static int SemanticHash(SparkSession session, Relation relation)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            SemanticHash = new AnalyzePlanRequest.Types.SemanticHash
            {
                Plan = new Plan
                {
                    Root = relation
                }
            }
            , SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };


        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.SemanticHash.Result;
    }

    public static StorageLevel StorageLevel(SparkSession session, Relation relation)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            GetStorageLevel = new AnalyzePlanRequest.Types.GetStorageLevel
            {
                Relation = relation
            }
            , SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };


        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.GetStorageLevel.StorageLevel;
    }

    public static bool IsStreaming(SparkSession session, Plan plan)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            IsStreaming = new AnalyzePlanRequest.Types.IsStreaming
            {
                Plan = plan
            }
            , SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.IsStreaming.IsStreaming_;
    }

    public static bool SameSemantics(SparkSession session, Relation target, Relation other)
    {
        var analyzeRequest = new AnalyzePlanRequest
        {
            SameSemantics = new AnalyzePlanRequest.Types.SameSemantics
            {
                OtherPlan = new Plan
                {
                    Root = other
                }
                , TargetPlan = new Plan
                {
                    Root = target
                }
            }
            , SessionId = session.SessionId, UserContext = session.UserContext, ClientType = session.ClientType
        };

        var analyzeResponse = session.GrpcClient.AnalyzePlan(analyzeRequest, session.Headers);
        return analyzeResponse.SameSemantics.Result;
    }

    public static Relation Exec(SparkSession session, Plan plan)
    {
        var task = Exec(session.GrpcClient, session.Host, session.SessionId, plan, session.Headers, session.UserContext,
            session.ClientType);
        task.Wait();
        return task.Result.Item1;
    }

    public static async Task<(Relation, DataType?, string)> Exec(SparkConnectService.SparkConnectServiceClient client,
        string host, string sessionId, Plan plan, Metadata headers, UserContext userContext, string clientType)
    {
        // Console.WriteLine("** PLAN **");
        // Console.WriteLine(plan);

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
        var batchCount = 0;
        var outputString = "";

        //the rust implementation looks prttier, consider switching to a match expression
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
                outputString = await DumpArrowBatch(current.ArrowBatch);
                batchCount++;
            }

            if (current?.WriteStreamOperationStartResult != null)
            {
            }

            if (current.Metrics != null)
            {
                DumpMetrics(current.Metrics);
            }

            if (current.ObservedMetrics != null && current.ObservedMetrics.Count > 0)
            {
                DumpObservedMetrics(current.ObservedMetrics);
            }


            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return (dataframe ?? plan.Root, schema, outputString);
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
                return session.GrpcClient.ExecutePlan(executeRequest, session.Headers);
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

    public static async Task<(StreamingQueryInstanceId, StreamingQueryCommandResult.Types.StatusResult)>
        ExecStreamingQueryCommandResponse(SparkSession session, Plan plan)
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
                return session.GrpcClient.ExecutePlan(executeRequest, session.Headers);
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

            if (current.Metrics != null)
            {
                DumpMetrics(current.Metrics);
            }

            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return (queryId, result);
    }

    public static async
        Task<(List<ExecutePlanResponse.Types.ArrowBatch>, DataType?,
            RepeatedField<ExecutePlanResponse.Types.Metrics.Types.MetricObject>?)> ExecArrowResponse(
            SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan, Metadata headers,
            UserContext userContext, string clientType)
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
        RepeatedField<ExecutePlanResponse.Types.Metrics.Types.MetricObject>? metrics = null;

        var batches = new List<ExecutePlanResponse.Types.ArrowBatch>();

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

            if (current.Metrics != null)
            {
                metrics = DumpMetrics(current.Metrics);
            }

            if (current.ObservedMetrics != null && current.ObservedMetrics.Count > 0)
            {
                DumpObservedMetrics(current.ObservedMetrics);
            }

            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        if (dataframe == null)
        {
            Logger.WriteLine("EXEC Relation is NULL");
        }

        return (batches, schema, metrics);
    }

    private static void DumpObservedMetrics(
        RepeatedField<ExecutePlanResponse.Types.ObservedMetrics> currentObservedMetrics)
    {
        foreach (var metric in currentObservedMetrics)
        {
            for (var i = 0; i < metric.Keys.Count; i++)
            {
                Console.WriteLine(
                    $"observed metric: {metric.Name}, {metric.PlanId}, {metric.Keys[i]} = {metric.Values[i]}");
            }
        }
    }

    private static RepeatedField<ExecutePlanResponse.Types.Metrics.Types.MetricObject> DumpMetrics(
        ExecutePlanResponse.Types.Metrics currentMetrics)
    {
        foreach (var metric in currentMetrics.Metrics_)
        foreach (var value in metric.ExecutionMetrics)
        {
            Console.WriteLine(
                $"metric: {metric.Name}, parent: {metric.Parent} planid: {metric.PlanId}, {value.Key} = {value.Value}");
        }

        return currentMetrics.Metrics_;
    }


    public static async Task<bool> ExecStreamingQueryAwaitCommandResponse(SparkSession session, Plan plan)
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
                return session.GrpcClient.ExecutePlan(executeRequest, session.Headers);
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

        bool? terminated = null;

        while (current != null)
        {
            if (current.StreamingQueryCommandResult?.AwaitTermination != null)
            {
                terminated = current.StreamingQueryCommandResult.AwaitTermination.Terminated;
            }

            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return terminated.Value;
    }

    public static async Task<StreamingQueryCommandResult.Types.ExceptionResult>
        ExecStreamingQueryExceptionCommandResponse(SparkSession session, Plan plan)
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
                return session.GrpcClient.ExecutePlan(executeRequest, session.Headers);
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

        StreamingQueryCommandResult.Types.ExceptionResult result = null;

        while (current != null)
        {
            if (current.StreamingQueryCommandResult?.Exception != null)
            {
                result = current.StreamingQueryCommandResult.Exception;
            }

            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return result;
    }

    public static async Task<StreamingQueryCommandResult.Types.RecentProgressResult>
        ExecStreamingQueryProgressCommandResponse(SparkSession session, Plan plan)
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
                return session.GrpcClient.ExecutePlan(executeRequest, session.Headers);
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

        while (current != null)
        {
            if (current?.StreamingQueryCommandResult?.RecentProgress != null)
            {
                return current?.StreamingQueryCommandResult?.RecentProgress;
            }

            await execResponse.ResponseStream.MoveNext(new CancellationToken());
            current = execResponse.ResponseStream.Current;
        }

        return null;
    }


    public static async Task ExecUnSetConfigCommandResponse(SparkSession session, string key)
    {
        var configRequest = new ConfigRequest
        {
            ClientType = session.ClientType, SessionId = session.SessionId, UserContext = session.UserContext, Operation = new ConfigRequest.Types.Operation
            {
                Unset = new ConfigRequest.Types.Unset
                {
                    Keys = { key }
                }
            }
        };


        AsyncUnaryCall<ConfigResponse> Exec()
        {
            try
            {
                return session.GrpcClient.ConfigAsync(configRequest, session.Headers);
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

        var response = await Exec();

        foreach (var warning in response.Warnings)
        {
            Console.WriteLine($"Config::Warning: '{warning}'");
        }
    }

    public static async Task ExecSetConfigCommandResponse(SparkSession session, IDictionary<string, string> options)
    {
        var configRequest = new ConfigRequest
        {
            ClientType = session.ClientType, SessionId = session.SessionId, UserContext = session.UserContext, Operation = new ConfigRequest.Types.Operation
            {
                Set = new ConfigRequest.Types.Set
                {
                    Pairs =
                    {
                        options.Select(p => new KeyValue
                        {
                            Key = p.Key, Value = p.Value
                        })
                    }
                }
            }
        };


        AsyncUnaryCall<ConfigResponse> Exec()
        {
            try
            {
                return session.GrpcClient.ConfigAsync(configRequest, session.Headers);
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

        var response = await Exec();

        foreach (var warning in response.Warnings)
        {
            Console.WriteLine($"Config::Warning: '{warning}'");
        }
    }

    public static async Task<Dictionary<string, string>> ExecGetAllConfigCommandResponse(SparkSession session,
        string? prefix = null)
    {
        var configRequest = new ConfigRequest
        {
            ClientType = session.ClientType, SessionId = session.SessionId, UserContext = session.UserContext, Operation = new ConfigRequest.Types.Operation
            {
                GetAll = new ConfigRequest.Types.GetAll()
            }
        };

        if (!string.IsNullOrEmpty(prefix))
        {
            configRequest.Operation.GetAll.Prefix = prefix;
        }

        AsyncUnaryCall<ConfigResponse> Exec()
        {
            try
            {
                return session.GrpcClient.ConfigAsync(configRequest, session.Headers);
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

        var response = await Exec();

        foreach (var warning in response.Warnings)
        {
            Console.WriteLine($"Config::Warning: '{warning}'");
        }

        var items = new Dictionary<string, string>();
        foreach (var pair in response.Pairs)
        {
            items[pair.Key] = pair.Value;
        }

        return items;
    }


    public static async Task ExecStreamingQueryProcessAvailableCommandResponse(SparkSession session, Plan plan)
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
                return session.GrpcClient.ExecutePlan(executeRequest, session.Headers);
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
    }
}