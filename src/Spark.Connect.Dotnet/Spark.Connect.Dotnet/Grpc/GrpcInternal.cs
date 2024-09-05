using System.Text;
using Apache.Arrow.Ipc;
using Google.Protobuf.Collections;
using Grpc.Core;
using Spark.Connect.Dotnet.Grpc.SparkExceptions;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Grpc;

public static class GrpcInternal
{
    public static string Explain(SparkConnectService.SparkConnectServiceClient client, string sessionId, Plan plan,
        Metadata headers, UserContext userContext, string clientType, bool explainExtended, string? mode)
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
    
}