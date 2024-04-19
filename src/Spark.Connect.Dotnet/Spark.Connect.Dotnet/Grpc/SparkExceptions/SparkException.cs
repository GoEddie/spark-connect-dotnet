using Grpc.Core;

namespace Spark.Connect.Dotnet.Grpc;

public class SparkException : Exception
{
    protected string OverrideMessage = "";

    public SparkException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        if (exception is AggregateException && exception.InnerException is RpcException rpcException)
        {
            OverrideMessage = rpcException.Status.Detail;
        }

        if (exception is RpcException rpcExceptionFromRpc)
        {
            OverrideMessage = rpcExceptionFromRpc.Status.Detail;
        }
        
        Console.WriteLine(GrpcInternal.LastPlan);
    }

    public override string Message
    {
        get => OverrideMessage;
    }

    public SparkException(Exception exception) : base(exception.Message, exception)
    {
        if (exception is AggregateException && exception.InnerException is RpcException rpcException)
        {
            OverrideMessage = rpcException.Status.Detail;
        }
    }
    
    public SparkException(string message) : base(message)
    {
        
    }
}