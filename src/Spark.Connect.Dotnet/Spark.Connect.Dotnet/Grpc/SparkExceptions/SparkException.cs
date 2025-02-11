using Grpc.Core;

namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Helper exceptions for any errors we get 
/// </summary>
public class SparkException : Exception
{
    /// <summary>
    /// Implementing exceptions can override the default error message
    /// </summary>
    protected string OverrideMessage = "";

    
    /// <summary>
    /// Helper exceptions for any errors we get
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
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
    }

    /// <summary>
    /// Helper exceptions for any errors we get
    /// </summary>
    /// <param name="exception"></param>
    public SparkException(Exception exception) : base(exception.Message, exception)
    {
        if (exception is AggregateException && exception.InnerException is RpcException rpcException)
        {
            OverrideMessage = rpcException.Status.Detail;
        }
    }

    /// <summary>
    /// Helper exceptions for any errors we get
    /// </summary>
    /// <param name="message"></param>
    public SparkException(string message) : base(message)
    {
        OverrideMessage = message;
    }

    /// <summary>
    /// Error message
    /// </summary>
    public override string Message => OverrideMessage;
}