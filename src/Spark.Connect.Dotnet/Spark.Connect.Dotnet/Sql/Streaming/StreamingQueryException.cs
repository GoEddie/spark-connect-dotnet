using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql.Streaming;

public class StreamingQueryException : SparkException
{
    public StreamingQueryException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
    }

    public StreamingQueryException(Exception exception) : base(exception)
    {
    }

    public StreamingQueryException(string message) : base(message)
    {
    }
}