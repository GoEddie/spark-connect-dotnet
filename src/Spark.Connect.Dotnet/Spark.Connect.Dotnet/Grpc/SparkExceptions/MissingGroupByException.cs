namespace Spark.Connect.Dotnet.Grpc;

public class MissingGroupByException : SparkException
{
    public MissingGroupByException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
    }

    public MissingGroupByException(Exception exception) : base(exception)
    {
    }
}