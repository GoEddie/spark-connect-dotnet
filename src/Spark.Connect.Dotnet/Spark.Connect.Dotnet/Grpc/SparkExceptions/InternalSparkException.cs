namespace Spark.Connect.Dotnet.Grpc;

public class InternalSparkException : SparkException{
    //Internal
    public InternalSparkException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        
    }

    public InternalSparkException(Exception exception) : base(exception)
    {
        
    }
}