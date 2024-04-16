namespace Spark.Connect.Dotnet.Grpc;

public class UnresolvedRoutineException : SparkException
{
    //"UNRESOLVED_ROUTINE"
    
    public UnresolvedRoutineException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        
    }
}
public class UnavailableException : SparkException
{
    //"Unavailable"
    
    public UnavailableException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        if (exceptionMessage == "Error connecting to subchannel.")
        {
            OverrideMessage = "Cannot connect to remote Spark Server, if your $SPARK_REMOTE set correctly?";
        }
    }
}
