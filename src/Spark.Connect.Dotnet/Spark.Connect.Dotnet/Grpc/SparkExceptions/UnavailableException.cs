namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Wrap Unavailable
/// </summary>
public class UnavailableException : SparkException
{
    //"Unavailable"

    /// <summary>
    /// Wrap Unavailable
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
    public UnavailableException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        if (exceptionMessage == "Error connecting to subchannel.")
        {
            OverrideMessage = "Cannot connect to remote Spark Server, if your $SPARK_REMOTE set correctly?";
        }
    }
}