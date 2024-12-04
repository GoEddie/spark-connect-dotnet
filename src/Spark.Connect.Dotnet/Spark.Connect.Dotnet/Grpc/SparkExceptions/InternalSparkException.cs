namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Generic internal error
/// </summary>
public class InternalSparkException : SparkException
{
    //Internal
    /// <summary>
    /// Generic internal error
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
    public InternalSparkException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
    }

    /// <summary>
    /// Generic internal error
    /// </summary>
    /// <param name="exception"></param>
    public InternalSparkException(Exception exception) : base(exception)
    {
    }
}