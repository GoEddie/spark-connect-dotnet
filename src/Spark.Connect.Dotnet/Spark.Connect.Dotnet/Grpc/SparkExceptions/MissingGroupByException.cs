namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Wrap MISSING_GROUP_BY
/// </summary>
public class MissingGroupByException : SparkException
{
    /// <summary>
    /// Wrap MISSING_GROUP_BY
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
    public MissingGroupByException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
    }

    /// <summary>
    /// Wrap MISSING_GROUP_BY
    /// </summary>
    /// <param name="exception"></param>
    public MissingGroupByException(Exception exception) : base(exception)
    {
    }
}