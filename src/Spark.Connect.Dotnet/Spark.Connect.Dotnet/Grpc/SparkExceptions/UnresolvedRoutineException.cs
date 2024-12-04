namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Wrap UNRESOLVED_ROUTINE
/// </summary>
public class UnresolvedRoutineException : SparkException
{
    //"UNRESOLVED_ROUTINE"

    /// <summary>
    /// Wrap UNRESOLVED_ROUTINE
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
    public UnresolvedRoutineException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
    }
}