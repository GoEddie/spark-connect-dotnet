namespace Spark.Connect.Dotnet.Grpc;


/// <summary>
/// Wrap GROUP_BY_AGGREGATE
/// </summary>
public class GroupByAggregateException : SparkException
{
    //GROUP_BY_AGGREGATE
    /// <summary>
    /// Wrap GROUP_BY_AGGREGATE
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
    public GroupByAggregateException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
    }

    /// <summary>
    /// Wrap GROUP_BY_AGGREGATE
    /// </summary>
    /// <param name="exception"></param>
    public GroupByAggregateException(Exception exception) : base(exception)
    {
    }
}