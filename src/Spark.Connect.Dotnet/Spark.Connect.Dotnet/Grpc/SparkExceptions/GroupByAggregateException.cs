namespace Spark.Connect.Dotnet.Grpc;

public class GroupByAggregateException : SparkException
{
    //GROUP_BY_AGGREGATE
    public GroupByAggregateException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
    }

    public GroupByAggregateException(Exception exception) : base(exception)
    {
    }
}