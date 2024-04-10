namespace Spark.Connect.Dotnet.Sql;

[NotImplemented]
public class GroupedData
{
    public readonly SparkSession _session;
    public readonly Relation _relation;
    protected internal readonly Relation Relation;
    
    public GroupedData(SparkSession session, Relation relation)
    {
        _session = session;
        _relation = relation;
    }
}