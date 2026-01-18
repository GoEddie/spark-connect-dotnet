using Spark.Connect.Dotnet.Pipelines.Attributes;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Tests.Pipelines;

[DeclarativePipeline(Storage = "file:///tmp/spark-pipelines-test")]
public class SimplestDeclarativePipeline()
{
    [MaterializedView(Name = "GoldOutputTable")]
    public Dotnet.Sql.DataFrame Gold(SparkSession spark)
    {
        var df = spark.Range(100).WithColumn("metrics", Functions.Col("id") * 123.12);
        df = df.Select(Functions.Sum(Functions.Col("metrics")));
        return df;
    }
    
    [StreamingTable]
    public Dotnet.Sql.DataFrame Bronze(SparkSession spark)
    {
        var df = spark.ReadStream().Format("rate").Load().WithColumn("source", Functions.Lit("StreamingTable::Bronze"));
        return df;
    }
    
    [StreamingTable]
    public Dotnet.Sql.DataFrame Silver(SparkSession spark)
    {
        var df = spark.ReadStream().Format("rate").Load().WithColumn("source", Functions.Lit("StreamingTable::Silver"));
        return df;
    }
}