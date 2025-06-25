using Spark.Connect.Dotnet.Pipelines.Attributes;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Tests.Pipelines;

[DeclarativePipeline]
public class SimplestDeclarativePipeline()
{
    [PipelineMaterializedView(Name = "GoldOutputTable")]
    public Dotnet.Sql.DataFrame Gold(SparkSession spark)
    {
        var df = spark.Read.Table("Silver");
        df = df.Select(Functions.Sum(Functions.Col("metrics")));
        return df;
    }
    
    [PipelineTable]
    public Dotnet.Sql.DataFrame Bronze(SparkSession spark)
    {
        var df = spark.Range(100).WithColumn("metrics", Functions.Rand());
        df.PrintSchema();
        df.Show();
        return df;
    }
    
    [PipelineTable]
    public Dotnet.Sql.DataFrame Silver(SparkSession spark)
    {
        var df = spark.Read.Table("Bronze");
        df = df.WithColumn("load_time", Functions.CurrentTimestamp());
        return df;
    }
}