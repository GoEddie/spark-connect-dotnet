using Spark.Connect.Dotnet.Pipelines.Attributes;
using Spark.Connect.Dotnet.Sql;

namespace pipelines_example;

[DeclarativePipeline(DefaultDatabase = "streamingfun")]
public class StreamingPipeline
{
    [StreamingTable]
    public DataFrame BronzeRate(SparkSession spark)
    {
        return spark
            .ReadStream()
            .Format("rate")
            .Option("rowsPerSecond", "1")
            .Load();
    }
}

[DeclarativePipeline(DefaultDatabase = "streamingfun")]
public class PipelineWithTableInADifferentDatabase
{
   
    [MaterializedView(Name = "abc.def")]
    public DataFrame BooksRaw(SparkSession spark)
    {
        return spark
            .Range(1, 10).WithColumn("name", Functions.Lit("Eddie"));
    }
}