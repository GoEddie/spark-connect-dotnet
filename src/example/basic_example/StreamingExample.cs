using Spark.Connect.Dotnet.Sql;

class StreamingExample
{
    private readonly SparkSession spark;

    public StreamingExample(SparkSession sparkSession)
    {
        spark = sparkSession;
    }

    public void Run()
    {
        Console.WriteLine("Structured Streaming ");
        var sq = spark.ReadStream().Format("rate").Load().WriteStream().Format("memory").QueryName("this_query").Start();
        var sqm = spark.Streams;
        foreach (var query in sqm.Active.ToList())
        {
            query.Stop();
        }
    }
}