using Spark.Connect.Dotnet.Sql;

internal class SimpleExample
{
    private readonly SparkSession spark;

    public SimpleExample(SparkSession sparkSession)
    {
        spark = sparkSession;
    }

    public async Task Run()
    {
        var dataFrame = spark.Range(1000);
        dataFrame.Show();

        var dataFrame2 = dataFrame
            .WithColumn("Hello",
                Functions.Lit("Hello From Spark, via Spark Connect"));

        dataFrame2.Show();

        var dfFromSl =
            spark.Sql(
                "SELECT *, id + 10, 'Hello' as abc, id * 10 m2 FROM range(100) union SELECT *, id + 10, 'Hello' as abc, id * 10 m2 FROM range(100)");
        dfFromSl.Show();
        dfFromSl.CreateOrReplaceTempView("table_a");

        var dfFromView = spark.Sql("SELECT min(id), avg(m2) from table_a group by id");
        dfFromView.Show();

        dataFrame = spark.Range(1000);
        dataFrame.Show();

        var tempFolder = Path.GetTempPath();

        var tempOutputFolder = Path.Join(tempFolder, "spark-connect-examples");
        if (Path.Exists(tempOutputFolder))
        {
            Directory.Delete(tempOutputFolder, true);
        }

        var jsonPath = Path.Join(tempOutputFolder, "jsonFile");

        dataFrame.Write().Json(jsonPath);
        Console.WriteLine($"Wrote Json to '{jsonPath}'");

        var parquetPath = Path.Join(tempOutputFolder, "parquetFile");

        var df3 = dataFrame2.WithColumn("id multiplied by 1000", Functions.Col("id") * 1000);
            
        df3
            .Write()
            .Format("parquet")
            .Write(parquetPath);

        Console.WriteLine($"Wrote Parquet to '{parquetPath}'");
        
        df3.ShowRelation();
    }
}