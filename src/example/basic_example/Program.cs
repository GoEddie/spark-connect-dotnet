using Spark.Connect;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

var dataFrame = spark.Range(1000);
dataFrame.Show();

var dataFrame2 = dataFrame
    .WithColumn("Hello",
        Lit("Hello From Spark, via Spark Connect"));

dataFrame2.Show();

var dfFromSl = await spark.SqlAsync("SELECT *, id + 10, 'Hello' as abc, id * 10 m2 FROM range(100) union SELECT *, id + 10, 'Hello' as abc, id * 10 m2 FROM range(100)");
dfFromSl.Show();
dfFromSl.CreateOrReplaceTempView("table_a");

var dfFromView = await spark.SqlAsync("SELECT min(id), avg(m2) from table_a group by id");
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

dataFrame2
    .WithColumn("id multiplied by 1000", Functions.Col("id") * 1000)
    .Write()
    .Format("parquet")
    .Write(parquetPath);

Console.WriteLine($"Wrote Parquet to '{parquetPath}'");

Console.WriteLine("Sturctured Streaming ");

var sq = spark.ReadStream().Format("rate").Load().WriteStream().Format("memory").QueryName("this_query").Start();
var sqm = spark.Streams;
foreach (var query in sqm.Active.ToList())
{
    query.Stop();
}



Console.WriteLine("Finished");