using Spark.Connect;
using Spark.Connect.Dotnet.Sql;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

var dataFrame = spark.Range(1000);
dataFrame.Show();

var dataFrame2 = dataFrame
    .WithColumn("Hello",
        Functions.Lit("Hello From Spark"));

dataFrame2.Show();

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

Console.WriteLine("Finished");