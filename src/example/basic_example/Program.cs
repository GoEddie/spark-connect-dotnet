using basic_example;
using Spark.Connect.Dotnet.Sql;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

spark.Conf.Set("spark.connect.dotnet.grpclogging", "console");
spark.Conf.Set("spark.connect.dotnet.showmetrics", "true");

new DataFrameShowExamples(spark).Run();
await new SimpleExample(spark).Run();
new StreamingExample(spark).Run();
new CreateDataFrameExamples(spark).Run();