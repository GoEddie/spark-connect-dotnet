using basic_example;using Spark.Connect.Dotnet.Sql;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

new DataFrameShowExamples(spark).Run();
await new SimpleExample(spark).Run();
new StreamingExample(spark).Run();
