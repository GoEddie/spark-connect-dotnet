using Spark.Connect.Dotnet.Sql;

var spark = SparkSession.Builder.Profile("M1").UserName("Ed Elliott").UserId("ed.elliott@outlook.com").GetOrCreate();
var dataFrame = spark.Sql($"SELECT id, id as two, id * 188 as three FROM Range(10)");
var dataFrame2 = spark.Sql($"SELECT id, id as two, id * 188 as three FROM Range(20)");

var dataFrame3 = dataFrame.Union(dataFrame2);
dataFrame3.Show(1000);




