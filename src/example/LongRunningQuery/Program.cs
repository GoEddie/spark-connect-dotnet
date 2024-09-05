
using Spark.Connect.Dotnet.Sql;

var spark = SparkSession.Builder.Profile("M1").DatabricksWaitForClusterMaxTime(5).GetOrCreate();
spark.Conf.Set("spark.connect.dotnet.grpclogging", "console");
spark.Sql("SELECT REFLECT('java.lang.Thread', 'sleep', CAST(6000000 as BIGINT))").Show();

