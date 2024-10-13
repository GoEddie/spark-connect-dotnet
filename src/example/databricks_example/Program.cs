using Spark.Connect;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

//You will need to configure your databricks tokens and give it a profile name that we can use
// create a ~/.databrickscfg (you don't need any tools to create one: https://docs.databricks.com/en/dev-tools/cli/profiles.html)
// if you don't want to create a ~/.databrickscfg then you can pass the host, credentials, and cluster id to the spark builder:
// var spark = (SparkSession
//             .Builder
//                 .Token("bearertoken")
//                 .ClusterId("ClusterId")
//                 .Remote("https://databricksurl.com"));

// var spark = SparkSession.Builder.Profile("M1").DatabricksWaitForClusterMaxTime(5).GetOrCreate();

var spark = SparkSession.Builder.Remote(Environment.GetEnvironmentVariable("DATABRICKS_URL")).Token(Environment.GetEnvironmentVariable("DATABRICKS_TOKEN")).ClusterId(Environment.GetEnvironmentVariable("DATABRICKS_CLUSTERID")).DatabricksWaitForClusterMaxTime(10).GetOrCreate();
var dataFrame = spark.Sql("SELECT id, id as two, id * 188 as three FROM Range(10)");
var dataFrame2 = spark.Sql("SELECT id, id as two, id * 188 as three FROM Range(20)");

var dataFrame3 = dataFrame.Union(dataFrame2);
dataFrame3.Show(1000);

//Mix gRPC calls with DataFrame API calls - the gRPC  client is available on the SparkSession:
var plan = new Plan
{
    Root = new Relation
    {
        Sql = new SQL
        {
            Query = "SELECT * FROM Range(100)"
        }
    }
};

var executor = new RequestExecutor(spark, plan);
await executor.ExecAsync();
var dataFrameFromRelation = new DataFrame(spark, executor.GetRelation());
dataFrameFromRelation.Show();


spark.Conf.Set("spark.connect.dotnet.grpclogging", "console");

dataFrame.Select(Reflect(Lit("java.lang.Thread"), Lit("sleep"), Lit((long)160000))).Show();