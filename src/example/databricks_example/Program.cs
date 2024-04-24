using Spark.Connect.Dotnet.Sql;

//You will need to configure your databricks tokens and give it a profile name that we can use
// create a ~/.databrickscfg (you don't need any tools to create one: https://docs.databricks.com/en/dev-tools/cli/profiles.html)
// if you don't want to create a ~/.databrickscfg then you can pass the host, credentials, and cluster id to the spark builder:
// var spark = (SparkSession
//             .Builder
//                 .Token("bearertoken")
//                 .ClusterId("ClusterId")
//                 .Remote("https://databricksurl.com"));



var spark = SparkSession.Builder.Profile("M1").DatabricksWaitForClusterMaxTime(2).GetOrCreate();
var dataFrame = spark.Sql("SELECT id, id as two, id * 188 as three FROM Range(10)");
var dataFrame2 = spark.Sql("SELECT id, id as two, id * 188 as three FROM Range(20)");

var dataFrame3 = dataFrame.Union(dataFrame2);
dataFrame3.Show(1000);
