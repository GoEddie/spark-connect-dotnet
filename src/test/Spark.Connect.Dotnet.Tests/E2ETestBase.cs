using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Tests;

public class E2ETestBase
{
    protected static SparkSession Spark;
    protected readonly string OutputPath;
    protected readonly string RemotePath;
    public E2ETestBase()
    {
        RemotePath = Environment.GetEnvironmentVariable("SPARK_REMOTE") ?? "http://localhost:15002";
        Spark = SparkSession.Builder.Remote(RemotePath).GetOrCreate();

        var tempFolder = Path.GetTempPath();

        OutputPath = Path.Join(tempFolder, "spark-connect-tests");
    }
}