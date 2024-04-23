using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Tests;

public class E2ETestBase
{
    protected static SparkSession Spark;
    protected readonly string OutputPath;

    public E2ETestBase()
    {
        var remoteAddress = Environment.GetEnvironmentVariable("SPARK_REMOTE") ?? "http://localhost:15002";
        Spark = SparkSession.Builder.Remote(remoteAddress).GetOrCreate();

        var tempFolder = Path.GetTempPath();

        OutputPath = Path.Join(tempFolder, "spark-connect-tests");
    }
}