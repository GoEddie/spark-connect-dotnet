using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests;

public class E2ETestBase
{
    protected readonly string OutputPath;
    protected readonly string RemotePath;

    protected ITestOutputHelper Logger;
    protected SparkSession Spark;

    public E2ETestBase(ITestOutputHelper logger)
    {
        Logger = logger;

        RemotePath = Environment.GetEnvironmentVariable("SPARK_REMOTE") ?? "http://localhost:15002";
        Spark = SparkSession.Builder.Remote(RemotePath).GetOrCreate();
        Spark.Console = new TestOutputConsole(logger);
        var tempFolder = Path.GetTempPath();
        OutputPath = Path.Join(tempFolder, "spark-connect-tests");
        
        Spark.Conf.Set("spark.sql.ansi.enabled", "false"); //stricter parsing in 4 breaks the tests - TODO make tests ansi compliant
    }
}