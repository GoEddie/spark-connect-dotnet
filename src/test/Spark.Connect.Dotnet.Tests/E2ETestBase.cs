using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests;

public class E2ETestBase
{
    protected SparkSession Spark;
    protected readonly string OutputPath;
    protected readonly string RemotePath;

    protected ITestOutputHelper Logger;
    
    public E2ETestBase(ITestOutputHelper logger)
    {
        Logger = logger;
        
        RemotePath = Environment.GetEnvironmentVariable("SPARK_REMOTE") ?? "http://localhost:15002";
        Spark = SparkSession.Builder.Remote(RemotePath).GetOrCreate();
        Spark.Console = new TestOutputConsole(logger);
        var tempFolder = Path.GetTempPath();
        OutputPath = Path.Join(tempFolder, "spark-connect-tests");
        
    }
}