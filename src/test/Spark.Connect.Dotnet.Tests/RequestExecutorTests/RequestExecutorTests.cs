using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.RequestExecutorTests;

public class RequestExecutorTests : E2ETestBase
{

    public RequestExecutorTests(ITestOutputHelper logger) : base(logger)
    {
    }

    [Fact(Skip = "Pauses for 2 mins - run when testing RequestExecutor")]
    public void Initial_Request_Takes_Longer_Than_Timeout()
    {
        Spark.Conf.Set(SparkDotnetKnownConfigKeys.GrpcLogging, "console");
        Spark.Range(1).Select(Reflect(Lit("java.lang.Thread"), Lit("sleep"), Lit((long)160000))).Show();
    }
}