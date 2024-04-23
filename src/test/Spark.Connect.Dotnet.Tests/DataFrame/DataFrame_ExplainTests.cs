using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class DataFrameExplainTests : E2ETestBase
{
    [Fact]
    public void DefaultExplain()
    {
        var df1 = Spark.Range(0, 5).WithColumn("Name", Lit("ed"));
        var response = df1.Explain();

        Assert.Contains("== Physical Plan ==\n", response);
        Assert.Contains("Range (0, 5, step=1, splits=1)", response);
    }
}