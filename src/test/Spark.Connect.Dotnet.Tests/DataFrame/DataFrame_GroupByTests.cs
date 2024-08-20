using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class DataFrameGroupByTests : E2ETestBase
{
    public DataFrameGroupByTests(ITestOutputHelper logger) : base(logger)
    {
    }

    [Fact]
    public void UnionTest()
    {
        var df1 = Spark.Range(0, 5).WithColumn("Name", Lit("ed"));
        var df2 = Spark.Range(0, 10).WithColumn("Name", Lit("fred"));
        var df3 = Spark.Range(0, 15).WithColumn("Name", Lit("jed"));

        var df4 = df1.Union(df2).Union(df3);
        var groupedData = df4.GroupBy(Col("Name"));
        df4.Collect();
    }
}