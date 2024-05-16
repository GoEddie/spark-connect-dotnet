using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class DataFrameUnionTests : E2ETestBase
{
    public DataFrameUnionTests(ITestOutputHelper logger) : base(logger)
    {
    }
    
    [Fact]
    public void UnionTest()
    {
        var df1 = Spark.Range(0, 10);
        var df2 = Spark.Range(11, 20);

        var df3 = df1.Union(df2);
        Assert.Equal(19, df3.Count());

        df3.Collect();
    }

    [Fact]
    public void UnionAll()
    {
        var df1 = Spark.Range(0, 10);
        var df2 = Spark.Range(0, 11);

        var df3 = df1.Union(df2);
        Assert.Equal(11, df3.Count());
    }

    [Fact]
    public void UnionByName()
    {
        var df1 = Spark.Range(0, 10).WithColumn("ABC", Functions.Lit("SomeText"));
        var df2 = Spark.Range(11, 20).WithColumn("ABC", Functions.Lit("SomeText"));

        df1 = df1.Select(df1["ABC"], df1["id"]);

        var df3 = df1.UnionByName(df2, false);
        Assert.Equal(19, df3.Count());

        df3.Collect();
    }
}