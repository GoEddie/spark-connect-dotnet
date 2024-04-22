using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class DataFrame_Tests : E2ETestBase
{
    [Fact]
    public void AliasTest()
    {
        var df1 = Spark.Range(0, 5).Alias("bob");
        df1.Select(Col("bob.id")).Show();
    }

    [Fact]
    public void PersistTest()
    {
        var df1 = Spark.Range(0, 5).Cache();
        df1.Show();
    }

    [Fact]
    public void PartitionTest()
    {
        Spark.Range(1).Coalesce().Show();
        Spark.Range(1).Coalesce(1).Show();
        Spark.Range(1).Repartition(10, Col("id")).Show();
        Spark.Range(1).Repartition(Col("id")).Show();
    }

    [Fact]
    public void ColRegexTest()
    {
        var df = Spark.Sql("SELECT 'a' as Col1, id as Col2 from range(100)");
        df.Select(df.ColRegex("`(Col1)?+.+`")).Show();
    }

    [Fact]
    public void ColumnsTest()
    {
        var df = Spark.Sql("SELECT 'a' as Col1, id as Col2 from range(100)");
        Assert.Equal(new List<string> { "Col1", "Col2" }, df.Columns);
    }

    [Fact]
    public void CorrTest()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)");
        Assert.Equal(1F, df.Corr("col1", "col2"));
    }

    [Fact]
    public void CovTest()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)");
        Assert.Equal(841, (int)df.Cov("col1", "col2"));
    }


    [Fact]
    public void CrossTabTest()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)").CrossTab("col1", "col2");
        df.Show();
    }

    [Fact]
    public void CubeTest()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)")
            .Cube("col1", "col2")
            .Agg(Count(Col("Col2")));

        df.Show();
    }

    [Fact]
    public void VersionTest()
    {
        var df = Spark.Version();
        Console.WriteLine($"SPARK Version: {df}");
    }
}