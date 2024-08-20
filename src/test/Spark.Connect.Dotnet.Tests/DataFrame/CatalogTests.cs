using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class CatalogTests : E2ETestBase
{
    public CatalogTests(ITestOutputHelper logger) : base(logger)
    {
    }

    [Fact]
    public void CurrentCatalog_Test()
    {
        var currentCatalog = Spark.Catalog.CurrentCatalog();
        Assert.Equal("spark_catalog", currentCatalog);
    }

    [Fact]
    public void DatabaseExists_Test()
    {
        var currentDatabase = Spark.Catalog.CurrentDatabase();
        Assert.True(Spark.Catalog.DatabaseExists(currentDatabase));
        Assert.False(Spark.Catalog.DatabaseExists("dsadadadafdsfasdfvdjsfhk"));
    }

    [Fact]
    public void GetDatabaseTest()
    {
        var currentDatabase = "default";
        Spark.Catalog.GetDatabase(currentDatabase);
    }

    [Fact]
    public void GetFunctionTest()
    {
        Spark.Sql("CREATE OR REPLACE FUNCTION my_func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'");
        var function = Spark.Catalog.GetFunction("my_func1");
        Assert.Equal("my_func1", function.name);
        Console.WriteLine(function.ToString());
    }

    [Fact]
    public void ListCatalogTest()
    {
        Console.WriteLine(Spark.Catalog.ListCatalogs());
    }
}