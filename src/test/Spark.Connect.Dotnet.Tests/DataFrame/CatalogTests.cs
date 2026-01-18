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
        var database = Spark.Catalog.GetDatabase(currentDatabase);
        Assert.Equal(database.name, currentDatabase);
    }

    [Fact]
    public void GetFunctionTest()
    {
        Spark.Sql("CREATE OR REPLACE FUNCTION my_func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'");
        var function = Spark.Catalog.GetFunction("my_func1");
        Assert.Equal("my_func1", function.name);
        Assert.Equal(new[]{"default"}, function.namesSpace);
    }
    
    [Fact]
    public void ListFunctionTest()
    {
        Spark.Sql("CREATE OR REPLACE FUNCTION my_func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'");
        Spark.Sql("CREATE OR REPLACE FUNCTION my_func2 AS 'test.org.apache.spark.sql.MyDoubleAvg'");
        Spark.Sql("CREATE OR REPLACE FUNCTION my_func3 AS 'test.org.apache.spark.sql.MyDoubleAvg'");

        var functions = Spark.Catalog.ListFunctions();
        Assert.Contains(functions, f => f.name == "my_func1");
        Assert.Contains(functions, f => f.name == "my_func2");
        Assert.Contains(functions, f => f.name == "my_func3");
    }
    
    [Fact]
    public void ListDatabasesTest()
    {
        
        Spark.Sql("CREATE DATABASE IF NOT EXISTS db1");
        Spark.Sql("CREATE DATABASE IF NOT EXISTS db2");
        Spark.Sql("CREATE DATABASE IF NOT EXISTS db3");
        
        var databases = Spark.Catalog.ListDatabases();
        Assert.Contains(databases, f => f.name == "db1");
        Assert.Contains(databases, f => f.name == "db2");
        Assert.Contains(databases, f => f.name == "db3");
    }
    
    [Fact]
    public void ListColumnsTest()
    {
        var tableName = $"columns_test_{Guid.NewGuid().ToString().Replace("-", "")}";
        Spark.Sql("SELECT 1 as i, 2.0 as f, 'test' as str, current_timestamp() as now").Write().SaveAsTable(tableName, null, "overwrite");

        var columns = Spark.Catalog.ListColumns(tableName);
        Assert.Contains(columns, f => f.name == "i");
        Assert.Contains(columns, f => f.name == "f");

        Assert.Contains(columns, f => f.name == "now");
    }
    
    [Fact]
    public void GetTableTest()
    {
        var tableName = $"my_table_{Guid.NewGuid().ToString().Replace("-", "")}";
        Spark.Range(100).Write().SaveAsTable(tableName, "parquet", "overwrite");
        var table = Spark.Catalog.GetTable(tableName);
        Assert.Equal(tableName.ToLower(), table.name);
        Assert.Equal(new[]{"default"}, table.nameSpace);
    }

    
    [Fact]
    public void ListTablesTest()
    {
        var suffix = Guid.NewGuid().ToString().Replace("-", "");
        var tableName1 = $"my_table1_{suffix}";
        var tableName2 = $"my_table2_{suffix}";
        var tableName3 = $"my_table3_{suffix}";

        Spark.Range(100).Write().SaveAsTable(tableName1, "parquet", "overwrite");
        Spark.Range(100).Write().SaveAsTable(tableName2, "parquet", "overwrite");
        Spark.Range(100).Write().SaveAsTable(tableName3, "csv", "overwrite");

        var table = Spark.Catalog.ListTables(Spark.Catalog.CurrentDatabase());

        Assert.Contains(table, p => p.name == tableName1.ToLower());
        Assert.Contains(table, p => p.name == tableName2.ToLower());
        Assert.Contains(table, p => p.name == tableName3.ToLower());
    }

 
    
    [Fact]
    public void TableExistsTest()
    {
        var tableName = $"my_table_{Guid.NewGuid().ToString().Replace("-", "")}";
        Spark.Range(100).Write().SaveAsTable(tableName, "parquet", "overwrite");

        var exists = Spark.Catalog.TableExists(tableName);
        Assert.True(exists);

        exists = Spark.Catalog.TableExists("IDONTEXIST");
        Assert.False(exists);
    }

    [Fact]
    public void ListCatalogTest()
    {
        var catalogs = Spark.Catalog.ListCatalogs();
        Assert.Contains(catalogs, f => f.name == "spark_catalog");
    }

    [Fact]
    public void IsCachedTest()
    {
        var tableName = Guid.NewGuid().ToString().Replace("-", "");
        Spark.Range(100).Write().SaveAsTable(tableName, null, "overwrite");
        Spark.Catalog.IsCached(tableName);
        Assert.False(Spark.Catalog.IsCached(tableName));
        
        Spark.Catalog.CacheTable(tableName, new StorageLevel(){ UseDisk = true});
        Assert.True(Spark.Catalog.IsCached(tableName));
    }
}