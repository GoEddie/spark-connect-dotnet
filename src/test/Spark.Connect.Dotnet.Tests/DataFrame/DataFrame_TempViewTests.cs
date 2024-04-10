namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class DataFrameTempViewTests : E2ETestBase
{
    [Fact]
    public void CreateOrReplaceTempViewTest()
    {
        var tableName = $"{nameof(CreateOrReplaceTempViewTest)}_{Guid.NewGuid().ToString().Substring(0, 5)}";
    
        var dataFrame = Spark.Sql("SELECT * FROM Range(100)");
        dataFrame.CreateOrReplaceTempView(tableName); 
        dataFrame.CreateOrReplaceTempView(tableName); //overwrite
        
        var rows = Spark.Sql($"SELECT * FROM {tableName}").Collect();
        
        Assert.Equal(100, rows.Count);
        Assert.Equal(0L, rows[0][0]);
    }
    
    [Fact]
    public void CreateTempViewTest()
    {
        var tableName = $"{nameof(CreateTempViewTest)}_{Guid.NewGuid().ToString().Substring(0, 5)}";
        
        var dataFrame = Spark.Sql("SELECT * FROM Range(100)");
        dataFrame.CreateTempView(tableName);
        
        var rows = Spark.Sql($"SELECT * FROM {tableName}").Collect();
        
        Assert.Equal(100, rows.Count);
        Assert.Equal(0L, rows[0][0]);

        Assert.Throws<AggregateException>(() => dataFrame.CreateTempView(tableName));
    }
    
    [Fact]
    public void CreateOrReplaceGlobalTempViewTest()
    {
        var tableName = $"{nameof(CreateOrReplaceGlobalTempViewTest)}_{Guid.NewGuid().ToString().Substring(0, 5)}";
        
        var dataFrame = Spark.Sql("SELECT * FROM Range(100)");
        dataFrame.CreateOrReplaceGlobalTempView(tableName);
        dataFrame.CreateOrReplaceGlobalTempView(tableName); //overwrite
        
        var rows = Spark.Sql($"SELECT * FROM global_temp.{tableName}").Collect();
        
        Assert.Equal(100, rows.Count);
        Assert.Equal(0L, rows[0][0]);
    }
    
    [Fact]
    public void CreateGlobalTempViewTest()
    {
        var tableName = $"{nameof(CreateGlobalTempViewTest)}_{Guid.NewGuid().ToString().Substring(0, 5)}";
        
        var dataFrame = Spark.Sql("SELECT * FROM Range(100)");
        dataFrame.CreateGlobalTempView(tableName);
        
        var rows = Spark.Sql($"SELECT * FROM global_temp.{tableName}").Collect();
        
        Assert.Equal(100, rows.Count);
        Assert.Equal(0L, rows[0][0]);

        Assert.Throws<AggregateException>(() => dataFrame.CreateGlobalTempView(tableName));
    }
}