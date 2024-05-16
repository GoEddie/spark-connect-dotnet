using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.Sql;

public class SqlFormatterTests : E2ETestBase
{
    public SqlFormatterTests(ITestOutputHelper logger) : base(logger)
    {
    }
    
    [Fact]
    public void SelectFromDataFrame_Test()
    {
        var df = Spark.Range(100);
        var output = SqlFormatter.Format("SELECT * FROM {df1}", new Dictionary<string, object>() { {"df1", df }});
        Spark.Sql(output).Show();
    }
    
    [Fact]
    public void SelectColumnFromDataFrame_Test()
    {
        var df = Spark.Range(100);
        var output = SqlFormatter.Format("SELECT {idCol} FROM {df1}", new Dictionary<string, object>() { {"df1", df }, {"idCol", df["id"]}});
        Spark.Sql(output).Show();
    }
    
    [Fact]
    public void SelectWhereColEqualsNumber_Test()
    {
        var df = Spark.Range(100);
        var output = SqlFormatter.Format("SELECT * FROM {df1} WHERE id = {three}", new Dictionary<string, object>() { {"df1", df }, {"three", 3}});
        Spark.Sql(output).Show();
    }
    
    [Fact]
    public void SelectWhereColEqualsString_Test()
    {
        var df = Spark.Range(100).WithColumn("str", Lit("hello"));
        var output = SqlFormatter.Format("SELECT * FROM {df1} WHERE str = {hello}", new Dictionary<string, object>() { {"df1", df }, {"hello", "hello"}});
        Spark.Sql(output).Show();
    }
    
}