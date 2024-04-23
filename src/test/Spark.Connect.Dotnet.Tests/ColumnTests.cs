using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests;

using static Functions;
using static SparkDataType;

public class ColumnTests : E2ETestBase
{
    public ColumnTests(ITestOutputHelper testOutputHelper)
    {
    }

    [Fact]
    public void Cast_Works()
    {
        var source = Spark.Sql("SELECT id FROM RANGE(10)");
        Assert.IsType<BigIntType>(source.Select(Col("id")).Schema.Fields.FirstOrDefault()!.DataType);
        Assert.IsType<StringType>(source.Select(Col("id").Cast("string")).Schema.Fields.FirstOrDefault()!.DataType);
        Assert.IsType<StringType>(source.Select(Col("id").Cast(StringType())).Schema.Fields.FirstOrDefault()!.DataType);

        Assert.IsType<ShortType>(source.Select(Col("id").Cast(ShortType())).Schema.Fields.FirstOrDefault()!.DataType);
        Assert.IsType<BigIntType>(source.Select(Col("id").Cast(BigIntType())).Schema.Fields.FirstOrDefault()!.DataType);
        Assert.IsType<BigIntType>(source.Select(Col("id").Cast(LongType())).Schema.Fields.FirstOrDefault()!.DataType);
    }

    
    
    [Fact]
    public void IsNullTest()
    {
        var df = Spark.Sql("SELECT cast(null as string) as a, id from range(4) union SELECT 'aa' as a, id from range(100, 10)");
        df.Filter(Col("a").IsNull()).Show();
    }
    
    
    [Fact]
    public void BetweenTest()
    {
        var df = Spark.Sql("SELECT cast(null as string) as a, id from range(100)");
        df.Select(Col("id"), Col("id").Between(Lit(3), Lit(5))).Show();
        
    }
    
    [Fact]
    public void EndsWithTest()
    {
        var df = Spark.Sql("SELECT 'abcdef' as a, id from range(5) union SELECT 'abcdef1' as a, id from range(5, 10) ");
        df.Select(Col("id"), Col("a"), Col("a").EndsWith("def")).Show();
    }
    
    [Fact]
    public void DropFieldsTest()
    {
        var df = Spark.Sql("SELECT struct('id' as a, id as b, id as c, id as d) as a, id as idouter from range(5)");
        var dfDropped = df.WithColumn("a", df["a"].DropFields("b", "c"));
        df.Show();
        dfDropped.Show();
        
        df.PrintSchema(5);
        dfDropped.PrintSchema(5);
    }
    
    [Fact]
    public void GetFieldTest()
    {
        var df = Spark.Sql("SELECT struct('a' as a, 'b' as b, 'c' as c, struct('da' as a, 'db' as b) as d) as a, id as idouter from range(5)");
        df.Select(df["a"].GetField("c")).Show();
        df.Select(df["a.d"].GetField("b")).Show();
    }
    
    [Fact]
    public void SubstrTest()
    {
        var df = Spark.Sql("SELECT 'ABCDEFHIJ' a from range(5)");
        df.Select(df["a"].Substr(2, 2)).Show();
    }
}