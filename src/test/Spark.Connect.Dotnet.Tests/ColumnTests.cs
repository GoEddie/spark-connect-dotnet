using System.Runtime.InteropServices.JavaScript;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests;
using static Spark.Connect.Dotnet.Sql.Functions;
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
}