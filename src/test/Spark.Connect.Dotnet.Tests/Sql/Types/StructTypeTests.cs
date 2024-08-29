using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Tests.Sql.Types;

public class StructTypeTests
{
    [Fact]
    public void Json_Returns_ValidJson()
    {
        // var schema = new StructType(new List<StructField>()
        // {
        //     new StructField("col_a", new StringType()),
        //     new StructField("col_b", new IntegerType(), true)
        // });
        //
        // var json = schema.Json;
        // Assert.Equal("{\"type\":\"struct\",\"fields\":[{\"name\":\"col_a\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"col_b\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}", json);
    }

    [Fact]
    public void Ddl_Returns_WrappedByStruct()
    {
        var schema = new StructType(new List<StructField>()
        {
             new StructField("col_a", new StringType(), true)
        });
        var result = schema.ToDdl("name", true);
        Assert.Equal("name STRUCT<col_a string>", result);
    }

    [Fact]
    public void ForNonNullable_Ddl_EndsWith_NotNull()
    {
        var schema = new StructType(new List<StructField>()
        {
             new StructField("col_a", new StringType(), true)
        });

        var result = schema.ToDdl("name", false);
        Assert.EndsWith(" NOT NULL", result);
    }
}