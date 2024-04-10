using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Tests.Sql.Types;

public class StructTypeTests
{
    [Fact]
    public void Json_Returns_ValidJson()
    {
        var schema = new StructType(new List<StructField>()
        {
            new StructField("col_a", new StringType()),
            new StructField("col_b", new IntegerType(), true)
        });

        var json = schema.Json;
        Assert.Equal("{\"type\":\"struct\",\"fields\":[{\"name\":\"col_a\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"col_b\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}", json);
    }

    // [Fact]
    // public void FromJson_Returns_SturctType()
    // {
    //     StructType.FromJson(
    //         "{\"fields\":[{\"metadata\":\"hello\",\"name\":\"col_a\",\"nullable\":false,\"type\":\"string\"},{\"metadata\":\"\",\"name\":\"col_b\",\"nullable\":false,\"type\":\"string\"}],\"type\":\"struct\"}");
    // }
}