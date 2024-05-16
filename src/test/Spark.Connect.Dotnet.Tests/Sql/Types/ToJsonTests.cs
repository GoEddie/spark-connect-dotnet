
using Spark.Connect.Dotnet.Sql.Types;
using static Spark.Connect.Dotnet.Sql.Types.SparkDataType;

namespace Spark.Connect.Dotnet.Tests.Sql.Types;

public class ToJsonTests
{
    [Fact]
    public void StructType_SingleField_No_Metadata()
    {
        var field = new StructField("CoLuMn__a", IntegerType(), false);
        var schema = new StructType(field);
        var actual = DataTypeJsonSerializer.StructTypeToJson(schema); 
        Assert.Equal("{\"fields\":[{\"metadata\":{},\"name\":\"CoLuMn__a\",\"nullable\":false,\"type\":\"integer\"}],\"type\":\"struct\"}", actual);
    }

    [Fact]
    public void StructType_SingleField_Single_Metadata()
    {
        var field = new StructField("CoLuMn__a", IntegerType(), false, new Dictionary<string, object>(){{"KeY", 12}});
        var schema = new StructType(field);
        var actual = DataTypeJsonSerializer.StructTypeToJson(schema); 
        Assert.Equal("{\"fields\":[{\"metadata\":{\"KeY\":12},\"name\":\"CoLuMn__a\",\"nullable\":false,\"type\":\"integer\"}],\"type\":\"struct\"}", actual);
    }
    
    [Fact]
    public void StructType_SingleField_Multi_Metadata()
    {
        var field = new StructField("CoLuMn__a", StringType(), true, new Dictionary<string, object>(){{"KeY", 12}, {"SomeOtherValue", "hello"}});
        var schema = new StructType(field);
        var actual = DataTypeJsonSerializer.StructTypeToJson(schema); 
        Assert.Equal("{\"fields\":[{\"metadata\":{\"KeY\":12,\"SomeOtherValue\":\"hello\"},\"name\":\"CoLuMn__a\",\"nullable\":true,\"type\":\"string\"}],\"type\":\"struct\"}", actual);
    }
    
    [Fact]
    public void StructType_MultipleFields_Single_Metadata()
    {
        var fieldA = new StructField("a", IntegerType(), false, new Dictionary<string, object>(){{"KeY", 12}});
        var fieldB = new StructField("BBBBBB", DoubleType(), true, new Dictionary<string, object>(){{"A", 123}});
        var schema = new StructType(fieldA, fieldB);
        var actual = DataTypeJsonSerializer.StructTypeToJson(schema); 
        Assert.Equal("{\"fields\":[{\"metadata\":{\"KeY\":12},\"name\":\"a\",\"nullable\":false,\"type\":\"integer\"},{\"metadata\":{\"A\":123},\"name\":\"BBBBBB\",\"nullable\":true,\"type\":\"double\"}],\"type\":\"struct\"}", actual);
    }
    
    [Fact]
    public void StructType_MultipleFields_Multiple_Metadata()
    {
        var fieldA = new StructField("a", IntegerType(), false, new Dictionary<string, object>(){{"KeY", 12},{ "an", "a"}});
        var fieldB = new StructField("BBBBBB", DoubleType(), true, new Dictionary<string, object>(){{"A", 123},{ "an", "a"}});
        var schema = new StructType(fieldA, fieldB);
        var actual = DataTypeJsonSerializer.StructTypeToJson(schema); 
        Assert.Equal("{\"fields\":[{\"metadata\":{\"KeY\":12,\"an\":\"a\"},\"name\":\"a\",\"nullable\":false,\"type\":\"integer\"},{\"metadata\":{\"A\":123,\"an\":\"a\"},\"name\":\"BBBBBB\",\"nullable\":true,\"type\":\"double\"}],\"type\":\"struct\"}", actual);
    }
    
    [Fact]
    public void StructType_WithSingleStructType_Field()
    {
        var fieldA = new StructField("child", IntegerType(), false);
        var subStruct = new StructType(fieldA);
        
        var fieldB = new StructField("parent", subStruct, false);
        var schema = new StructType(fieldB);
        
        var actual = DataTypeJsonSerializer.StructTypeToJson(schema); 
        Assert.Equal("{\"fields\":[{\"metadata\":{},\"name\":\"parent\",\"nullable\":false,\"type\":{\"fields\":[{\"metadata\":{},\"name\":\"child\",\"nullable\":false,\"type\":\"integer\"}],\"type\":\"struct\"}}],\"type\":\"struct\"}", actual);
    }
    
    [Fact]
    public void StructType_WithSingleStructType_AndSimpleType_Field()
    {
        var fieldA = new StructField("child", IntegerType(), false);
        var subStruct = new StructType(fieldA);
        
        var fieldB = new StructField("parent", subStruct, false);
        var fieldC = new StructField("2", BooleanType(), true);
        var schema = new StructType(fieldB, fieldC);
        
        var actual = DataTypeJsonSerializer.StructTypeToJson(schema); 
        
        Assert.Equal("{\"fields\":[{\"metadata\":{},\"name\":\"parent\",\"nullable\":false,\"type\":{\"fields\":[{\"metadata\":{},\"name\":\"child\",\"nullable\":false,\"type\":\"integer\"}],\"type\":\"struct\"}},{\"metadata\":{},\"name\":\"2\",\"nullable\":true,\"type\":\"boolean\"}],\"type\":\"struct\"}", actual);
    }
    
    [Fact]
    public void ArrayType_Field()
    {
        var schema = new StructType(new StructField("a", ArrayType(IntegerType(), false), true));
        var actual = DataTypeJsonSerializer.StructTypeToJson(schema); 
        
        Assert.Equal("{\"fields\":[{\"metadata\":{},\"name\":\"a\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"integer\",\"type\":\"array\"}}],\"type\":\"struct\"}", actual);
    }
}