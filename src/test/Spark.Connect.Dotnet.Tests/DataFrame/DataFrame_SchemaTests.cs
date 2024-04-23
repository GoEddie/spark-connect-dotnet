using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using static Spark.Connect.Dotnet.Sql.Types.SparkDataType;


namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class DataFrame_SchemaTests : E2ETestBase
{
    [Fact]
    public void Schema_SimpleString_Test()
    {
        var df1 = Spark.Range(0, 5).WithColumn("Name", Functions.Lit("ed"));
        var schema = df1.Schema.SimpleString();

        Assert.Equal("StructType<id:bigint,Name:string>", schema);
    }

    [Fact]
    public void Schema_ComplexType_SimpleString_Test()
    {
        var df1 = Spark.Sql("SELECT struct(id as id1, id as id2, id as id3) from range(100)");
        var schema = df1.Schema.SimpleString();

        Assert.Equal("StructType<struct(id AS id1, id AS id2, id AS id3):StructType<id1:bigint,id2:bigint,id3:bigint>>",
            schema);
    }

    [Fact]
    public void Schema_ArrayType_SimpleString_Test()
    {
        var df1 = Spark.Sql("SELECT array(01, 02, 03, 04, 05) as arr from range(100)");
        var schema = df1.Schema.SimpleString();

        Assert.Equal("StructType<arr:array<int>>", schema);
    }

    [Fact]
    public void Schema_ArrayOfStructType_SimpleString_Test()
    {
        var df1 = Spark.Sql(
            "SELECT array(struct(01 as a, 02 as b), struct(01 as a, 02 as b), struct(01 as a, 02 as b)) as arr from range(100)");
        var schema = df1.Schema.SimpleString();

        Assert.Equal("StructType<arr:array<StructType<a:int,b:int>>>", schema);
    }

    [Fact]
    public void Schema_StructOfStructType_SimpleString_Test()
    {
        var df1 = Spark.Sql("SELECT struct(struct(01 as a, 02 as b), struct(01 as a, 02 as b)) as arr from range(100)");
        var schema = df1.Schema.SimpleString();

        Assert.Equal("StructType<arr:StructType<col1:StructType<a:int,b:int>,col2:StructType<a:int,b:int>>>", schema);
    }

    [Fact]
    public void Schema_StructOfStructType__Test()
    {
        var df1 = Spark.Sql(
            "SELECT struct(struct(01 as a, 02 as b), struct(01 as a, 02 as b)) as col1 from range(100)");
        var schema = df1.Schema;
        var expected =
            new StructType(
                new StructField("col1",
                    StructType(new StructField("a", IntegerType(), false), new StructField("b", IntegerType(), false)),
                    false),
                new StructField("col2",
                    StructType(new StructField("a", IntegerType(), false), new StructField("b", IntegerType(), false)),
                    false)
            );

        var expectedSchema = new StructType(new StructField("col1", expected, false));
        Assert.Equal(expectedSchema.SimpleString(), schema.SimpleString());
    }
}