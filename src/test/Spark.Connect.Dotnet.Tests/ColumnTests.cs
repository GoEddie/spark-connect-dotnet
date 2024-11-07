using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests;

using static Functions;
using static SparkDataType;

public class ColumnTests : E2ETestBase
{
    public ColumnTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
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
        var df = Spark.Sql(
            "SELECT cast(null as string) as a, id from range(4) union SELECT 'aa' as a, id from range(100, 10)");
        df.Select(Col("a"), Col("a").IsNull()).Show();
    }
    
    [Fact]
    public void IsNotNullTest()
    {
        var df = Spark.Sql(
            "SELECT cast(null as string) as a, id from range(4) union SELECT 'aa' as a, id from range(100, 10)");
        df.Select(Col("a"), Col("a").IsNotNull()).Show();
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
        var df = Spark.Sql(
            "SELECT struct('a' as a, 'b' as b, 'c' as c, struct('da' as a, 'db' as b) as d) as a, id as idouter from range(5)");
        df.Select(df["a"].GetField("c")).Show();
        df.Select(df["a.d"].GetField("b")).Show();
    }

    [Fact]
    public void SubstrTest()
    {
        var df = Spark.Sql("SELECT 'ABCDEFHIJ' a from range(5)");
        df.Select(df["a"].Substr(2, 2)).Show();
    }

    [Fact]
    public void TypesTests()
    {
        var df = Spark.Sql("SELECT * FROM range(100)");
        var id = Col("id");

        df.Select(id * 1, id * 1F, id * 1d, id / 1, id / 1F, id / 1d, id % 1, id % 1F, id % 1d).Show();
    }

    [Fact]
    public void CollectTests()
    {
        var df = Spark.Sql("SELECT *, 'aaa' as col2, 334 * id as col3 FROM range(100)");

        foreach (var row in df.Collect())
        {
            Console.WriteLine(row.Get("col2"));
            Console.WriteLine(row[2]);

            Assert.Throws<IndexOutOfRangeException>(() => row.Get("FakeColumn"));
        }
    }

    [Fact]
    public void CollectTests_IntArray()
    {
        var df = Spark.Sql("SELECT array(id, 1, 2, 3) FROM range(100)");

        foreach (var row in df.Collect())
        {
            Console.WriteLine(row);
            Assert.NotNull(row[0]);
        }
    }

    [Fact]
    public void CollectTests_StringArray()
    {
        var df = Spark.Sql("SELECT array(\"ABC\", \"DEF\") FROM range(100)");

        foreach (var row in df.Collect())
        {
            Console.WriteLine(row);
            Assert.NotNull(row[0]);
            Assert.Equal("ABC", (row[0] as string[])[0]);
            Assert.Equal("DEF", (row[0] as string[])[1]);
        }
    }

    [Fact]
    public void IsInTests()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { (2, "Alice"), (5, "Bob") }, "age", "name");
        df.Select(Col("name"), df["name"].IsIn("Bob", "Mike")).Show();
        foreach (var row in df.Collect())
        {
            Console.WriteLine(row[0]);
        }
    }
    
    [Fact]
    public void CanFilter_AgainstStringNativeTypes()
    {
        var df = Spark.Range(10).WithColumn("S", Lit("a string"));
        df.Filter(Col("s") == "a string").Show();
    }
}