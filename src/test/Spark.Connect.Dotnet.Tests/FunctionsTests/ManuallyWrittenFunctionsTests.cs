using System.Collections;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using static Spark.Connect.Dotnet.Sql.Functions;
using static Spark.Connect.Dotnet.Sql.Types.SparkDataType;
using DateType = Spark.Connect.Dotnet.Sql.Types.DateType;
using StructType = Spark.Connect.Dotnet.Sql.Types.StructType;

namespace Spark.Connect.Dotnet.Tests.FunctionsTests;

public class ManuallyWrittenFunctionsTests : E2ETestBase
{
    private static readonly Dotnet.Sql.DataFrame Source = Spark.Sql(
        "SELECT array(id, id + 1, id + 2) as idarray, array(array(id, id + 1, id + 2), array(id, id + 1, id + 2)) as idarrayarray, cast(id as binary) as idbinary, cast(id as boolean) as idboolean, cast(id as int) as idint, id, id as id0, id as id1, id as id2, id as id3, id as id4, current_date() as dt, current_timestamp() as ts, 'hello' as str, 'SGVsbG8gRnJpZW5kcw==' as b64, map('k', id) as m, array(struct(1, 'a'), struct(2, 'b')) as data, '[]' as jstr, 'year' as year_string, struct('a', 1) as struct_  FROM range(100)");

    private static WindowSpec Window =   Dotnet.Sql.Window.OrderBy("id").PartitionBy("id");
    private static WindowSpec OtherWindow = new WindowSpec().OrderBy("id").PartitionBy("id");

    [Fact]
    public void DatePart_Test()
    {
        Source.Select(DatePart(Lit("YEAR"), "ts").Alias("year")).Show();
        Source.Select(DatePart(Lit("YEAR"), Col("ts")).Alias("year")).Show();
    }

    [Fact]
    public void Reflect_Test()
    {
        Source.Select(Reflect(Lit("java.util.UUID"), Lit("fromString"), Lit("60edd1e0-0c85-418f-af6c-3e4e5b1328f2")))
            .Show();
        Spark.Sql("select '60edd1e0-0c85-418f-af6c-3e4e5b1328f2' as uuid from range(10)")
            .Select(Reflect(Lit("java.util.UUID"), Lit("fromString"), Col("uuid"))).Show();
    }


    [Fact]
    public void JavaMethod_Test()
    {
        Source.Select(JavaMethod(Lit("java.util.UUID"), Lit("fromString"), Lit("60edd1e0-0c85-418f-af6c-3e4e5b1328f2")))
            .Show();
        Spark.Sql("select '60edd1e0-0c85-418f-af6c-3e4e5b1328f2' as uuid from range(10)")
            .Select(JavaMethod(Lit("java.util.UUID"), Lit("fromString"), Col("uuid"))).Show();
    }

    [Fact]
    public void Extract_Test()
    {
        Source.Select(Extract(Lit("YEAR"), "ts").Alias("year")).Show();
        Source.Select(Extract(Lit("YEAR"), Col("ts")).Alias("year")).Show();
    }

    //
    [Fact]
    public void TryToNumber_Test()
    {
        var source = Spark.Sql("SELECT '$78.12' as e from range(10)");
        source.Select(TryToNumber(source["e"], "$99.99")).Show();
        source.Select(TryToNumber("e", "$99.99")).Show();
    }

    [Fact]
    public void TryElementAt_Test()
    {
        Source.Select(TryElementAt("idarray", Lit(1))).Show();
        Source.Select(TryElementAt(Lit(new[] { 1, 2 }), Lit(1))).Show();
        Source.Select(TryElementAt(Col("idarray"), Lit(1))).Show();
    }

    [Fact]
    public void ToVarchar_Test()
    {
        var source = Spark.Sql("SELECT '$78.12' as e from range(10)");
        source.Select(ToVarchar(source["e"], "$99.99")).Show();
        source.Select(ToVarchar("e", "$99.99")).Show();
        source.Select(ToVarchar(Lit("$78.12"), Lit("$99.99"))).Show();
    }


    [Fact]
    public void SplitPart_Test()
    {
        var source = Spark.Sql("SELECT 'a|str|with|pipes' src, '|' as delimiter, 3 as part from range(10)");

        source.Select(SplitPart("src", "delimiter", "part")).Show();
        source.Select(SplitPart(Col("src"), Lit("|"), Lit(3))).Show();
        source.Select(SplitPart(Col("src"), Col("delimiter"), Col("part"))).Show();
        source.Select(SplitPart(Col("src"), Lit("|"), Col("part"))).Show();
        source.Select(SplitPart(Col("src"), Col("delimiter"), Lit(3))).Show();
    }


    [Fact]
    public void HistogramNumeric_Test()
    {
        Source.Select(HistogramNumeric("id", Lit(3))).Show();
        Source.Select(HistogramNumeric(Col("id"), Lit(3))).Show();
    }

    [Fact]
    public void XpathDouble_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(XpathDouble("x", Lit("//b"))).Show();
        source.Select(XpathDouble(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void Xpath_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(Xpath("x", Lit("//b"))).Show();
        source.Select(Xpath(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void XpathBoolean_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(XpathBoolean("x", Lit("//b"))).Show();
        source.Select(XpathBoolean(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void XpathNumber_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(XpathNumber("x", Lit("//b"))).Show();
        source.Select(XpathNumber(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void XpathFloat_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(XpathFloat("x", Lit("//b"))).Show();
        source.Select(XpathFloat(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void XpathInt_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(XpathInt("x", Lit("//b"))).Show();
        source.Select(XpathInt(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void XpathLong_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(XpathLong("x", Lit("//b"))).Show();
        source.Select(XpathLong(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void XpathShort_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(XpathShort("x", Lit("//b"))).Show();
        source.Select(XpathShort(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void XpathString_Test()
    {
        var source = Spark.Sql("SELECT '<a><b>c</b></a>' as x from range(10)");
        source.Select(XpathString("x", Lit("//b"))).Show();
        source.Select(XpathString(Col("x"), Lit("//b"))).Show();
    }

    [Fact]
    public void Sha2_Test()
    {
        Source.Select(Sha2("idbinary", Lit(0))).Show();
        Source.Select(Sha2(Lit(new byte[] { 0x10 }), Lit(0))).Show();
        Source.Select(Sha2(Col("idbinary"), Lit(0))).Show();
    }

    [Fact]
    public void Atan2_Test()
    {
        var source = Spark.Sql("select float(10) as f1, float(12) as f2 from range(10)");
        source.Select(Atan2("f1", 12.0F)).Show();
    }

    [Fact]
    public void Stack_Test()
    {
        Source.Select(Stack(Lit(10), Col("idint"))).Show();
        Source.Select(Stack(Lit(2), Lit(new[] { 100, 200, 300, 400 }))).Show();
    }

    [Fact]
    public void NamedStruct_Test()
    {
        Source.Select(NamedStruct(Lit("struct_"), Lit("id"))).Show();
    }

    [Fact]
    public void When_Test()
    {
        Source.Select(When(Col("id") == 2, "TWO")).Show();
    }

    [Fact]
    public void Otherwise_Test()
    {
        Source.Select(When(Col("id") == 2, "TWO").Otherwise(Lit("NOOOOOO"))).Show();

        try
        {
            Source.Select(Col("id").Otherwise(Lit("NOOOOOO"))).Show();
            Assert.Fail("Should have an exception");
        }
        catch (InvalidOperationException ex)
        {
            Assert.Equal("Otherwise() can only be applied on a Column previously generated by When()", ex.Message);
        }

        try
        {
            Source.Select(Upper(Col("id")).Otherwise(Lit("NOOOOOO"))).Show();
            Assert.Fail("Should have an exception");
        }
        catch (InvalidOperationException ex)
        {
            Assert.Equal(
                "Otherwise() can only be applied on a Column previously generated by When(), it looks like the previous function was 'Upper'",
                ex.Message);
        }

        try
        {
            Source.Select(When(Col("id") == 2, "two").Otherwise(Lit("NOOOOOO")).Otherwise(Lit("NOOOOOO"))).Show();
            Assert.Fail("Should have an exception");
        }
        catch (InvalidOperationException ex)
        {
            Assert.Equal(
                "Otherwise() can only be applied on a Column previously generated by When(), has Otherwise() already been called on the column?",
                ex.Message);
        }
    }

    [Fact]
    public void FormatNumber_Test()
    {
        Source.Select(FormatNumber("id", Lit(4))).Show();
        Source.Select(FormatNumber(Col("id"), Lit(4))).Show();
    }

    [Fact]
    public void MapContainsKey_Test()
    {
        Source.Select(MapContainsKey("m", Lit("a"))).Show();
        Source.Select(MapContainsKey(Col("m"), Lit("a"))).Show();
        Source.Select(MapContainsKey(Lit(new Dictionary<string, string> { { "a", "b" } }), Lit("a"))).Show();
    }
    
    [Fact]
    public void Expr_Test()
    {
        var str = "id = id";
        var col = Expr(str);
        Assert.Equal(str, col.Expression.ExpressionString.Expression);
    }

    [Fact]
    public void AddMonths_Test()
    {
        var row1 = new List<object>() { DateTime.Parse("2023-01-09"), 1L };
        var row2 = new List<object>() { DateTime.Parse("2023-01-09"), 2L };
        
        var df = Spark.CreateDataFrame( new IEnumerable<object>[]{row1, row2}, new StructType(new StructField("startDate", DateType(), false), new StructField("id", LongType(), false)));
        df.Select(AddMonths("startDate", 12)).Show();
        df.Select(AddMonths(Col("startDate"), 12)).Show();
        
        df.Select(AddMonths("startDate", Lit(12))).Show();
        df.Select(AddMonths(Col("startDate"), Lit(12))).Show();
        
        df.Select(AddMonths("startDate", Col("id"))).Show();
        df.Select(AddMonths(Col("startDate"), Col("id"))).Show();
    }
    
    [Fact]
    public void ApproxCountDistinct_Test()
    {
        var rows = ToRows(
            ToRow(1), ToRow(2), ToRow(3), ToRow(3), ToRow(3)
        );
        
        var df = Spark.CreateDataFrame( rows, new StructType(new StructField("id", IntType(), false)));
        df.Select(ApproxCountDistinct(Col("id"))).Show();
        df.Select(ApproxCountDistinct(Col("id"), 0.087)).Show();
        //
         df.Select(ApproxCountDistinct(Col("id"))).Show();
        df.Select(ApproxCountDistinct(Col("id"), Lit(0.12))).Show();
    }
    
    [Fact]
    public void ApproxPercentile_Test()
    {
        var key = (Col("id") % 3).Alias("key");
        var value = (Randn(42) + key * 10).Alias("value");

        var df = Spark.Range(0, 1000, 1, 1).Select(key, value);
        var percentage = 0.13F;
        var percentages = Lit(new[] { 0.25F, 0.5F, 0.75F });
        var accuracy = Lit(1000000L);
        df.Select(
            ApproxPercentile(Col("value"),percentages, accuracy)
        ).Show();
        
        df.Select(
            ApproxPercentile(Col("value"), percentage, 1000)
        ).Show();
    }

    [Fact]
    public void ArrayJoin_Test()
    {
        //python:
        //df = spark.createDataFrame([(["a", "b", "c"],), (["a", None],)], ['data'])
        
        //cs:
        // var df = Spark.CreateDataFrame(ToRows(
        //     ToRow(new[]{"a", "b", "c" }),
        //                 ToRow(new []{"a", null})
        //     ), new StructType(new StructField("data", ArrayType(StringType()), true)));

        //use sql until we can pass complex types to CreateDataFrame (need to add a ListType to the Arrow builder stuff)
        var df = Spark.Sql(@"
                SELECT array('a', 'b', 'c') as data union SELECT array('a', null) as data
        ");

        df.Show();
        df.Select(ArrayJoin(df["data"], ",").Alias("joined")).Show();
        df.Select(ArrayJoin(df["data"], ",", "WASNULL").Alias("joined")).Show();
        df.Select(ArrayJoin(df["data"], Lit(",")).Alias("joined")).Show();
        df.Select(ArrayJoin(df["data"], Lit(","), Lit("NO LONGER NULL")).Alias("joined")).Show();
        
    }
    
    [Fact]
    public void ArrayInsert_Test()
    {
        //use sql until we can pass complex types to CreateDataFrame (need to add a ListType to the Arrow builder stuff)
        var df = Spark.Sql(@"
                SELECT array('a', 'b', 'c') as data, 2 as pos, 'd' as what union SELECT array('c', 'b', 'a'), -2, 'd'
        ");

        df.Show();
        df.Select(ArrayInsert(df["data"], Col("pos"), Col("what")).Alias("joined")).Show();
        df.Select(ArrayInsert(df["data"], 5, Lit("hello")).Alias("data")).Show();
    }
    
    [Fact]
    public void ArrayRepeat_Test()
    {
        //use sql until we can pass complex types to CreateDataFrame (need to add a ListType to the Arrow builder stuff)
        var df = Spark.Sql(@"
                SELECT array('a', 'b', 'c') as data, 10 as count
        ");

        df.Show();
        df.Select(ArrayRepeat(df["data"], 54).Alias("repeated")).Show();
        df.Select(ArrayRepeat(df["data"], Col("count")).Alias("repeated")).Show();
    }
    
    [Fact]
    public void AssertTrue_Test()
    {
        //use sql until we can pass complex types to CreateDataFrame (need to add a ListType to the Arrow builder stuff)
        var df = Spark.CreateDataFrame(ToRows(ToRow(0, 1)), new StructType(
            new StructField("a", IntegerType(), false),
            new StructField("b", IntegerType(), false)
        ));

        df.Show();
        var exception = Assert.Throws<InternalSparkException>(() => df.Select(AssertTrue(df["a"] > df["b"], "a is smaller than b")).Show());
        Assert.Equal("a is smaller than b", exception.Message);
    }
    
    [Fact]
    public void Broadcast_Test()
    {
        //use sql until we can pass complex types to CreateDataFrame (need to add a ListType to the Arrow builder stuff)
        var df = Spark.CreateDataFrame(ToRows(ToRow(0, 1)), new StructType(
            new StructField("a", IntegerType(), false),
            new StructField("b", IntegerType(), false)
        ));
        
        var df2 = Spark.CreateDataFrame(ToRows(ToRow(0, 1)), new StructType(
            new StructField("a", IntegerType(), false),
            new StructField("b", IntegerType(), false)
        ));

        df.Join(df2, on: new []{"a"}, JoinType.Inner).Explain();
        Broadcast(df).Join(Broadcast(df2), on: new[] { "a" }, JoinType.Inner).Explain();
    }
    
    [Fact]
    public void BTrim_Test()
    {
        //use sql until we can pass complex types to CreateDataFrame (need to add a ListType to the Arrow builder stuff)
        var df = Spark.Sql(@"
                SELECT '@@HELLO@@' as data, '   HELLO    ' as data2
        ");

        df.Show();
        df.Select(BTrim(df["data"], "@")).Show();
        df.Select(BTrim(df["data2"])).Show();
    }
    
    [Fact]
    public void Bucket_Test()
    {
        var spark = SparkSession.Builder.Remote(RemotePath).Config("spark.sql.catalogImplementation", "hive")
            .GetOrCreate();

        var conf = spark.Conf.GetAll();
        foreach (var item  in conf)
        {
            Console.WriteLine($"key: {item.Key} value: {item.Value}");
        }
        
        var df = Spark.Range(100);
        Assert.Throws<AggregateException>(() => df.WriteTo("bucket_test").PartitionedBy(Bucket(10, Col("id"))).CreateOrReplace());
        
    }
    
    [Fact]
    public void ConcatWs_Test()
    {
        var df = Spark.Sql("SELECT 'abcd' as a, '123' as d");
        df.Select(ConcatWs("+", "a", "d")).Show();
        df.Select(ConcatWs("-", Col("a"), Col("d"))).Show();
    }
    
    
    
    
    
    static IEnumerable<IEnumerable<object>> ToRows(params object[] objects)
    {   //don't do new on List<>(){} otherwise the child objects get flattened
        return objects.Cast<IEnumerable<object>>().ToList();
    }

    static IEnumerable<object> ToRow(params object[] items) => items.ToList<object>();
}