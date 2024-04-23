using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.FunctionsTests;

public class ManuallyWrittenFunctionsTests : E2ETestBase
{
    private static readonly Dotnet.Sql.DataFrame Source = Spark.Sql(
        "SELECT array(id, id + 1, id + 2) as idarray, array(array(id, id + 1, id + 2), array(id, id + 1, id + 2)) as idarrayarray, cast(id as binary) as idbinary, cast(id as boolean) as idboolean, cast(id as int) as idint, id, id as id0, id as id1, id as id2, id as id3, id as id4, current_date() as dt, current_timestamp() as ts, 'hello' as str, 'SGVsbG8gRnJpZW5kcw==' as b64, map('k', id) as m, array(struct(1, 'a'), struct(2, 'b')) as data, '[]' as jstr, 'year' as year_string, struct('a', 1) as struct_  FROM range(100)");

    private static Window Window = new Window().OrderBy("id").PartitionBy("id");

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
}