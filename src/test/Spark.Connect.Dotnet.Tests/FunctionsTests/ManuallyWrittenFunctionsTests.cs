using System.Text;
using Apache.Arrow.Scalars;
using Newtonsoft.Json;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;
using static Spark.Connect.Dotnet.Sql.Types.SparkDataType;
using StructType = Spark.Connect.Dotnet.Sql.Types.StructType;
using static Spark.Connect.Dotnet.Sql.Types.StructType;
using static Spark.Connect.Dotnet.Sql.DataFrame;
using Column = Spark.Connect.Dotnet.Sql.Column;

namespace Spark.Connect.Dotnet.Tests.FunctionsTests;

public class ManuallyWrittenFunctionsTests : E2ETestBase
{
    private static WindowSpec TheWindow = Window.OrderBy("id").PartitionBy("id");
    private static WindowSpec OtherWindow = new WindowSpec().OrderBy("id").PartitionBy("id");

    private readonly Dotnet.Sql.DataFrame Source;

    public ManuallyWrittenFunctionsTests(ITestOutputHelper logger) : base(logger)
    {
        Source = Spark.Sql(
            "SELECT array(id, id + 1, id + 2) as idarray, array(array(id, id + 1, id + 2), array(id, id + 1, id + 2)) as idarrayarray, cast(cast(id as STRING) as binary) as idbinary, cast(id as boolean) as idboolean, cast(id as int) as idint, id, id as id0, id as id1, id as id2, id as id3, id as id4, current_date() as dt, current_timestamp() as ts, 'hello' as str, 'SGVsbG8gRnJpZW5kcw==' as b64, map('k', id) as m, array(struct(1, 'a'), struct(2, 'b')) as data, '[]' as jstr, 'year' as year_string, struct('a', 1) as struct_  FROM range(100)");
    }

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
        var source = Spark.Sql("SELECT '78.12' as e from range(10)");
        source.Select(ToVarchar(source["e"], "$99.99")).Show();
        source.Select(ToVarchar("e", "$99.99")).Show();
        source.Select(ToVarchar(Lit("78.12"), Lit("$99.99"))).Show();
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
        var row1 = new List<object> { DateTime.Parse("2023-01-09"), 1L };
        var row2 = new List<object> { DateTime.Parse("2023-01-09"), 2L };

        var df = Spark.CreateDataFrame(new IEnumerable<object>[] { row1, row2 },
            new StructType(new StructField("startDate", DateType(), false), new StructField("id", LongType(), false)));
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

        var df = Spark.CreateDataFrame(rows, new StructType(new StructField("id", IntType(), false)));
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
            ApproxPercentile(Col("value"), percentages, accuracy)
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
        try
        {
            df.Select(AssertTrue(df["a"] > df["b"], "a is smaller than b")).Show();
        }
        catch (Exception ex)
        {
            Assert.True(ex.InnerException.Message.Contains("a is smaller than b"));
            return;
        }

        Assert.Fail("Should have thrown an exception");
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

        df.Join(df2, new[] { "a" }).Explain();
        Broadcast(df).Join(Broadcast(df2), new[] { "a" }).Explain();
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


    public void Bucket_Test()
    {
        var spark = SparkSession.Builder.Remote(RemotePath).Config("spark.sql.catalogImplementation", "hive")
            .GetOrCreate();

        var conf = spark.Conf.GetAll();
        foreach (var item in conf)
        {
            Logger.WriteLine($"key: {item.Key} value: {item.Value}");
        }

        var df = Spark.Range(100);
        Assert.Throws<AggregateException>(() =>
            df.WriteTo("bucket_test").PartitionedBy(Bucket(10, Col("id"))).CreateOrReplace());
    }

    [Fact]
    public void ConcatWs_Test()
    {
        var df = Spark.Sql("SELECT 'abcd' as a, '123' as d");
        df.Select(ConcatWs("+", "a", "d")).Show();
        df.Select(ConcatWs("-", Col("a"), Col("d"))).Show();
    }

    [Fact]
    public void Conv_Test()
    {
        var df = Spark.CreateDataFrame(new[]
        {
            new[] { "010101" }
        }, "n");

        df = df.Select(Conv("n", 2, 16).Alias("hex"));

        var rows = df.Collect();
        Assert.Equal("15", rows.First()[0]);
        df.Show();
    }

    [Fact]
    public void ConvertTimeZone_Test()
    {
        var df = Spark.CreateDataFrame(new[] { new[] { "2015-04-08" } }, "dt");
        df.Select(ConvertTimezone(null, Lit("America/Los_Angeles"), df["dt"])).Show();
        df.Select(ConvertTimezone(Lit("UTC"), Lit("America/Los_Angeles"), df["dt"])).Show();
        df.Select(ConvertTimezone(null, Lit("America/Los_Angeles"), CurrentTimestamp())).Show();
    }

    [Fact]
    public void CreateMap_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { ("Alice", 2), ("Bob", 5) }, "name", "age");
        df.Select(CreateMap("name", "age").Alias("map")).Show();
        df.Select(CreateMap(Col("name"), df["age"]).Alias("map")).Show();
    }

    [Fact]
    public void DateAdd_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { ("2015-04-08", 2) }, "dt", "add");
        df.Select(df["*"], DateAdd(df["dt"], 1).Alias("next_date")).Show();
        df.Select(df["*"], DateAdd("dt", 1).Alias("next_date")).Show();
    }

    [Fact]
    public void DateSub_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { ("2015-04-08", 2) }, "dt", "add");
        df.Select(df["*"], DateSub(df["dt"], 1).Alias("last_date")).Show();
        df.Select(df["*"], DateSub("dt", 1).Alias("last_date")).Show();
    }

    [Fact]
    public void ToDate_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { ("2015-04-08", 2) }, "dt", "add");
        df.Select(df["*"], Month(ToDate(df["dt"])).Alias("last_date")).Show();
    }

    [Fact]
    public void DateTrunc_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { ("1997-02-28 05:02:11", 0) }, "dt", "ignore");
        df.Select(df["*"], DateTrunc("year", df["dt"]).Alias("year")).Show();
        df.Select(df["*"], DateTrunc("year", "dt").Alias("year")).Show();
    }

    [Fact]
    public void First_Test()
    {
        var df = Spark.CreateDataFrame(
            new List<(object, object)> { ("Alice", 2), ("Alice", 90), ("Alice", 23), ("Bob", 5), ("Alice", null) },
            "name", "age");
        df.Show();

        df = df.OrderBy(df["age"]);

        df.GroupBy(Col("name")).Agg(First("age")).OrderBy("name").Show();
    }

    [Fact]
    public void FormatString_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { (5, "hello") }, "a", "b");
        df.Select(FormatString("%d %s", df["a"], df["b"]).Alias("v")).Show();
        df.Select(FormatString("%d %s", df["a"], df["b"]).Alias("v")).Collect();
    }


    [Fact]
    public void RoundNoScale_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { (5.123, "hello") }, "a", "b");
        df.Select(Round("a")).Alias("v").Show();
        df.Select(Round("a")).Alias("v").Collect();
    }


    [Fact]
    public void FromCsv_Test()
    {
        var data = ToRows(ToRow("1,   2,3"), ToRow("100,200,300"));

        var df = Spark.CreateDataFrame(data, "value");
        df.Show();
        df.Select(FromCsv(df["value"], Lit("a INT, b INT, c INT")).Alias("csv")).Show();
        var rows = df.Select(FromCsv(df["value"], Lit("a INT, b INT, c INT")).Alias("csv")).Collect();
        var row = rows.First();
        Logger.WriteLine(row.ToString());
        df.Select(FromCsv(df["value"], Lit("a INT, b INT, c INT")).Alias("csv")).Show();
        df.Select(FromCsv(df["value"], Lit("a INT, b INT, c INT")).Alias("csv")).Collect();
    }

    [Fact]
    public void FromJson_Tests()
    {
        var data = ToRow(1, "{\"a\": 123}");
        var schema = StructType(StructField("a", IntegerType()));
        var df = Spark.CreateDataFrame(ToRows(data), "key", "value");

        df.Select(FromJson(df["value"], schema).Alias("json")).Show();
        var rows = df.Select(FromJson(df["value"], schema).Alias("json")).Collect();
        Assert.Equal(123, (rows[0][0] as object[])[0]);

        rows = df.Select(FromJson(df["value"], "a INT").Alias("json")).Collect();
        Assert.Equal(123, (rows[0][0] as object[])[0]);

        rows = df.Select(FromJson(df["value"], "MAP<STRING,INT>").Alias("json")).Collect();
        var dict = rows[0][0] as IDictionary<string, object>;
        Assert.NotNull(dict);
        Assert.Equal(123, dict["a"]);
    }

    [Fact]
    public void FromUtcTimestamp_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("1997-02-28 10:30:00", "JST")), "ts", "tz");
        df.Select(FromUtcTimestamp(df["ts"], Lit("PST")).Alias("local_time")).Collect();
        df.Select(FromUtcTimestamp(df["ts"], Lit("PST")).Alias("local_time")).Show();

        df.Select(FromUtcTimestamp(df["ts"], Col("tz")).Alias("local_time")).Collect();
        df.Select(FromUtcTimestamp(df["ts"], Col("tz")).Alias("local_time")).Show();
    }

    [Fact]
    public void Grouping_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("Alice", 2), ToRow("Bob", 5)), "name", "age");
        df.Cube("name").Agg(Grouping("name"), Sum("age")).Show();

        df.Cube("name").Agg(Grouping("name"), Sum("age")).OrderBy("name").Show();
        df.Cube("name").Agg(Grouping("name"), Sum("age")).OrderBy("name").Collect();
    }

    [Fact]
    public void JsonTuple_Test()
    {
        var data = ToRows(ToRow("1", "{\"f1\": \"value1\", \"f2\": \"value2\"}"), ToRow("2", "{\"f1\": \"value12\"}"));
        var df = Spark.CreateDataFrame(data, "key", "jstring");

        df.Select(df["key"], JsonTuple(df["jstring"], "f1", "f2")).Collect();
        df.Select(df["key"], JsonTuple(df["jstring"], "f1", "f2")).Show();
    }

    [Fact]
    public void Lag_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("a", 1),
            ToRow("a", 2),
            ToRow("a", 3),
            ToRow("b", 8),
            ToRow("b", 2)
        ), "c1", "c2");

        df.Show();

        var w = Window.PartitionBy("c1").OrderBy("c2");

        df.WithColumn("previos_value", Lag("c2").Over(w)).Show();
        df.WithColumn("previos_value", Lag("c2", 1, 0).Over(w)).Show();
        df.WithColumn("previos_value", Lag("c2", 2, -1).Over(w)).Show();

        df.WithColumn("previos_value", Lag("c2").Over(w)).Collect();
        df.WithColumn("previos_value", Lag("c2", 1, 0).Over(w)).Collect();
        df.WithColumn("previos_value", Lag("c2", 2, -1).Over(w)).Collect();
    }

    [Fact]
    public void Lead_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("a", 1),
            ToRow("a", 2),
            ToRow("a", 3),
            ToRow("b", 8),
            ToRow("b", 2)
        ), "c1", "c2");

        df.Show();

        var w = Window.PartitionBy("c1").OrderBy("c2");

        df.WithColumn("next_value", Lead("c2").Over(w)).Show();
        df.WithColumn("next_value", Lead("c2", 1, 0).Over(w)).Show();
        df.WithColumn("next_value", Lead("c2", 2, -1).Over(w)).Show();

        df.WithColumn("next_value", Lead("c2").Over(w)).Collect();
        df.WithColumn("next_value", Lead("c2", 1, 0).Over(w)).Collect();
        df.WithColumn("next_value", Lead("c2", 2, -1).Over(w)).Collect();
    }

    [Fact]
    public void Last_Test()
    {
        var df = Spark.Sql("SELECT  id from range(10)");
        df.Select(Last("id")).Show();

        var rows = df.Select(Last("id")).Collect();
        Assert.Equal(9L, rows[0][0]);
    }

    [Fact]
    public void Levenshtein_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow("Kitten", "Smitten"),
                ToRow("Kitten", "Sitting")
            ), "l", "r");

        df.Select(Levenshtein("l", "r").Alias("d")).Show();
        df.Select(Levenshtein("l", "r").Alias("d")).Collect();

        df.Select(Levenshtein("l", "r", 123).Alias("d")).Show();
        df.Select(Levenshtein("l", "r", 123).Alias("d")).Collect();

        df.Select(Levenshtein(df["l"], df["r"]).Alias("d")).Show();
        df.Select(Levenshtein(df["l"], df["r"]).Alias("d")).Collect();

        df.Select(Levenshtein(df["l"], df["r"], 123).Alias("d")).Show();
        df.Select(Levenshtein(df["l"], df["r"], 123).Alias("d")).Collect();
    }

    [Fact]
    public void Like_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow("Kitten", "%it%"),
                ToRow("Kitten", "%it%")
            ), "l", "r");

        df.Select(Like("l", "%it%").Alias("d")).Show();
        df.Select(Like("l", "%it%").Alias("d")).Collect();

        df.Select(Like(Col("l"), Lit("%it%")).Alias("d")).Show();
        df.Select(Like(Col("l"), Lit("%it%")).Alias("d")).Collect();

        df.Select(Like(Col("l"), Col("r")).Alias("d")).Show();
        df.Select(Like(Col("l"), Col("r")).Alias("d")).Collect();


        df.Select(Like("l", "%it%", "\\").Alias("d")).Show();
        df.Select(Like("l", "%it%", "\\").Alias("d")).Collect();

        df.Select(Like(Col("l"), Lit("%it%"), Lit("\\")).Alias("d")).Show();
        df.Select(Like(Col("l"), Lit("%it%"), Lit("\\")).Alias("d")).Collect();

        df.Select(Like(Col("l"), Col("r")).Alias("d")).Show();
        df.Select(Like(Col("l"), Col("r")).Alias("d")).Collect();
    }

    [Fact]
    public void Locate_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow("Kitten"),
                ToRow("Kitten")
            ), "l");

        df.Select(Locate("ten", "l").Alias("d")).Show();
        var locate = Locate("ten", "l", 1);
        df.Select(locate.Alias("d")).Show();
        df.Select(Locate("ten", "l", 100).Alias("d")).Show();
    }

    [Fact]
    public void LPad_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow("Kitten"),
                ToRow("Kitten")
            ), "l");

        df.Select(LPad("l", 100, "%|%").Alias("d")).Show();
        df.Select(LPad(df["l"], 100, "%|%").Alias("d")).Show();

        df.Select(LPad("l", 100, "%|%").Alias("d")).Collect();
        df.Select(LPad(df["l"], 100, "%|%").Alias("d")).Collect();
    }

    [Fact]
    public void RPad_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow("Kitten"),
                ToRow("Kitten")
            ), "r");

        df.Select(RPad("r", 100, "%|%").Alias("d")).Show();
        df.Select(RPad(df["r"], 100, "%|%").Alias("d")).Show();

        df.Select(RPad("r", 100, "%|%").Alias("d")).Collect();
        df.Select(RPad(df["r"], 100, "%|%").Alias("d")).Collect();
    }

    [Fact]
    public void MakeDtInterval_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow(1, 12, 30, 01.001001),
                ToRow(1, 12, 30, 01.001001)
            ), "day", "hour", "min", "sec");
        df.Select(MakeDtInterval(
            df["day"], df["hour"], df["min"], df["sec"]).Alias("r")).Show();
    }

    [Fact(Skip = "Need a SparkDataType for interval")]
    public void MakeDtInterval_Collect_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow(1, 12, 30, 01.001001),
                ToRow(1, 12, 30, 01.001001)
            ), "day", "hour", "min", "sec");
        df.Select(MakeDtInterval(
            df["day"], df["hour"], df["min"], df["sec"]).Alias("r")).Collect();
    }

    [Fact]
    public void MakeTimestamp_Test()
    {
        Spark.Conf.Set("spark.sql.session.timeZone", "America/Los_Angeles");
        var df = Spark.CreateDataFrame(ToRows(ToRow(2014, 12, 28, 6, 30, 45.887, "CET")), "year", "month", "day",
            "hour", "min", "sec", "timezone");

        df.Select(MakeTimestamp(
                df["year"], df["month"], df["day"], df["hour"], df["min"], df["sec"], df["timezone"]).Alias("r")
        ).Show();

        df.Select(MakeTimestamp(
                df["year"], df["month"], df["day"], df["hour"], df["min"], df["sec"], df["timezone"]).Alias("r")
        ).Collect();
    }

    [Fact]
    public void MakeTimestampLtz_Test()
    {
        Spark.Conf.Set("spark.sql.session.timeZone", "America/Los_Angeles");
        var df = Spark.CreateDataFrame(ToRows(ToRow(2014, 12, 28, 6, 30, 45.887, "CET")), "year", "month", "day",
            "hour", "min", "sec", "timezone");

        df.Select(MakeTimestampLtz(
                df["year"], df["month"], df["day"], df["hour"], df["min"], df["sec"], df["timezone"]).Alias("r")
        ).Show();

        df.Select(MakeTimestampLtz(
                df["year"], df["month"], df["day"], df["hour"], df["min"], df["sec"], df["timezone"]).Alias("r")
        ).Collect();
    }

    [Fact]
    public void Mask_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("AbCD123-@$#", 1), ToRow("abcd-EFGH-8765-4321", 1)), "data");
        var data = df["data"];
        df.Select(Mask(data).Alias("r")).Show();

        df.Select(Mask(data, Lit("Y")).Alias("r")).Show();

        df.Select(Mask(data, Lit("Y"), Lit("y")).Alias("r")).Show();

        df.Select(Mask(data, Lit("Y"), Lit("y"), Lit("d")).Alias("r")).Show();

        df.Select(Mask(data, Lit("Y"), Lit("y"), Lit("d"), Lit("*")).Alias("r")).Show();

        df.Select(Mask("data", null, "g", "D")).Show();
    }

    [Fact]
    public void MonthsBetween_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("1997-02-28 10:30:00", "1996-10-30")), "date1", "date2");
        df.Select(MonthsBetween(df["date1"], Col("date2")).Alias("months")).Show();
        df.Select(MonthsBetween("date1", "date2").Alias("months")).Show();
        df.Select(MonthsBetween(df["date1"], Col("date2"), false).Alias("months")).Show();
        df.Select(MonthsBetween("date1", "date2", false).Alias("months")).Show();

        df.Select(MonthsBetween(df["date1"], Col("date2")).Alias("months")).Collect();
        df.Select(MonthsBetween("date1", "date2").Alias("months")).Collect();
        df.Select(MonthsBetween(df["date1"], Col("date2"), false).Alias("months")).Collect();
        df.Select(MonthsBetween("date1", "date2", false).Alias("months")).Collect();
    }

    [Fact]
    public void NthValue_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)>
        {
            ("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)
        }, "c1", "c2");

        df.Show();

        var w = Window.PartitionBy("c1").OrderBy("c2");
        df.WithColumn("nth_value", NthValue("c2", 1).Over(w)).Show();
        df.WithColumn("nth_value", NthValue("c2", 1).Over(w)).Collect();

        df.WithColumn("nth_value", NthValue("c2", 2).Over(w)).Show();
        df.WithColumn("nth_value", NthValue("c2", 2).Over(w)).Collect();
    }

    [Fact]
    public void Ntile_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)>
        {
            ("a", 1), ("a", 2), ("a", 3), ("b", 8), ("b", 2)
        }, "c1", "c2");

        df.Show();

        var w = Window.PartitionBy("c1").OrderBy("c2");
        df.WithColumn("ntile", Ntile(2).Over(w)).Show();
        df.WithColumn("ntile", Ntile(2).Over(w)).Collect();

        df.WithColumn("ntile", Ntile(2).Over(w)).Show();
        df.WithColumn("ntile", Ntile(2).Over(w)).Collect();
    }

    [Fact]
    public void Overlay_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { ("SPARK_SQL", "CORE") }, "x", "y");

        df.Select(Overlay("x", "y", 7).Alias("overlayed")).Show();
        df.Select(Overlay("x", "y", 7).Alias("overlayed")).Collect();

        df.Select(Overlay("x", "y", 7, 0).Alias("overlayed")).Show();
        df.Select(Overlay("x", "y", 7, 0).Alias("overlayed")).Collect();

        df.Select(Overlay(Col("x"), df["y"], 7, 0).Alias("overlayed")).Show();
        df.Select(Overlay(Col("x"), df["y"], 7, 0).Alias("overlayed")).Collect();

        df.Select(Overlay(Col("x"), df["y"], 7).Alias("overlayed")).Show();
        df.Select(Overlay(Col("x"), df["y"], 7).Alias("overlayed")).Collect();

        df.Select(Overlay("x", "y", 7, 2).Alias("overlayed")).Show();
        df.Select(Overlay("x", "y", 7, 2).Alias("overlayed")).Collect();
    }

    [Fact]
    public void Percentile_Test()
    {
        var key = (Col("id") % 3).Alias("key");
        var value = (Randn(42) + key * 10).Alias("value");

        var df = Spark.Range(0, 1000, 1, 1).Select(key, value);

        df.Select(
            Percentile(Col("value"), Lit(new[] { 0.25F, 0.5F, 0.75F }), Lit(1)).Alias("quantiles")
        ).Show();

        df.GroupBy("key").Agg(
            Percentile("value", 0.5F, 1).Alias("median")
        ).Show();

        df.Select(
            Percentile(Col("value"), Lit(new[] { 0.25F, 0.5F, 0.75F }), Lit(1)).Alias("quantiles")
        ).Collect();

        df.GroupBy("key").Agg(
            Percentile("value", 0.5F, 1).Alias("median")
        ).Collect();

        df.Select(
            Percentile(Col("value"), Lit(new[] { 0.25F, 0.5F, 0.75F }), Lit(1)).Alias("quantiles")
        ).PrintSchema();

        df.GroupBy("key").Agg(
            Percentile("value", 0.5F, 1).Alias("median")
        ).PrintSchema();
    }

    [Fact]
    public void PercentileApprox_Test()
    {
        var key = (Col("id") % 3).Alias("key");
        var value = (Randn(42) + key * 10).Alias("value");

        var df = Spark.Range(0, 1000, 1, 1).Select(key, value);

        df.Select(
            PercentileApprox(Col("value"), Lit(new[] { 0.25F, 0.5F, 0.75F }), Lit(100)).Alias("quantiles")
        ).Show();

        df.GroupBy("key").Agg(
            PercentileApprox("value", 0.5F, 1000000).Alias("median")
        ).Show();

        df.Select(
            PercentileApprox(Col("value"), Lit(new[] { 0.25F, 0.5F, 0.75F }), Lit(100)).Alias("quantiles")
        ).Collect();

        df.GroupBy("key").Agg(
            PercentileApprox("value", 0.5F, 1000000).Alias("median")
        ).Collect();

        df.Select(
            PercentileApprox(Col("value"), Lit(new[] { 0.25F, 0.5F, 0.75F }), Lit(100)).Alias("quantiles")
        ).PrintSchema();

        df.GroupBy("key").Agg(
            PercentileApprox("value", 0.5F, 1000000).Alias("median")
        ).PrintSchema();
    }

    [Fact]
    public void ParseUrl_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("http://spark.apache.org/path?query=1", "QUERY", "query")), "a",
            "b", "c");
        df.Show();
        df.Select(ParseUrl("a", "b", "c").Alias("r")).Show();
        df.Select(ParseUrl("a", "b").Alias("r")).Show();

        var first = df.Select(ParseUrl("a", "b", "c").Alias("r")).Collect();
        var second = df.Select(ParseUrl("a", "b").Alias("r")).Collect();

        Assert.Equal("1", first[0][0]);
        Assert.Equal("query=1", second[0][0]);
    }

    [Fact]
    public void Position_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("bar", "foobarbar", 5)), "a", "b", "c");
        df.Show();
        df.Select(Position("a", "b", "c").Alias("r")).Show();
        df.Select(Position(Col("a"), df["b"]).Alias("r")).Show();
        Assert.Equal(7, df.Select(Position("a", "b", "c").Alias("r")).Collect()[0][0]);
        Assert.Equal(4, df.Select(Position(Col("a"), df["b"]).Alias("r")).Collect()[0][0]);
    }

    [Fact]
    public void Printf_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("aa%d%s", 123, "cc")), "a", "b", "c");
        df.Show();
        df.Select(PrintF("a", "b", "c").Alias("r")).Show();
        df.Select(PrintF(Col("a"), df["b"], df["c"]).Alias("r")).Show();

        Assert.Equal("aa123cc", df.Select(PrintF("a", "b", "c").Alias("r")).Collect()[0][0]);
    }

    [Fact]
    public void RaiseError_Test()
    {
        var df = Spark.Range(1);
        var exception = Assert.ThrowsAny<Exception>(() => df.Select(RaiseError("My error message")).Show());
        Assert.True(exception.Message.Contains("My error message") ||
                    exception.InnerException.Message.Contains("My error message"));
    }

    [Fact]
    public void RegexpExtract_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("100-200")), "str");
        df.Select(RegexpExtract("str", "(\\d+)-(\\d+)", 1)).Show();
        df.Select(RegexpExtract(Col("str"), "(\\d+)-(\\d+)", 1)).Collect();

        df.Select(RegexpExtract(Col("str"), Lit("(\\d+)-(\\d+)"), Lit(1))).Show();
        df.Select(RegexpExtract(Col("str"), Lit("(\\d+)-(\\d+)"), Lit(1))).Collect();

        df = Spark.CreateDataFrame(ToRows(ToRow("foo")), "str");
        df.Select(RegexpExtract("str", @"(\d+)", 1).Alias("d")).Show();
        df.Select(RegexpExtract("str", @"(\d+)", 1).Alias("d")).Collect();

        df = Spark.CreateDataFrame(ToRows(ToRow("aaaac")), "str");
        df.Select(RegexpExtract("str", @"(a+)(b)?(c)", 2).Alias("e")).Show();
        df.Select(RegexpExtract("str", @"(a+)(b)?(c)", 2).Alias("e")).Collect();
    }

    [Fact]
    public void RegexpExtractAll_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("100-200, 300-400")), "str");
        df.Select(RegexpExtractAll("str", "(\\d+)-(\\d+)", 1)).Show();
        df.Select(RegexpExtractAll(Col("str"), "(\\d+)-(\\d+)", 1)).Collect();
        df.Select(RegexpExtractAll(Col("str"), Lit("(\\d+)-(\\d+)"), Lit(1))).Show();
        df.Select(RegexpExtractAll(Col("str"), Lit("(\\d+)-(\\d+)"), Lit(1))).PrintSchema();
    }

    [Fact]
    public void RegexpExtractInstr_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("1a 2b 14m", "d+(a|b|m)")), "str", "regexp");
        df.Select(RegexpExtractInstr("str", "\\d+(a|b|m)", 1)).Show();
        df.Select(RegexpExtractInstr(Col("str"), "\\d+(a|b|m)", 1)).Collect();
        df.Select(RegexpExtractInstr(Col("str"), Col("regexp"), Lit(1))).Collect();
        df.Select(RegexpExtractInstr(Col("str"), Lit("(\\d+)-(\\d+)"), Lit(1))).Show();
        df.Select(RegexpExtractInstr(Col("str"), Lit("(\\d+)-(\\d+)"), Lit(1))).PrintSchema();
    }

    [Fact]
    public void RegexpReplace_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("100-200, 300-400", @"(\d+)", @"--")), "str", "pattern",
            "replacement");
        df.Select(RegexpReplace("str", @"(\d+)", @"--")).Show();
        df.Select(RegexpReplace(Col("str"), Col("pattern"), Col("replacement"))).Show();
    }

    [Fact]
    public void Replace_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("ABCabc", "abc", "DEF")), "a", "b", "c");
        df.Select(Replace(Col("a"), Col("b"), Col("c"))).Show();
        df.Select(Replace(Col("a"), Col("b"), Col("c"))).Show();
        df.Select(Replace(Col("a"), Lit("abc"), Lit("DEF"))).Show();
        df.Select(Replace(Col("a"), "abc", "DEF")).Show();

        df.Select(Replace("a", "b", "c")).Collect();
        df.Select(Replace(Col("a"), "b", "c")).Collect();
        df.Select(Replace(Col("a"), Lit("abc"), Lit("DEF"))).Collect();
        df.Select(Replace(Col("a"), "abc", "DEF")).Collect();
    }

    [Fact]
    public void SchemaOfCsv_Test()
    {
        var df = Spark.Range(1);
        var csvschema_df = df.Select(SchemaOfCsv(Lit("123|col2"), new Dictionary<string, object> { { "sep", "|" } })
            .Alias("csv"));
        csvschema_df.PrintSchema();
        csvschema_df.Show(truncate: 1000, vertical: true);
        var rows = csvschema_df.Collect();
        Assert.Equal("STRUCT<_c0: INT, _c1: STRING>", rows[0][0]);
        df.Select(SchemaOfCsv("1|a", new Dictionary<string, object> { { "sep", "|" }, { "header", "true" } })
            .Alias("csv")).Show();
    }

    [Fact]
    public void SchemaOfJson_Test()
    {
        var df = Spark.Range(1);
        var jsonschema_df = df.Select(SchemaOfJson(@"{""a"": 0, ""b"": [1,2,3,4]}").Alias("schema"));
        jsonschema_df.PrintSchema();
        jsonschema_df.Show(truncate: 1000, vertical: true);
        var rows = jsonschema_df.Collect();
        Assert.Equal("STRUCT<a: BIGINT, b: ARRAY<BIGINT>>", rows[0][0]);
        df.Select(SchemaOfCsv("{a: 1}", new Dictionary<string, object> { { "allowUnquotedFieldNames", true } })
            .Alias("schema")).Show();
    }

    [Fact]
    public void Sentences_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("This is an example sentence.")), "string");
        df.Select(Sentences(df["string"], Lit("en"), Lit("US"))).Show(truncate: 1000);

        df = Spark.CreateDataFrame(ToRows(ToRow("Hello world. How are you?")), "string");
        df.Select(Sentences(df["string"])).Show(truncate: 1000);
    }

    [Fact]
    public void Struct_Test()
    {
        var df = Spark.Range(10);
        df.Select(Struct(Lit("HELLO"), Col("id"), df["id"]).Alias("hey")).Show();
    }

    [Fact]
    public void Get_Test()
    {
        var df = Spark.Sql("SELECT array(\"a\", \"b\", \"c\") data, 1 index");
        df.Select(Get("data", 0)).Show();

        df.Select(Get(Col("data"), Col("index"))).Show();

        df.Select(Get("data", "index")).Show();
    }

    [Fact]
    public void Sequence_Test()
    {
        var df1 = Spark.CreateDataFrame(new (object, object)[] { (-200, 200) }, "c1", "c2");
        df1.Select(Sequence(Col("c1"), Col("c2")).Alias("r")).Show();
        df1.Select(Sequence(Col("c1"), Col("c2"), Lit(50)).Alias("r")).Show(vertical: true, truncate: 1000);
    }

    [Fact]
    public void SessionWindow_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow(DateTime.Parse("2016-03-11 09:00:07"), 1))).ToDf("date", "val");
        var w = df.GroupBy(SessionWindow("date", "5 seconds")).Agg(Sum("val").Alias("sum"));
        w.Select(w["session_window"]["start"].Cast("string").Alias("start"),
            w["session_window"]["end"].Cast("string").Alias("start"), Col("sum")).Show();

        w = df.GroupBy(SessionWindow(Col("date"), Lit("5 hours"))).Agg(Sum("val").Alias("sum"));
        w.Select(w["session_window"]["start"].Cast("string").Alias("start"),
            w["session_window"]["end"].Cast("string").Alias("stop"), Col("sum")).Show();
    }

    [Fact]
    public void Slice_Test()
    {
        var df = Spark.Range(1).WithColumn("x", Array(1, 2, 3))
            .Union(
                Spark.Range(1).WithColumn("x", Array(4, 5))).Drop("id");

        df.Select(Slice(df["x"], 2, 2).Alias("sliced")).Show();
    }

    [Fact]
    public void SortArray_Test()
    {
        var df = Spark.Range(1).WithColumn("data", Array(2, 1, null, 3)).Union(
            Spark.Range(1).WithColumn("data", Array(1))).Union(
            Spark.Range(1).WithColumn("data", Array()));

        df.Select(SortArray(df["data"]).Alias("r")).Show();
        df.Select(SortArray(df["data"], false).Alias("r")).Show();
    }

    [Fact]
    public void Split_Test()
    {
        var df = Spark.CreateDataFrame(new List<(object, object)> { ("oneAtwoBthreeC", 1) }, "s", "ignore");
        df.Select(Split("s", "[ABC]", 2)).Show();
        df.Select(Split("s", "[ABC]", -1)).Show();
    }

    [Fact]
    public void StrToMap_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("a:1,b:2,c:3")), "e");
        df.Select(StrToMap(df["e"], Lit(","), Lit(":").Alias("r"))).Show();
    }

    [Fact]
    public void Substr_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("Spark SQL", 5, 1)), "a", "b", "c");
        df.Select(Substr("a", "b", "c")).Show();
    }

    [Fact]
    public void Substring_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("Spark SQL", 5, 1)), "a", "b", "c");
        df.Select(Substring("a", 1, 2)).Show();
    }

    [Fact]
    public void SubstringIndex_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("a.b.c.d")), "s");
        df.Select(SubstringIndex(df["s"], ".", 2)).Show();
    }

    [Fact]
    public void ToBinary_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("abc")), "e");
        df.Select(ToBinary(df["e"], Lit("utf-8"))).Show();

        df = Spark.CreateDataFrame(ToRows(ToRow("414243")), "e");
        df.Select(ToBinary(df["e"])).Show();
    }

    [Fact]
    public void TryToBinary_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("abc")), "e");
        df.Select(TryToBinary(df["e"], Lit("utf-8"))).Show();

        df = Spark.CreateDataFrame(ToRows(ToRow("414243")), "e");
        df.Select(TryToBinary(df["e"])).Show();
    }

    [Fact]
    public void ToChar_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow(78.12)), "e");
        df.Select(ToChar(df["e"], Lit("$99.99"))).Show();
    }

    [Fact]
    public void ToNumber_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("$78.12")), "e");
        df.Select(ToNumber(df["e"], Lit("$99.99"))).Show();
    }

    [Fact]
    public void ToCsv_Test()
    {
        var df = Spark.Sql("SELECT 1 as key, struct( 2, \"alice\") as data");
        df.Select(Col("data"), ToCsv("data").Alias("csv")).Show();
        var dict = new Dictionary<string, object>
        {
            { "sep", "::" }, { "header", "true" }
        };

        df.Select(Col("data"), ToCsv("data", dict).Alias("csv")).Show();
    }

    [Fact]
    public void ToJson_Test()
    {
        var df = Spark.Sql(
            "SELECT 1 as key, struct( 2, \"alice\") as data union SELECT 2 as key, struct( 5, \"bert\") as dat");
        df.Select(Col("data"), ToJson("data").Alias("json")).Show();
        var dict = new Dictionary<string, object>
        {
            { "locale", "fr-FR" }
        };

        df.Select(Col("data"), ToJson("data", dict).Alias("json")).Show();
    }

    [Fact]
    public void ToTimestampLtz_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("2016-12-31")), "e");
        df.Select(ToTimestampLtz(Col("e")).Alias("r")).Show();

        df = Spark.CreateDataFrame(ToRows(ToRow("31-12-2016")), "e");
        df.Select(ToTimestampLtz(Col("e"), Lit("dd-MM-yyyy")).Alias("r")).Show();
    }

    [Fact]
    public void ToTimestampNtz_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("2016-12-31")), "e");
        df.Select(ToTimestampNtz(Col("e")).Alias("r")).Show();

        df = Spark.CreateDataFrame(ToRows(ToRow("31-12-2016")), "e");
        df.Select(ToTimestampNtz(Col("e"), Lit("dd-MM-yyyy")).Alias("r")).Show();
    }

    [Fact]
    public void ToUnixTimestamp_Test()
    {
        Spark.Conf.Set("spark.sql.session.timeZone", "America/Los_Angeles");
        var df = Spark.CreateDataFrame(ToRows(ToRow("2016-12-31")), "e");
        df.Select(ToUnixTimestamp(Col("e")).Alias("r")).Show();

        df = Spark.CreateDataFrame(ToRows(ToRow("31-12-2016")), "e");
        df.Select(ToUnixTimestamp(Col("e"), Lit("dd-MM-yyyy")).Alias("r")).Show();
        Logger.WriteLine($"timeZone: {Spark.Conf.Get("spark.sql.session.timeZone")}");
        Spark.Conf.Unset("spark.sql.session.timeZone");
        Logger.WriteLine($"timeZone: {Spark.Conf.Get("spark.sql.session.timeZone")}");
    }

    [Fact]
    public void ToUtcTimestamp_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("1997-02-28 10:30:00", "JST")), "ts", "tz");
        df.Select(ToUtcTimestamp("ts", "PST").Alias("r")).Show();

        df.Select(ToUtcTimestamp(Col("ts"), Lit("PST")).Alias("r")).Show();

        df.Select(ToUtcTimestamp(Col("ts"), Col("tz")).Alias("r")).Show();

        df.Select(ToUtcTimestamp(df["ts"], df["tz"]).Alias("r")).Show();
    }

    [Fact]
    public void Translate_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow("translate")), "a");
        df.Select(Translate("a", "rnlt", "123").Alias("r")).Show();
    }

    [Fact]
    public void WidthBucket_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(
            ToRow(5.3, 0.2, 10.6, 5),
            ToRow(-2.1, 1.3, 3.4, 3),
            ToRow(8.1, 0.0, 5.7, 4),
            ToRow(-0.9, 5.2, 0.5, 2)), "v", "min", "max", "n");

        df.Select(WidthBucket("v", "min", "max", "n")).Show();
    }

    [Fact]
    public void Window_Test()
    {
        var df = Spark.Sql("SELECT current_timestamp as date, 1 as val");
        var w = df.GroupBy(WindowFunction.Window("date", "5 seconds")).Agg(Sum("val").Alias("sum"));
        var output = w.Select(
            w["Window"]["start"].Cast("string").Alias("start"),
            w["Window"]["end"].Cast("string").Alias("end"),
            Col("sum")
        );

        output.Show();
        w.PrintSchema();
    }

    [Fact]
    public void WindowTime_Test()
    {
        var df = Spark.Sql("SELECT current_timestamp as date, 1 as val");
        var w = df.GroupBy(WindowFunction.Window("date", "5 seconds")).Agg(Sum("val").Alias("sum"));
        var output = w.Select(
            w["Window"]["start"].Cast("string").Alias("start"),
            WindowTime(w["Window"]).Cast("string").Alias("window_time"),
            Col("sum")
        );

        output.Show();
    }

    [Fact]
    public void CountMinSketch_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow(1),
                ToRow(2),
                ToRow(1)
            ), "data");

        var a = df.Agg(CountMinSketch(df["data"], Lit(0.5), Lit(0.5), Lit(1)).Alias("sketch"));
        a.Show(vertical: true, truncate: 1000);
    }

    [Fact]
    public void HllSketchAgg_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow(1),
                ToRow(2),
                ToRow(2),
                ToRow(3)
            ), "value");

        var a = df.Agg(HllSketchEstimate(HllSketchAgg("value")).Alias("distinct_cnt"));
        a.Show(vertical: true, truncate: 1000);
    }

    [Fact]
    public void HllUnion_Test()
    {
        var df = Spark.CreateDataFrame(
            ToRows(
                ToRow(1, 4),
                ToRow(2, 5),
                ToRow(2, 5),
                ToRow(3, 6)
            ), "v1", "v2");

        var a = df.Agg(
            HllSketchAgg("v1").Alias("sketch1"),
            HllSketchAgg("v2").Alias("sketch2")
        );

        var counts = a.WithColumn("distinct_cnt", HllSketchEstimate(HllUnion("sketch1", "sketch2")));
        counts.Show(vertical: true, truncate: 1000);
    }

    [Fact]
    public void AesDecrypt_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(
                ToRow("AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4", "abcdefghijklmnop12345678ABCDEFGH", "GCM",
                    "DEFAULT", "This is an AAD mixed into the input")),
            "input", "key", "mode", "padding", "aad");

        var data = df.Select(AesDecrypt(Unbase64(df["input"]), df["key"], df["mode"], df["padding"], df["aad"]))
            .Collect()[0][0];

        Logger.WriteLine(Encoding.UTF8.GetString(data as byte[]));
    }

    [Fact]
    public void TryAesDecrypt_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(
                ToRow("AAAAAAAAAAAAAAAAQiYi+sTLm7KD9UcZ2nlRdYDe/PX4", "abcdefghijklmnop12345678ABCDEFGH", "GCM",
                    "DEFAULT", "This is an AAD mixed into the input")),
            "input", "key", "mode", "padding", "aad");

        var data = df.Select(TryAesDecrypt(Unbase64(df["input"]), df["key"], df["mode"], df["padding"], df["aad"]))
            .Collect()[0][0];

        Logger.WriteLine(Encoding.UTF8.GetString(data as byte[]));


        data = df.Select(TryAesDecrypt(Unbase64(df["input"]), Lit("definetly not the key"), df["mode"], df["padding"],
            df["aad"])).Collect()[0][0];

        Logger.WriteLine($"Output: '{Encoding.UTF8.GetString(data as byte[])}'");
    }

    [Fact]
    public void AesEncrypt_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(
                ToRow("Spark", "abcdefghijklmnop12345678ABCDEFGH", "GCM", "DEFAULT", "000000000000000000000000",
                    "This is an AAD mixed into the input")),
            "input", "key", "mode", "padding", "iv", "aad");

        df.Select(Base64(AesEncrypt(df["input"], df["key"], df["mode"], df["padding"], ToBinary(df["iv"], Lit("Hex")),
            df["aad"]))).Show(truncate: 1000, vertical: true);

        df = Spark.CreateDataFrame(ToRows(ToRow("Spark SQL", "1234567890abcdef", "ECB", "PKCS")), "input", "key",
            "mode", "padding");

        df.Show();

        df.Select(AesEncrypt(df["input"], df["key"], df["mode"], df["padding"])).Show();

        var data = df.Select(AesDecrypt(
                AesEncrypt(df["input"], df["key"], df["mode"], df["padding"]),
                df["key"], df["mode"], df["padding"]).Alias("r")
        ).Collect();

        Logger.WriteLine(Encoding.UTF8.GetString(data[0][0] as byte[]));
    }

    [Fact]
    public void MakeInterval_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow(100, 11, 1, 1, 12, 30, 01.001001)), "year", "month", "week", "day",
            "hour", "min", "sec");
        df.Select(MakeInterval(df["year"], df["month"], df["week"], df["day"], df["hour"], df["min"], df["sec"]))
            .Show(truncate: 10000, vertical: true);
        df.Select(MakeInterval(df["year"], df["month"], df["week"], df["day"], df["hour"], df["min"]))
            .Show(truncate: 10000, vertical: true);
        df.Select(MakeInterval(df["year"], df["month"], df["week"], df["day"], df["hour"]))
            .Show(truncate: 10000, vertical: true);
        df.Select(MakeInterval(df["year"], df["month"], df["week"], df["day"])).Show(truncate: 10000, vertical: true);
        df.Select(MakeInterval(df["year"], df["month"], df["week"])).Show(truncate: 10000, vertical: true);
        df.Select(MakeInterval(df["year"], df["month"])).Show(truncate: 10000, vertical: true);
        df.Select(MakeInterval(df["year"])).Show(truncate: 10000, vertical: true);
    }

    [Fact]
    public void MakeYmInterval_Test()
    {
        var df = Spark.CreateDataFrame(ToRows(ToRow(2014, 12)), "year", "month");
        df.Select(MakeYmInterval(df["year"], df["month"])).Show(truncate: 10000);
        var data = df.Select(MakeYmInterval(df["year"], df["month"])).Collect();
        Logger.WriteLine($"data: '{((YearMonthInterval)data[0][0]).Months}'");
    }

    [Fact]
    public void TryToTimestamp_Test()
    {
        Source.Select(TryToTimestamp("id")).Show();
        Source.Select(TryToTimestamp(Lit("ABC"))).Show();
        Source.Select(TryToTimestamp(Col("id"))).Show();
    }

    [Fact]
    public void LongerPlan_Test()
    {
        var a = Spark.Range(100).Repartition(30).GroupBy("id").Count();
        var b = Spark.Range(100).WithColumn("id2", Col("id") % 5);
        var c = a.Join(b, a["id"] == b["id2"]);
        c.Collect();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void ParseJson_Test()
    {
        var df = Spark.Range(10).WithColumn("a", ParseJson(Lit("{\"abc\": 123}")));
        var rows = df.Collect();
        Logger.WriteLine(rows.Count().ToString());
        df.Show();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void TryParseJson_Test()
    {
        var df = Spark.Range(10).WithColumn("a", TryParseJson(Lit("cxcaal;dkcfas;l{\"abc\": 123}")));
        var rows = df.Collect();
        Logger.WriteLine(rows.Count().ToString());
        df.Show();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void VariantGet_Test()
    {
        var df = Spark.Range(10).WithColumn("a", ParseJson(Lit("{\"abc\": 123}")));
        df.WithColumn("abc", VariantGet(df["a"], "$.abc")).Show();
        df.WithColumn("abc", VariantGet(df["a"], "$.abc", "int")).Show();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void TryVariantGet_Test()
    {
        var df = Spark.Range(10).WithColumn("a", ParseJson(Lit("{\"abc\": 123}")));
        df.WithColumn("abc", TryVariantGet(df["a"], "$.abc", "int")).Show();
        df.WithColumn("abc", TryVariantGet(df["a"], "$.abc")).Show();

        df.WithColumn("abc", TryVariantGet(df["a"], "$.SNSNSSNSSNSNSNS", "int")).Show();
        df.WithColumn("abc", TryVariantGet(df["a"], "$.SNSNSNSNSNSNS")).Show();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void IsVariantNull_Test()
    {
        var df = Spark.Range(10).WithColumn("a", TryParseJson(Lit("{\"a\": null")));
        df.Show();
        df.Select(IsVariantNull("a")).Show();
        df.Select(IsVariantNull(Col("a"))).Show();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void SchemaOfVariant_Test()
    {
        var df = Spark.Range(1).WithColumn("a", ParseJson(Lit("{\"a\": 123.0}")));
       
        df.Select(SchemaOfVariant("a")).Show();
        df.Select(SchemaOfVariant(Col("a"))).Show(vertical: true, truncate: 1000);
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void SchemaOfVariantAgg_Test()
    {
        var df = Spark.Range(1).WithColumn("a", ParseJson(Lit("{\"a\": 123.0}")));
       
        df.Select(SchemaOfVariantAgg("a")).Show();
        df.Select(SchemaOfVariantAgg(Col("a"))).Show(vertical: true, truncate: 1000);
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void SchemaOfXml_Test()
    {
        var df = Spark.Range(1).WithColumn("x", Lit("<p><a>1</a><a>2</a></p>"));
        df.Select(SchemaOfXml("<p><a>1</a><a>2</a></p>")).Show();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void FromXml_Test()
    {
        var df = Spark.Range(1).WithColumn("x", Lit("<p><a>1</a><a>2</a></p>"));
        var schema = df.Select(SchemaOfXml("<p><a>1</a><a>2</a></p>"));
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void TryRemainder_Test()
    {
        var df = Spark.Range(10).WithColumnRenamed("id", "a").WithColumn("b", Lit(0));
        df.WithColumn("abc", TryRemainder(df["a"], df["b"])).Show();
        df.WithColumn("abc", TryRemainder("a", "b")).Show();
    }

    [Fact]
    public void Ceil_With_Scale_Test()
    {
        var df = Spark.Range(10).Select(Ceil(Lit(-0.1), Lit(5)));
        df.Show();

        df = Spark.Range(10).Select(Ceil("id", 5));
        df.Show();
    }

    [Fact]
    public void Floor_With_Scale_Test()
    {
        Source.Select(Floor("id", 23)).Show();
        Source.Select(Floor(Lit(180), Lit(23))).Show();
        Source.Select(Floor(Col("id"), Lit(1290))).Show();
    }
    
    [Fact]
    public void Round_With_Scale_Test()
    {
        Source.Select(Round(Col("id"), 23)).Show();
        Source.Select(Round(Lit(180), Lit(23))).Show();
        Source.Select(Round(Col("id"), Lit(1290))).Show();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Monthname_Test()
    {
        Spark.Sql("SELECT current_timestamp() as ct").Select(Monthname("ct")).Show();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Dayname_Test()
    {
        Spark.Sql("SELECT current_timestamp() as ct").Select(Dayname("ct")).Show();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void TimestampDiff_Test()
    {
        Spark.Sql("SELECT current_timestamp() as ct, current_timestamp()  as end").Select(TimestampDiff("MICROSECOND", "ct", "end")).Show();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void TimestampAdd_Test()
    {
        Spark.Sql("SELECT current_timestamp() as ct").Select(TimestampAdd("YEAR", 10000, "ct")).Show();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void SessionUser_Test()
    {
       Spark.Range(20).Select(SessionUser()).Show();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Collate_Test()
    {
        Spark.Conf.Set("spark.sql.collation.enabled", "true");  //is hidden behind feature flag
        Spark.Sql("SELECT 'abc' as c").Select(Collate("c", "UTF8_BINARY")).Show();
        Spark.Sql("SELECT 'abc' as c").Select(Collate(Col("c"), Lit("UTF8_BINARY"))).Show();
        Spark.Conf.Set("spark.sql.collation.enabled", "false");  //is hidden behind feature flag
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Collation_Test()
    {
        Spark.Conf.Set("spark.sql.collation.enabled", "true");  //is hidden behind feature flag
        Spark.Sql("SELECT 'abc' as c").Select(Collation("c")).Show();
        Spark.Sql("SELECT 'abc' as c").Select(Collation("c")).Show();
        Spark.Conf.Set("spark.sql.collation.enabled", "false");  //is hidden behind feature flag
    }
}
