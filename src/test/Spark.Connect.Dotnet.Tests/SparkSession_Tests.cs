using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests;

public class SparkSession_Tests : E2ETestBase
{
    public SparkSession_Tests(ITestOutputHelper logger) : base(logger)
    {
    }

    [Fact]
    public void CreateDataFrame_Test()
    {
        var data = new List<IList<object>>
        {
            new List<object>
            {
                "Hello", 123, "Bye", 99.9
            }
            , new List<object>
            {
                "dsdsd", 12333, "sds", 1239.9
            }
        };

        var df = Spark.CreateDataFrame(data, new StructType(
            new StructField("string_a", SparkDataType.StringType(), false),
            new StructField("int_a", SparkDataType.IntType(), false),
            new StructField("string_b_for_bertie", SparkDataType.StringType(), false),
            new StructField("int_B", SparkDataType.DoubleType(), false)
        ));

        df.Show();

        var dicts = new List<Dictionary<string, object>>();
        dicts.Add(new Dictionary<string, object>
        {
            { "abc", 199 }, { "def", 120 }, { "dewewf", "120" }
        });

        dicts.Add(new Dictionary<string, object>
        {
            { "abc", 194343439 }, { "def", 1434320 }, { "dewewf", "120" }
        });

        df = Spark.CreateDataFrame(dicts);
        df.Show();

        df = Spark.CreateDataFrame(new List<(object, object)> { ("BING", 99), ("Pow", 12345), ("PedWow", 12345) });
        df.Show();
    }

    [Fact]
    public void CreateDataFrame_WithNulls_Test()
    {
        var data = new List<IList<object>>
        {
            new List<object>
            {
                "Hello", 123, "Bye", 99.9, 10000323232L
            }
            , new List<object>
            {
                null, null, null, null, null
            }
            , new List<object>
            {
                "Hello9999", -123, "Bye9999", -99.9, -1000L
            }
        };

        var schema = new StructType(
            new StructField("string_a", SparkDataType.StringType(), true),
            new StructField("int_a", SparkDataType.IntType(), true),
            new StructField("string_b_for_bertie", SparkDataType.StringType(), true),
            new StructField("int_B", SparkDataType.DoubleType(), true),
            new StructField("date", SparkDataType.LongType(), true)
        );

        var df = Spark.CreateDataFrame(data).WithColumn("date", CurrentTimestamp());
        df.Show();
        foreach (var row in df.Collect())
        {
            Console.WriteLine($"ROW: {row}");
        }
    }

    [Fact]
    public void SqlWithDict_Test()
    {
        var df = Spark.Range(100);
        var dict = new Dictionary<string, object>
        {
            ["c"] = df["id"], //could do Col("id") etc
            ["dataFramePassedIn"] = df
            , ["three"] = 3
        };

        Spark.Sql("SELECT {c} FROM {dataFramePassedIn} WHERE {c} = {three}", dict).Show();
        var rows = Spark.Sql("SELECT {c} FROM {dataFramePassedIn} WHERE {c} = {three}", dict).Collect();
        Assert.Equal(1, rows.Count);
        Assert.Equal(3L, rows[0][0]);
    }

    [Fact]
    public void SqlWithDataFrames_Test()
    {
        var df = Spark.Range(100);
        var df2 = Spark.Range(1000, 2000);

        Spark.Sql("SELECT * FROM {dataFramePassedIn} union all SELECT * FROM {anotherDataFramePassedIn}",
            ("dataFramePassedIn", df), ("anotherDataFramePassedIn", df2)).Show();
        var rows = Spark.Sql("SELECT * FROM {dataFramePassedIn} union all SELECT * FROM {anotherDataFramePassedIn}",
            ("dataFramePassedIn", df), ("anotherDataFramePassedIn", df2)).Collect();
        Assert.Equal(1100, rows.Count);
        Assert.Equal(0L, rows[0][0]);
    }
    
    
    [Fact]
    public void SqlWithDataFramesJoin_Test()
    {
        var df = Spark.Range(100);
        var df2 = Spark.Range(0, 2000);

        Spark.Sql("SELECT * FROM {dataFramePassedIn} a LEFT OUTER JOIN {anotherDataFramePassedIn} b on a.id = b.id",
            ("dataFramePassedIn", df), ("anotherDataFramePassedIn", df2)).Show();
        var rows = Spark
            .Sql("SELECT * FROM {dataFramePassedIn} a LEFT OUTER JOIN {anotherDataFramePassedIn} b on a.id = b.id",
                ("dataFramePassedIn", df), ("anotherDataFramePassedIn", df2)).Collect();
        Assert.Equal(100, rows.Count);
        Assert.Equal(1L, rows[1][0]);
        Assert.Equal(1L, rows[1][1]);
    }

    [Fact]
    public void SqlTestForDocs()
    {
        var df = Spark.Range(1).WithColumn("new_column", Lit(1)).WithColumn("another_col", Lit("hi"))
            .Select(Col("another_col"));
        Logger.WriteLine(df.Relation.ToString());
    }

    [Fact]
    public void InterruptAll_Test()
    {
        Spark.InterruptAll();
    }
}