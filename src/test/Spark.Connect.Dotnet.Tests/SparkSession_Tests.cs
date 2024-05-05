using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Tests;

public class SparkSession_Tests : E2ETestBase
{
    [Fact]
    public void CreateDataFrame_Test()
    {
        var data = new List<IList<object>>
        {
            new List<object>
            {
                "Hello", 123, "Bye", 99.9
            },
            new List<object>
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
            },
            new List<object>
            {
                null, null, null, null, null
            },
            new List<object>
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
        
        var df = Spark.CreateDataFrame(data).WithColumn("date", Functions.CurrentTimestamp());
        df.Show();
        foreach (var row in df.Collect())
        {
            Console.WriteLine($"ROW: {row}");
        }
        

    }
}