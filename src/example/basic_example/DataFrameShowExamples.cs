using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using static Spark.Connect.Dotnet.Sql.DataFrame;
using static Spark.Connect.Dotnet.Sql.Types.SparkDataType;

namespace basic_example;

public class DataFrameShowExamples
{
    private readonly SparkSession spark;

    public DataFrameShowExamples(SparkSession sparkSession)
    {
        spark = sparkSession;
    }
    
    public void Run()
    {
        var rows = ToRows(
                ToRow("Hello", 123, 1234d, 12345f, 1.23, DateTime.UtcNow),
                            ToRow("Hello 22", 123, 1234d, 12345f, 1.23, DateTime.UtcNow),
                            ToRow("Hello 33", 12345, 123445d, 1234567f, 1.23, DateTime.UtcNow)
            );
        
        var dataFrame = spark.CreateDataFrame(rows, "str_col", "int_col", "double_col", "float_col", "double2_col", "date_col");
        dataFrame.Show();
        
        spark.Sql(
                "SELECT * FROM {the_data_frame}", 
                    ("the_data_frame", dataFrame)
                ).Show(1000, 1000, true);
        
        var dataFrame2 = spark.CreateDataFrame(new (object, object)[]{("Hello", 1), ("Hello", 1), ("Hello", 1), ("Hello", 1), ("Hello", 1)} );
        var schema = dataFrame2.Schema;
        Console.WriteLine(schema.SimpleString());

        var dataFrame3 = spark.CreateDataFrame(new List<Dictionary<string, object>>()
        {
            new()
            {
                { "column____1", DateTime.UtcNow },{ "column____2", 100 },{ "column3", 100.1 },{ "column4", "c" }
            },
            new()
            {
                { "column____1", DateTime.UtcNow },{ "column____2", 100 },{ "column3", null },{ "column4", null }
            }
        });
        
        dataFrame3.Show();
        dataFrame3.PrintSchema();

        var dataFrame4 = spark.CreateDataFrame(new List<List<object>>()
        {
            new()
            {
                DateTime.Now, 100, 100.1, "c"
            },
            new()
            {
                DateTime.Now, 100, null, null
            }
        }, dataFrame3.Schema);
        
        dataFrame4.PrintSchema();
        dataFrame4.Show();
        
        var dataFrame5Schema = new StructType(new StructField("__1", TimestampNtzType(), false));
        var dataFrame5 = spark.CreateDataFrame(new List<List<object>>()
        {
            new()
            {
                DateTime.Now
            },
            new()
            {
                DateTime.Now
            }
        }, dataFrame5Schema);
        
        dataFrame5.PrintSchema();
        dataFrame5.Show();
    }
}