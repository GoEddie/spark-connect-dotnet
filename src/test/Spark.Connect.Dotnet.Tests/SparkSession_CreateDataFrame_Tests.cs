using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests;

public class SparkSession_CreateDataFrame_Tests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    public void CreateDataFrame_WithDenseVector_Test()
    {
        var rawData = new List<(int Id, int? abc, DenseVector Vector)>()
        {
            (1, 3, new DenseVector([0.0, 1.1, 0.1])), (2, null, new DenseVector([2.0, 1.0, -1.0])), (3, 99, new DenseVector([2.0, 1.3, 1.0]))
            , (4, null, new DenseVector([0.0, 1.2, -0.5]))
        };

        var df = Spark.CreateDataFrame(rawData.Cast<ITuple>());
        df.Show();
        df.PrintSchema();
    }
    
    [Fact]
    public void CreateDataFrame_WithSparseVector_Test()
    {
        var rawData = new List<(int Id, int? abc, SparseVector Vector)>()
        {
            (1, 3, new SparseVector(5, [0, 1, 3], [0.0, 1.1, 0.1])),
            (2, null, new SparseVector(5, [0, 1,3], [0.0, 1.1, 0.1])),
            (3, 99, new SparseVector(5, [1,2, 3], [0.0, 1.1, 0.1])), 
            (4, null, new SparseVector(5, [0,1,4], [0.0, 1.1, 0.1]))
        };

        var df = Spark.CreateDataFrame(rawData.Cast<ITuple>());
        df.Show(4, 1000);
        df.PrintSchema();
    }

    [Fact]
    public void CreateDataFrame_WithSchema_Test()
    {
        var rawData = new List<(int Id, int? abc, DenseVector Vector)>()
        {
            (1, 3, new DenseVector([0.0, 1.1, 0.1])), 
            (2, null, new DenseVector([2.0, 1.0, -1.0])), 
            (3, 99, new DenseVector([2.0, 1.3, 1.0])), 
            (4, null, new DenseVector([0.0, 1.2, -0.5]))
        };

        var schema = new StructType(
            new List<StructField>()
            {
                new StructField("id99999999HSHSHSHSHSHSH", SparkDataType.IntType(), false), new StructField("abc", SparkDataType.IntType(), true)
                , new StructField("vector", new VectorUDT(), false)
            }
        );

        var df = Spark.CreateDataFrame(rawData.Cast<ITuple>(), schema);  //remove tolist
        df.Show();
        df.PrintSchema();
    }
    
    
    [Fact]
    public void CreateDataFrameWithStruct_Test()
    {
        var schema  = new StructType(new[]
        {
            new StructField("Id", new IntegerType(), true),
            new StructField("Name", new StringType(), true),
            new StructField("Details", new StructType(new[]
            {
                new StructField("JoinDate", new DateType(), true),
                new StructField("IsActive", new BooleanType(), true),
                new StructField("Position", new StructType(new[]
                {
                    new StructField("Department", new StringType(), true),
                    new StructField("Level", new IntegerType(), true),
                    new StructField("Employment", new StructType(new[]
                    {
                        new StructField("Salary", new DecimalType(10, 2), true),
                        new StructField("Currency", new StringType(), true),
                        new StructField("Benefits", new StructType(new[]
                        {
                            new StructField("VacationDays", new IntegerType(), true),
                            new StructField("HasHealthInsurance", new BooleanType(), true),
                            new StructField("InsuranceProvider", new StringType(), true)
                        }), true)
                    }), true)
                }), true)
            }), true)
        });
        var df = Spark.CreateDataFrame(deeplyNestedData.Cast<ITuple>(), schema);
        df.Show();
        df.PrintSchema();
    }

    [Fact]
    public void CreateDataFrameWithAllTypes_Test()
    {
        var data
            = new List<(int id, short ids, byte b, string s, bool bo, DateTime dt, decimal d, float f, double dbl, long l, byte[] bytes, char c, Guid guid, DateTimeOffset
                dto)>
            {
                (1, 1, 1, "1", true, new DateTime(2020, 1, 1), 1.0m, 1.0f, 1.0, 1, new byte[] { 1, 2, 3 }, '1', Guid.NewGuid()
                    , new DateTimeOffset(2020, 1, 1, 1, 1, 1, TimeSpan.Zero))
                ,(2, 2, 2, "2", false, new DateTime(2020, 2, 2), 2.0m, 2.0f, 2.0, 2, new byte[] { 2, 2, 2 }, '2', Guid.NewGuid()
                    , new DateTimeOffset(2020, 2, 2, 2, 2, 2, TimeSpan.Zero))
                ,(3, 3, 3, "3", true, new DateTime(2020, 3, 3), 3.0m, 3.0f, 3.0, 3, new byte[] { 3, 3, 3 }, '3', Guid.NewGuid()
                    , new DateTimeOffset(2020, 3, 3, 3, 3, 3, TimeSpan.Zero))
            };
        
        var df = Spark.CreateDataFrame(data.Cast<ITuple>());
        // df.Write().Parquet("/Users/ed/tmp/arrow/ouput.parquet");
        df.Show();
        df.PrintSchema();
       
    }

    [Fact]
    public void CreateDataFrameWithList_Test()
    {
        var schema = new StructType(new[]
        {
            new StructField("Id", new IntegerType(), true),
            new StructField("data", new ArrayType(new IntegerType(), true), true),
        });

        var data = new List<(int id, int[] Id)>()
        {
            (1, [100, 200, 300]), (2, [991, 2, 3434, 5554, 666]),
        };
        
        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();
        df.PrintSchema();
        
    }
    

    private List<(
        int Id,
        string Name,
        (
        DateTime JoinDate,
        bool IsActive,
        (
        string Department,
        int Level,
        (
        decimal Salary,
        string Currency,
        (
        int VacationDays,
        bool HasHealthInsurance,
        string InsuranceProvider
        ) Benefits
        ) Employment
        ) Position
        ) Details
        )> deeplyNestedData = new ()
    {
        (
            1,
            "John Smith",
            (
                new DateTime(2020, 5, 15),
                true,
                (
                    "Engineering",
                    3,
                    (
                        85000.00m,
                        "USD",
                        (
                            25,
                            true,
                            "BlueCross"
                        )
                    )
                )
            )
        )
        , (
            2,
            "Jane Doe",
            (
                new DateTime(2019, 8, 22),
                true,
                (
                    "Marketing",
                    4,
                    (
                        92000.00m,
                        "USD",
                        (
                            30,
                            true,
                            "Aetna"
                        )
                    )
                )
            )
        )
        , (
            3,
            "Mike Johnson",
            (
                new DateTime(2021, 3, 10),
                false,
                (
                    "Sales",
                    2,
                    (
                        65000.00m,
                        "USD",
                        (
                            20,
                            false,
                            null
                        )
                    )
                )
            )
        )
    };
}