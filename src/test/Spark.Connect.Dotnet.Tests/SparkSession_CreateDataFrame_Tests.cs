using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests;

public class SparkSession_CreateDataFrame_Tests : E2ETestBase
{
    public SparkSession_CreateDataFrame_Tests(ITestOutputHelper logger) : base(logger)
    {
    }
    
    [Fact]
    public void CreateDataFrame_Test()
    {
        var rawData = new List<(int Id, int? abc, DenseVector Vector)>()
        {
            (1, 3, new DenseVector([0.0, 1.1, 0.1])),
            (2, null, new DenseVector([2.0, 1.0, -1.0])),
            (3, 99, new DenseVector([2.0, 1.3, 1.0])),
            (4, null, new DenseVector([0.0, 1.2, -0.5]))
        };

        var df = Spark.CreateDataFrame(rawData.Cast<ITuple>());
        df.Show();
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
                new StructField("id99999999HSHSHSHSHSHSH", SparkDataType.IntType(), false),
                new StructField("abc", SparkDataType.IntType(), true),
                new StructField("vector", new DenseVectorUDT(), false)
            }
        );
        
        var df = Spark.CreateDataFrame(rawData.Cast<ITuple>(), schema);
        df.Show();
        df.PrintSchema();
    }
}