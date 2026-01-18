using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class MaxAbsScalerTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void MaxAbsScaler_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([1.0, 0.1, -8.0]), 0),
            (new DenseVector([2.0, 1.0, -4.0]), 0),
            (new DenseVector([4.0, 10.0, 8.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var scaler = new MaxAbsScaler();
        scaler.SetInputCol("features");
        scaler.SetOutputCol("scaledFeatures");

        var model = scaler.Fit(df);
        var scaled = model.Transform(df);
        scaled.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void MaxAbsScaler_Parameters_Test()
    {
        var scaler = new MaxAbsScaler();

        scaler.SetInputCol("input");
        scaler.SetOutputCol("output");

        Assert.Equal("input", scaler.GetInputCol());
        Assert.Equal("output", scaler.GetOutputCol());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void MaxAbsScaler_ReadWrite_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([1.0, 0.1, -8.0]), 0),
            (new DenseVector([2.0, 1.0, -4.0]), 0),
            (new DenseVector([4.0, 10.0, 8.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var scaler = new MaxAbsScaler();
        scaler.SetInputCol("features");
        scaler.SetOutputCol("scaledFeatures");

        var model = scaler.Fit(df);

        var savePath = $"/tmp/maxabs-scaler-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = MaxAbsScalerModel.Load(savePath, Spark);

        var scaled = loadedModel.Transform(df);
        scaled.Show(truncate: 100);
    }
}
