using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class StandardScalerTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void StandardScaler_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([0.0, 10.4, 1.0]), 0),
            (new DenseVector([2.0, 11.0, -1.0]), 0),
            (new DenseVector([4.0, 12.0, -2.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var scaler = new StandardScaler();
        scaler.SetInputCol("features");
        scaler.SetOutputCol("scaledFeatures");
        scaler.SetWithMean(true);
        scaler.SetWithStd(true);

        var model = scaler.Fit(df);
        var scaled = model.Transform(df);
        scaled.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void StandardScaler_Parameters_Test()
    {
        var scaler = new StandardScaler();

        scaler.SetInputCol("input");
        scaler.SetOutputCol("output");
        scaler.SetWithMean(true);
        scaler.SetWithStd(false);

        Assert.Equal("input", scaler.GetInputCol());
        Assert.Equal("output", scaler.GetOutputCol());
        Assert.True(scaler.GetWithMean());
        Assert.False(scaler.GetWithStd());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void StandardScaler_ReadWrite_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([0.0, 10.4, 1.0]), 0),
            (new DenseVector([2.0, 11.0, -1.0]), 0),
            (new DenseVector([4.0, 12.0, -2.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var scaler = new StandardScaler();
        scaler.SetInputCol("features");
        scaler.SetOutputCol("scaledFeatures");

        var model = scaler.Fit(df);

        var savePath = $"/tmp/standard-scaler-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = StandardScalerModel.Load(savePath, Spark);

        var scaled = loadedModel.Transform(df);
        scaled.Show(truncate: 100);
    }
}
