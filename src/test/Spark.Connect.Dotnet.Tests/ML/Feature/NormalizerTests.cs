using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class NormalizerTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void Normalizer_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([1.0, 0.5, -1.0]), 0),
            (new DenseVector([2.0, 1.0, 1.0]), 0),
            (new DenseVector([4.0, 10.0, 2.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var normalizer = new Normalizer(Spark);
        normalizer.SetInputCol("features");
        normalizer.SetOutputCol("normFeatures");
        normalizer.SetP(2.0); // L2 norm

        var normalized = normalizer.Transform(df);
        normalized.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void Normalizer_Parameters_Test()
    {
        var normalizer = new Normalizer(Spark);

        normalizer.SetInputCol("input");
        normalizer.SetOutputCol("output");
        normalizer.SetP(1.0); // L1 norm

        Assert.Equal("input", normalizer.GetInputCol());
        Assert.Equal("output", normalizer.GetOutputCol());
        Assert.Equal(1.0, normalizer.GetP());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void Normalizer_ReadWrite_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([1.0, 0.5, -1.0]), 0),
            (new DenseVector([2.0, 1.0, 1.0]), 0),
            (new DenseVector([4.0, 10.0, 2.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var normalizer = new Normalizer(Spark);
        normalizer.SetInputCol("features");
        normalizer.SetOutputCol("normFeatures");
        normalizer.SetP(2.0);

        var savePath = $"/tmp/normalizer-{Guid.NewGuid()}";
        normalizer.Save(savePath);

        var loadedNormalizer = Normalizer.Load(savePath, Spark);

        var normalized = loadedNormalizer.Transform(df);
        normalized.Show(truncate: 100);
    }
}
