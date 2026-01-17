using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Clustering;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Clustering;

public class GaussianMixtureTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void GaussianMixture_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([0.0, 0.0]), 0),
            (new DenseVector([1.0, 1.0]), 0),
            (new DenseVector([9.0, 8.0]), 0),
            (new DenseVector([8.0, 9.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var gmm = new GaussianMixture();
        gmm.SetK(2);
        gmm.SetMaxIter(10);
        gmm.SetSeed(1L);

        var model = gmm.Fit(df);
        var predictions = model.Transform(df);
        predictions.Show();
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void GaussianMixture_Parameters_Test()
    {
        var gmm = new GaussianMixture();

        gmm.SetK(3);
        gmm.SetMaxIter(100);
        gmm.SetTol(0.01);
        gmm.SetSeed(42L);

        Assert.Equal(3, gmm.GetK());
        Assert.Equal(100, gmm.GetMaxIter());
        Assert.Equal(0.01, gmm.GetTol());
        Assert.Equal(42L, gmm.GetSeed());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void GaussianMixture_ReadWrite_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([0.0, 0.0]), 0),
            (new DenseVector([1.0, 1.0]), 0),
            (new DenseVector([9.0, 8.0]), 0),
            (new DenseVector([8.0, 9.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var gmm = new GaussianMixture();
        gmm.SetK(2);
        gmm.SetMaxIter(10);
        gmm.SetSeed(1L);

        var model = gmm.Fit(df);

        var savePath = $"/tmp/gaussian-mixture-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = GaussianMixtureModel.Load(savePath, Spark);

        var predictions = loadedModel.Transform(df);
        predictions.Show();
    }
}
