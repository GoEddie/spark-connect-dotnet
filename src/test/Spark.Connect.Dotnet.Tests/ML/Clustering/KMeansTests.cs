using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Clustering;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Clustering;

public class KMeansTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void KMeans_Test()
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

        var kmeans = new KMeans();
        kmeans.SetK(2);
        kmeans.SetMaxIter(20);
        kmeans.SetSeed(1L);

        var model = kmeans.Fit(df);
        var predictions = model.Transform(df);
        predictions.Show();
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void KMeans_Parameters_Test()
    {
        var kmeans = new KMeans();

        kmeans.SetK(5);
        kmeans.SetMaxIter(50);
        kmeans.SetTol(0.001);
        kmeans.SetDistanceMeasure("cosine");
        kmeans.SetInitMode("random");
        kmeans.SetSeed(42L);

        Assert.Equal(5, kmeans.GetK());
        Assert.Equal(50, kmeans.GetMaxIter());
        Assert.Equal(0.001, kmeans.GetTol());
        Assert.Equal("cosine", kmeans.GetDistanceMeasure());
        Assert.Equal("random", kmeans.GetInitMode());
        Assert.Equal(42L, kmeans.GetSeed());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void KMeans_ReadWrite_Test()
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

        var kmeans = new KMeans();
        kmeans.SetK(2);
        var model = kmeans.Fit(df);
        model.Save("/tmp/kmeans-model");

        var loadedModel = KMeansModel.Load("/tmp/kmeans-model", Spark);
        var predictions = loadedModel.Transform(df);
        predictions.Show();
    }
}
