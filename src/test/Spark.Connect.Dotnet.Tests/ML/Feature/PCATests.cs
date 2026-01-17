using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class PCATests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void PCA_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([0.0, 1.0, 0.0, 7.0, 0.0]), 0),
            (new DenseVector([2.0, 0.0, 3.0, 4.0, 5.0]), 0),
            (new DenseVector([4.0, 0.0, 0.0, 6.0, 7.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var pca = new PCA();
        pca.SetInputCol("features");
        pca.SetOutputCol("pcaFeatures");
        pca.SetK(3);

        var model = pca.Fit(df);
        var transformed = model.Transform(df);
        transformed.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void PCA_Parameters_Test()
    {
        var pca = new PCA();

        pca.SetInputCol("input");
        pca.SetOutputCol("output");
        pca.SetK(5);

        Assert.Equal("input", pca.GetInputCol());
        Assert.Equal("output", pca.GetOutputCol());
        Assert.Equal(5, pca.GetK());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void PCA_ReadWrite_Test()
    {
        var data = new List<(DenseVector features, int dummy)>()
        {
            (new DenseVector([0.0, 1.0, 0.0, 7.0, 0.0]), 0),
            (new DenseVector([2.0, 0.0, 3.0, 4.0, 5.0]), 0),
            (new DenseVector([4.0, 0.0, 0.0, 6.0, 7.0]), 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("features", new VectorUDT(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var pca = new PCA();
        pca.SetInputCol("features");
        pca.SetOutputCol("pcaFeatures");
        pca.SetK(3);

        var model = pca.Fit(df);

        var savePath = $"/tmp/pca-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = PCAModel.Load(savePath, Spark);

        var transformed = loadedModel.Transform(df);
        transformed.Show(truncate: 100);
    }
}
