using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Classification;

public class RandomForestClassifierTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void RandomForestClassifier_Test()
    {
        var data = new List<(double label, DenseVector features)>()
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])),
            (0.0, new DenseVector([2.0, 1.0, -1.0])),
            (0.0, new DenseVector([2.0, 1.3, 1.0])),
            (1.0, new DenseVector([0.0, 1.2, -0.5]))
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        training.Show();

        var rf = new RandomForestClassifier();
        rf.SetNumTrees(10);
        rf.SetMaxDepth(5);

        var model = rf.Fit(training);
        var prediction = model.Transform(training);
        var result = prediction.Select("features", "label", "prediction", "probability");

        result.PrintSchema();
        result.Show(4, 1000);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void RandomForestClassifier_Parameters_Test()
    {
        var rf = new RandomForestClassifier();

        rf.SetNumTrees(50);
        rf.SetMaxDepth(10);
        rf.SetMaxBins(64);
        rf.SetSubsamplingRate(0.8);
        rf.SetFeatureSubsetStrategy("sqrt");
        rf.SetImpurity("entropy");

        Assert.Equal(50, rf.GetNumTrees());
        Assert.Equal(10, rf.GetMaxDepth());
        Assert.Equal(64, rf.GetMaxBins());
        Assert.Equal(0.8, rf.GetSubsamplingRate());
        Assert.Equal("sqrt", rf.GetFeatureSubsetStrategy());
        Assert.Equal("entropy", rf.GetImpurity());
    }

    [Fact(Skip = "Spark Connect does not support loading RandomForestClassificationModel")]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void RandomForestClassifier_ReadWrite_Test()
    {
        var data = new List<(double label, DenseVector features)>()
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])),
            (0.0, new DenseVector([2.0, 1.0, -1.0])),
            (0.0, new DenseVector([2.0, 1.3, 1.0])),
            (1.0, new DenseVector([0.0, 1.2, -0.5]))
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var rf = new RandomForestClassifier();
        rf.SetNumTrees(10);
        rf.SetMaxDepth(5);

        var model = rf.Fit(training);

        var savePath = $"/tmp/random-forest-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = RandomForestClassifierModel.Load(savePath, Spark);

        var prediction = loadedModel.Transform(training);
        var result = prediction.Select("features", "label", "prediction");

        result.PrintSchema();
        result.Show(4, 1000);
    }
}
