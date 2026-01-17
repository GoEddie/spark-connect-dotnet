using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Classification;

public class DecisionTreeClassifierTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void DecisionTreeClassifier_Test()
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

        var dt = new DecisionTreeClassifier();
        dt.SetMaxDepth(5);
        dt.SetImpurity("gini");

        var model = dt.Fit(training);
        var prediction = model.Transform(training);
        var result = prediction.Select("features", "label", "prediction", "probability");

        result.PrintSchema();
        result.Show(4, 1000);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void DecisionTreeClassifier_Parameters_Test()
    {
        var dt = new DecisionTreeClassifier();

        dt.SetMaxDepth(10);
        dt.SetMaxBins(64);
        dt.SetMinInstancesPerNode(2);
        dt.SetImpurity("entropy");

        Assert.Equal(10, dt.GetMaxDepth());
        Assert.Equal(64, dt.GetMaxBins());
        Assert.Equal(2, dt.GetMinInstancesPerNode());
        Assert.Equal("entropy", dt.GetImpurity());
    }

    [Fact(Skip = "Spark Connect does not support loading DecisionTreeClassificationModel")]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void DecisionTreeClassifier_ReadWrite_Test()
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

        var dt = new DecisionTreeClassifier();
        dt.SetMaxDepth(5);

        var model = dt.Fit(training);

        var savePath = $"/tmp/decision-tree-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = DecisionTreeClassifierModel.Load(savePath, Spark);

        var prediction = loadedModel.Transform(training);
        var result = prediction.Select("features", "label", "prediction");

        result.PrintSchema();
        result.Show(4, 1000);
    }
}
