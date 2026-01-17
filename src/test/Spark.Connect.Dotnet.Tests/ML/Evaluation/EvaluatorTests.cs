using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Evaluation;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.ML.Regression;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Evaluation;

public class EvaluatorTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void BinaryClassificationEvaluator_Test()
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

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var lr = new LogisticRegression();
        lr.SetMaxIter(10);
        var model = lr.Fit(df);
        var predictions = model.Transform(df);

        var evaluator = new BinaryClassificationEvaluator(Spark);
        evaluator.SetMetricName("areaUnderROC");
        var auc = evaluator.Evaluate(predictions);

        Logger.WriteLine($"AUC: {auc}");
        Assert.True(auc >= 0.0 && auc <= 1.0);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void MulticlassClassificationEvaluator_Test()
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

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var lr = new LogisticRegression();
        lr.SetMaxIter(10);
        var model = lr.Fit(df);
        var predictions = model.Transform(df);

        var evaluator = new MulticlassClassificationEvaluator(Spark);
        evaluator.SetMetricName("accuracy");
        var accuracy = evaluator.Evaluate(predictions);

        Logger.WriteLine($"Accuracy: {accuracy}");
        Assert.True(accuracy >= 0.0 && accuracy <= 1.0);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void RegressionEvaluator_Test()
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

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var lr = new LinearRegression();
        lr.SetMaxIter(10);
        var model = lr.Fit(df);
        var predictions = model.Transform(df);

        var evaluator = new RegressionEvaluator(Spark);
        evaluator.SetMetricName("rmse");
        var rmse = evaluator.Evaluate(predictions);

        Logger.WriteLine($"RMSE: {rmse}");
        Assert.True(rmse >= 0.0);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void BinaryClassificationEvaluator_Parameters_Test()
    {
        var evaluator = new BinaryClassificationEvaluator(Spark);

        evaluator.SetLabelCol("myLabel");
        evaluator.SetRawPredictionCol("myRawPrediction");
        evaluator.SetMetricName("areaUnderPR");

        Assert.Equal("myLabel", evaluator.GetLabelCol());
        Assert.Equal("myRawPrediction", evaluator.GetRawPredictionCol());
        Assert.Equal("areaUnderPR", evaluator.GetMetricName());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void MulticlassClassificationEvaluator_Parameters_Test()
    {
        var evaluator = new MulticlassClassificationEvaluator(Spark);

        evaluator.SetLabelCol("myLabel");
        evaluator.SetPredictionCol("myPrediction");
        evaluator.SetMetricName("f1");

        Assert.Equal("myLabel", evaluator.GetLabelCol());
        Assert.Equal("myPrediction", evaluator.GetPredictionCol());
        Assert.Equal("f1", evaluator.GetMetricName());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void RegressionEvaluator_Parameters_Test()
    {
        var evaluator = new RegressionEvaluator(Spark);

        evaluator.SetLabelCol("myLabel");
        evaluator.SetPredictionCol("myPrediction");
        evaluator.SetMetricName("r2");

        Assert.Equal("myLabel", evaluator.GetLabelCol());
        Assert.Equal("myPrediction", evaluator.GetPredictionCol());
        Assert.Equal("r2", evaluator.GetMetricName());
    }
}
