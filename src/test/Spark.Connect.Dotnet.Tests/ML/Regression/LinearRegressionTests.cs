using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.ML.Regression;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Regression;

public class LinearRegressionTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void LinearRegression_Test()
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

        var lr = new LinearRegression();
        lr.SetMaxIter(10);
        lr.SetRegParam(0.3);
        lr.SetElasticNetParam(0.8);

        var model = lr.Fit(training);
        var prediction = model.Transform(training);
        var result = prediction.Select("features", "label", "prediction");

        result.PrintSchema();
        result.Show(4, 1000);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void LinearRegression_Parameters_Test()
    {
        var lr = new LinearRegression();

        lr.SetFeaturesCol("myFeatures");
        lr.SetLabelCol("myLabel");
        lr.SetPredictionCol("myPrediction");
        lr.SetMaxIter(50);
        lr.SetRegParam(0.1);
        lr.SetElasticNetParam(0.5);
        lr.SetFitIntercept(false);
        lr.SetStandardization(false);

        Assert.Equal("myFeatures", lr.GetFeaturesCol());
        Assert.Equal("myLabel", lr.GetLabelCol());
        Assert.Equal("myPrediction", lr.GetPredictionCol());
        Assert.Equal(50, lr.GetMaxIter());
        Assert.Equal(0.1, lr.GetRegParam());
        Assert.Equal(0.5, lr.GetElasticNetParam());
        Assert.False(lr.GetFitIntercept());
        Assert.False(lr.GetStandardization());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void LinearRegression_ReadWrite_Test()
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

        var lr = new LinearRegression();
        lr.SetMaxIter(10);

        var model = lr.Fit(training);
        model.Save("/tmp/linear-regression-model");

        var loadedModel = LinearRegressionModel.Load("/tmp/linear-regression-model", Spark);
        var prediction = loadedModel.Transform(training);

        prediction.Show(4, 1000);
    }
}
