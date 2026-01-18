using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Classification;

public class LogisticRegressionTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void LogisticRegression_Test()
    {
        
        var data = new List<(double f, DenseVector Vector)>()
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])), 
            (0.0, new DenseVector([2.0, 1.0, -1.0])), 
            (0.0, new DenseVector([2.0, 1.3, 1.0])), 
            (1.0, new DenseVector([0.0, 1.2, -0.5]))
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        training.Show();

        var lr = new LogisticRegression();
        var paramMap = lr.ParamMap;
        paramMap.Add("maxIter", 10);
        paramMap.Add("regParam", 0.01);
        paramMap.Add("aggregationDepth", 299);
        paramMap.Add("rawPredictionCol", "my output col");

        var transformer = lr.Fit(training, lr.ParamMap.Update(paramMap));

        var prediction = transformer.Transform(training);
        var result = prediction.Select("features", "label", "my output col", "prediction");

        result.PrintSchema();
        result.Show(4, 1000);
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void LogisticRegression_ReadWrite_Test()
    {
        var data = new List<(double f, DenseVector Vector)>()
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])), 
            (0.0, new DenseVector([2.0, 1.0, -1.0])), 
            (0.0, new DenseVector([2.0, 1.3, 1.0])), 
            (1.0, new DenseVector([0.0, 1.2, -0.5]))
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        training.Show();

        var lr = new LogisticRegression();
        var paramMap = lr.ParamMap;
        paramMap.Add("maxIter", 10);
        paramMap.Add("regParam", 0.01);
        paramMap.Add("aggregationDepth", 299);
        paramMap.Add("rawPredictionCol", "my output col");

        var transformer = lr.Fit(training, paramMap);
        transformer.Save("/tmp/transformer-lr");
        
        var transformerFromDisk = LogisticRegressionModel.Load("/tmp/transformer-lr", Spark);
        
        var prediction = transformerFromDisk.Transform(training);
        var result = prediction.Select("features", "label", "my output col", "prediction");
        
        result.PrintSchema();
        result.Show(4, 1000);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void LogisticRegression_ModelProperties_Test()
    {
        var data = new List<(double f, DenseVector Vector)>()
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

        var lr = new LogisticRegression();
        lr.SetMaxIter(10);
        lr.SetRegParam(0.01);

        var model = lr.Fit(training);

        // Test model properties
        var coefficients = model.Coefficients;
        var intercept = model.Intercept;
        var numClasses = model.NumClasses;
        var numFeatures = model.NumFeatures;

        Logger.WriteLine($"Coefficients: [{string.Join(", ", coefficients)}]");
        Logger.WriteLine($"Intercept: {intercept}");
        Logger.WriteLine($"NumClasses: {numClasses}");
        Logger.WriteLine($"NumFeatures: {numFeatures}");

        Assert.NotNull(coefficients);
        Assert.Equal(3, coefficients.Count); // 3 features
        Assert.Equal(2, numClasses); // binary classification
        Assert.Equal(3, numFeatures);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void LogisticRegression_Parameters_Test()
    {
        var lr = new LogisticRegression();

        lr.SetFeaturesCol("myFeatures");
        lr.SetLabelCol("myLabel");
        lr.SetPredictionCol("myPrediction");
        lr.SetMaxIter(50);
        lr.SetRegParam(0.1);
        lr.SetElasticNetParam(0.5);
        lr.SetFitIntercept(false);
        lr.SetThresholds([0.3, 0.7]);
        lr.SetWeightCol("myWeight");

        Assert.Equal("myFeatures", lr.GetFeaturesCol());
        Assert.Equal("myLabel", lr.GetLabelCol());
        Assert.Equal("myPrediction", lr.GetPredictionCol());
        Assert.Equal(50, lr.GetMaxIter());
        Assert.Equal(0.1, lr.GetRegParam());
        Assert.Equal(0.5, lr.GetElasticNetParam());
        Assert.False(lr.GetFitIntercept());
        Assert.NotNull(lr.GetThresholds());
        Assert.Equal(2, lr.GetThresholds()!.Length);
        Assert.Equal(0.3, lr.GetThresholds()![0]);
        Assert.Equal(0.7, lr.GetThresholds()![1]);
        Assert.Equal("myWeight", lr.GetWeightCol());
    }
}