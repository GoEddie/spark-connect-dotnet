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
}