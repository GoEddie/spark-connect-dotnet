using System.Runtime.CompilerServices;
using System.Text;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Classification;

public class GBTClassifierTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void GBTClassifier_Test()
    {
        var data = new List<(double, IUserDefinedType)>()
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])),
            (0.0, Vectors.Sparse(3, [], []))
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        training.Show();

        var stringIndexer = new StringIndexer(new Dictionary<string, dynamic>()
        {
            {"inputCol", "label"}
        });
        
        stringIndexer.SetOutputCol("indexed");
        
        var siModel = stringIndexer.Fit(training);
        var dataToTransform = siModel.Transform(training);
        dataToTransform.PrintSchema();
        dataToTransform.Show(3, 1000);
        
        
        var gbt = new GBTClassifier(new Dictionary<string, dynamic>()
        {
            {"maxIter", 5}, {"maxDepth", 2}, {"labelCol", "indexed"}, {"leafCol", "leafId"}
        });
        
        gbt.SetMinWeightFractionPerNode(0.049f);
        
        var gbtModel = gbt.Fit(dataToTransform);

        var result = gbtModel.Transform(dataToTransform);
        
        result.Show(3, 1000);
        result.PrintSchema();
        
        Logger.WriteLine($"treeWeights: {string.Join(",", gbtModel.TreeWeights())}");
        Logger.WriteLine($"trees: {string.Join(",", gbtModel.Trees())}");
        Logger.WriteLine($"Predict: {gbtModel.Predict(new DenseVector([0.0, 1.1, 0.1]))}");

    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void GBTClassifier_Predict_SingleVector_Test()
    {
        var data = new List<(double, IUserDefinedType)>()
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])),
            (0.0, Vectors.Sparse(3, [], []))
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var stringIndexer = new StringIndexer(new Dictionary<string, dynamic>()
        {
            {"inputCol", "label"}
        });
        stringIndexer.SetOutputCol("indexed");

        var siModel = stringIndexer.Fit(training);
        var dataToTransform = siModel.Transform(training);

        var gbt = new GBTClassifier(new Dictionary<string, dynamic>()
        {
            {"maxIter", 5}, {"maxDepth", 2}, {"labelCol", "indexed"}
        });

        var gbtModel = gbt.Fit(dataToTransform);

        // Test single vector prediction
        var testVector = new DenseVector([0.0, 1.1, 0.1]);
        var prediction = gbtModel.Predict(testVector);

        Logger.WriteLine($"Predict result: {prediction}");

        // Prediction should be either 0.0 or 1.0 for binary classification
        Assert.True(prediction == 0.0 || prediction == 1.0);
    }
    
    
    [Fact(Skip = "Spark Connect does not support loading GBTClassifierModel")]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void GBTClassifier_ReadWrite_Test()
    {
        var data = new List<(double, IUserDefinedType)>()
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1])),
            (0.0, Vectors.Sparse(3, [], []))
        };

        var schema = new StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var stringIndexer = new StringIndexer(new Dictionary<string, dynamic>()
        {
            {"inputCol", "label"}
        });
        stringIndexer.SetOutputCol("indexed");

        var siModel = stringIndexer.Fit(training);
        var dataToTransform = siModel.Transform(training);

        var gbt = new GBTClassifier(new Dictionary<string, dynamic>()
        {
            {"maxIter", 5}, {"maxDepth", 2}, {"labelCol", "indexed"}
        });

        var model = gbt.Fit(dataToTransform);

        var savePath = $"/tmp/gbt-classifier-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = GBTClassifierModel.Load(savePath, Spark);

        var prediction = loadedModel.Transform(dataToTransform);
        prediction.Show(3, 1000);
        prediction.PrintSchema();
    }
}