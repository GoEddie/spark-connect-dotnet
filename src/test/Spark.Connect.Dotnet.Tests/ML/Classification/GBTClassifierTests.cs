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
        // Logger.WriteLine($"Predict: {gbtModel.Predict(new DenseVector([0.0, 1.1, 0.1]))}");
        
    }
    
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void NaiveBayes_ReadWrite_Test()
    {
        var data = new List<(double f, DenseVector Vector, float)>()
        {
            (1.0, new DenseVector([0.0, 1.1, 0.1]), 0.1F), 
            (0.0, new DenseVector([2.0, 1.0, 1.0]), 0.5F), 
            (0.0, new DenseVector([2.0, 1.3, 1.0]), 1.0F), 
            (1.0, new DenseVector([0.0, 1.2, 0.5]), 1.0F)
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("features", new VectorUDT(), false),
            new StructField("weight", new FloatType(), false),
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        training.Show();

        var nb = new NaiveBayes();
        nb.SetFeaturesCol("features");
        nb.SetThresholds([0.01F, 10.0F]);
        
        var model = nb.Fit(training);   
        
        model.Save("/tmp/nb-model");
        var modelFromDisk = NaiveBayesModel.Load("/tmp/nb-model", Spark);
        
        var dfOutput = modelFromDisk.Transform(training);
        dfOutput.Show(3, 10000);
        dfOutput.PrintSchema();
    }
}