using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class IDFTests : E2ETestBase
{
    public IDFTests(ITestOutputHelper logger) : base(logger)
    {
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void IDF_Test()
    {
        var data = new List<(double f, DenseVector Vector)>()
        {
            (1, new DenseVector([1.0, 2.0])), 
            (2, new DenseVector([0.0, 1.0])), 
            (3, new DenseVector([3.0, 0.2]))
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("tf", new VectorUDT(), false)
        });

        var dfSource = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var idf = new IDF(new Dictionary<string, dynamic>(){{"inputCol", "tf"}, {"minDocFreq", 10}});
        
        idf.ParamMap.Add("outputCol", "tf-idf");

        dfSource = dfSource.WithColumnRenamed("tf", "tfi");

        var idfModel = idf.Fit(dfSource, idf.ParamMap.Update(new Dictionary<string, dynamic>(){{"inputCol", "tfi"}}));
        
        Logger.WriteLine($"IDF Model Params: {idfModel.GetInputCol()}, {idfModel.GetOutputCol()}, {idfModel.GetMinDocFreq()}");

        var dfIDFOutput = idfModel.Transform(dfSource);
        dfIDFOutput.PrintSchema();
        dfIDFOutput.Show(3, 10000);
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void IDF_Write_Save_Test()
    {
        var data = new List<(double f, DenseVector Vector)>()
        {
            (1, new DenseVector([1.0, 2.0])), 
            (2, new DenseVector([0.0, 1.0])), 
            (3, new DenseVector([3.0, 0.2]))
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("tf99", new VectorUDT(), false)
        });

        var dfSource = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var idf = new IDF();
        idf.SetMinDocFreq(1000);
        idf.SetInputCol("tf99");
        
        idf.ParamMap.Add("outputCol", "tf-idf");

        dfSource = dfSource.WithColumnRenamed("tf", "tf99");

        var idfModel = idf.Fit(dfSource, idf.ParamMap.Update(new Dictionary<string, dynamic>(){{"inputCol", "tf99"}}));
        idfModel.Save("/tmp/transformers-idf-model");
        var loadedIdf = IDFModel.Load("/tmp/transformers-idf-model", Spark);
        var df = loadedIdf.Transform(dfSource);
        df.PrintSchema();
        df.Show();
        
    }
}