using System.Runtime.CompilerServices;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;
using DoubleType = Spark.Connect.Dotnet.Sql.Types.DoubleType;
using FloatType = Spark.Connect.Dotnet.Sql.Types.FloatType;
using StringType = Apache.Arrow.Types.StringType;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class BinarizerTests : E2ETestBase
{
    public BinarizerTests(ITestOutputHelper logger) : base(logger)
    {
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Binarizer_Tests()
    {
        var binarizer = new Binarizer(Spark);
        binarizer.SetInputCol("value100");
        binarizer.SetOutputCol("features");
        binarizer.SetThreshold(1.0f);
        var dfWords = Spark.CreateDataFrame((new List<(double, double)>()
        {
            (1.0, 1), (2.0, 2), (1.0, 3), (2.0, 4), (1.0, 5), (2.0, 6)
        }).Cast<ITuple>(), new Spark.Connect.Dotnet.Sql.Types.StructType((List<StructField>) [new StructField("value100", new DoubleType(), false), new StructField("values2", new DoubleType(), false)]));

       var buckets = binarizer.Transform(dfWords);
       buckets.Show(3, 10000);
       buckets.PrintSchema();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Binarizer_ReadWrite_Test()
    {
        var binarizer = new Binarizer(Spark);
        binarizer.SetInputCol("value1");
        binarizer.SetOutputCol("features");
        binarizer.SetThreshold(1.0f);
        var dfWords = Spark.CreateDataFrame((new List<(double, double)>()
        {
            (1.0, 1), (2.0, 2), (1.0, 3), (2.0, 4), (1.0, 5), (2.0, 6)
        }).Cast<ITuple>(), new Spark.Connect.Dotnet.Sql.Types.StructType((List<StructField>) [new StructField("value1", new DoubleType(), false), new StructField("values2", new DoubleType(), false)]));

        binarizer.Save("/tmp/transformers-binarizer");
        
        var binarizerFromDisk = Binarizer.Load("/tmp/transformers-binarizer", Spark);
        
       var tokens = binarizerFromDisk.Transform(dfWords, binarizerFromDisk.ParamMap.Update(new Dictionary<string, dynamic>(){{"outputCol", "override-output-col"}}));
       tokens.Show(3, 10000);
       tokens.PrintSchema();
    }
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Binarizer_Ctor_Test()
    {
        var binarizer = new Binarizer(Spark, new Dictionary<string, dynamic>()
        {
            {"inputCol", "value1"},
            {"outputCol", "features"},
            {"threshold", 1.0f}
        });
        
        var dfWords = Spark.CreateDataFrame((new List<(double, double)>()
        {
            (1.0, 1), (2.0, 2), (1.0, 3), (2.0, 4), (1.0, 5), (2.0, 6)
        }).Cast<ITuple>(), new Spark.Connect.Dotnet.Sql.Types.StructType((List<StructField>) [new StructField("value1", new DoubleType(), false), new StructField("values2", new DoubleType(), false)]));

        binarizer.Save("/tmp/transformers-binarizer");
        
        var binarizerFromDisk = Binarizer.Load("/tmp/transformers-binarizer", Spark);
        
        var tokens = binarizerFromDisk.Transform(dfWords, binarizerFromDisk.ParamMap.Update(new Dictionary<string, dynamic>(){{"outputCol", "override-output-col"}}));
        tokens.Show(3, 10000);
        tokens.PrintSchema();
    }
}