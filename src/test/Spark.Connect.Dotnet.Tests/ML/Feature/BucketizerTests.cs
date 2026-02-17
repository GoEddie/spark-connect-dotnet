using System.Runtime.CompilerServices;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;
using FloatType = Spark.Connect.Dotnet.Sql.Types.FloatType;
using StringType = Apache.Arrow.Types.StringType;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class BucketizerTests : E2ETestBase
{
    public BucketizerTests(ITestOutputHelper logger) : base(logger)
    {
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Bucketizer_Tests()
    {
        var bucketizer = new Bucketizer(Spark);
        bucketizer.SetInputCol("val");
        bucketizer.SetOutputCol("buckets");
        bucketizer.SetSplits([float.MinValue, 1, 3, 7, float.MaxValue]);
        var dfWords = Spark.CreateDataFrame((new List<(int id, float val)>()
        {
            (1, 1), (2, 2), (1, 3), (2, 4), (1, 5), (2, 6), (1, 7), (2, 8), 
        }).Cast<ITuple>(), new Spark.Connect.Dotnet.Sql.Types.StructType((List<StructField>) [new StructField("id", new Int32Type(), false), new StructField("val", new FloatType(), false)]));

       var buckets = bucketizer.Transform(dfWords);
       buckets.Show(3, 10000);
       buckets.PrintSchema();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Bucketizer_ReadWrite_Test()
    {
        var bucketizer = new Bucketizer(Spark);
        bucketizer.SetInputCol("val");
        bucketizer.SetOutputCol("buckets");
        bucketizer.SetSplits([float.MinValue, 1, 3, 7, float.MaxValue]);
        var dfWords = Spark.CreateDataFrame((new List<(int id, float val)>()
        {
            (1, 1), (2, 2), (1, 3), (2, 4), (1, 5), (2, 6), (1, 7), (2, 8), 
        }).Cast<ITuple>(), new Spark.Connect.Dotnet.Sql.Types.StructType((List<StructField>) [new StructField("id", new Int32Type(), false), new StructField("val", new FloatType(), false)]));

        bucketizer.Save("/tmp/transformers-tokenizer");
      
       var tokenizerFromDisk = Bucketizer.Load("/tmp/transformers-tokenizer", Spark);
        
       var tokens = tokenizerFromDisk.Transform(dfWords, bucketizer.ParamMap.Update(new Dictionary<string, dynamic>(){{"outputCol", "override-output-col"}}));
       tokens.Show(3, 10000);
       tokens.PrintSchema();
    }
}