using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class HashingTFTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void HashingTF_Transform_Test()
    {
        var data = new List<(int id, string[] words)>()
        {
            (0, new[] { "a", "b", "c" }),
            (1, new[] { "d", "e", "f" })
        };

        var schema = new StructType(new[]
        {
            new StructField("id", new IntegerType(), false),
            new StructField("words", new ArrayType(new StringType(), true), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var hashingTf = new HashingTF(Spark);
        hashingTf.SetInputCol("words");
        hashingTf.SetOutputCol("features");
        hashingTf.SetNumFeatures(10);
        hashingTf.SetBinary(false);

        var result = hashingTf.Transform(df);
        result.Show(3, 1000);
        result.PrintSchema();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void HashingTF_ReadWrite_Test()
    {
        var data = new List<(int id, string[] words)>()
        {
            (0, new[] { "a", "b" }),
            (1, new[] { "c", "d" })
        };

        var schema = new StructType(new[]
        {
            new StructField("id", new IntegerType(), false),
            new StructField("words", new ArrayType(new StringType(), true), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var hashingTf = new HashingTF(Spark);
        hashingTf.SetInputCol("words");
        hashingTf.SetOutputCol("features");

        hashingTf.Save("/tmp/transformers-hashingtf");
        var loaded = HashingTF.Load("/tmp/transformers-hashingtf", Spark);
        var result = loaded.Transform(df);
        result.Show(3, 1000);
        result.PrintSchema();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void HashingTF_Params_Test()
    {
        var hashingTf = new HashingTF(Spark);
        hashingTf.SetInputCol("input");
        Assert.Equal("input", hashingTf.GetInputCol());
        hashingTf.SetOutputCol("out");
        Assert.Equal("out", hashingTf.GetOutputCol());
        hashingTf.SetNumFeatures(123);
        Assert.Equal(123, hashingTf.GetNumFeatures());
        hashingTf.SetBinary(true);
        Assert.True(hashingTf.GetBinary());
    }
}
