using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class CountVectorizerTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void CountVectorizer_Test()
    {
        var data = new List<(string[] text, int dummy)>()
        {
            (new[] { "a", "b", "c" }, 0),
            (new[] { "a", "b", "b", "c", "a" }, 0),
            (new[] { "a", "a", "a", "a" }, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("text", new ArrayType(new StringType(), true), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var cv = new CountVectorizer();
        cv.SetInputCol("text");
        cv.SetOutputCol("vectors");
        cv.SetVocabSize(10);

        var model = cv.Fit(df);
        var vectorized = model.Transform(df);
        vectorized.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void CountVectorizer_Parameters_Test()
    {
        var cv = new CountVectorizer();

        cv.SetInputCol("input");
        cv.SetOutputCol("output");
        cv.SetVocabSize(1000);
        cv.SetMinDF(2.0);
        cv.SetMaxDF(0.9);
        cv.SetBinary(true);

        Assert.Equal("input", cv.GetInputCol());
        Assert.Equal("output", cv.GetOutputCol());
        Assert.Equal(1000, cv.GetVocabSize());
        Assert.Equal(2.0, cv.GetMinDF());
        Assert.Equal(0.9, cv.GetMaxDF());
        Assert.True(cv.GetBinary());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void CountVectorizer_ReadWrite_Test()
    {
        var data = new List<(string[] text, int dummy)>()
        {
            (new[] { "a", "b", "c" }, 0),
            (new[] { "a", "b", "b", "c", "a" }, 0),
            (new[] { "a", "a", "a", "a" }, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("text", new ArrayType(new StringType(), true), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var cv = new CountVectorizer();
        cv.SetInputCol("text");
        cv.SetOutputCol("vectors");

        var model = cv.Fit(df);

        var savePath = $"/tmp/count-vectorizer-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = CountVectorizerModel.Load(savePath, Spark);

        var vectorized = loadedModel.Transform(df);
        vectorized.Show(truncate: 100);
    }
}
