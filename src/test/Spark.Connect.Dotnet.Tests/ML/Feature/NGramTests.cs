using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class NGramTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void NGram_Test()
    {
        var data = new List<(string[] words, int dummy)>()
        {
            (new[] { "Hi", "I", "heard", "about", "Spark" }, 0),
            (new[] { "I", "wish", "Java", "could", "use", "case", "classes" }, 0),
            (new[] { "Logistic", "regression", "models", "are", "neat" }, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("words", new ArrayType(new StringType(), true), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show(truncate: 100);

        var ngram = new NGram(Spark);
        ngram.SetInputCol("words");
        ngram.SetOutputCol("ngrams");
        ngram.SetN(2);

        var result = ngram.Transform(df);
        result.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void NGram_Parameters_Test()
    {
        var ngram = new NGram(Spark);

        ngram.SetInputCol("input");
        ngram.SetOutputCol("output");
        ngram.SetN(3);

        Assert.Equal("input", ngram.GetInputCol());
        Assert.Equal("output", ngram.GetOutputCol());
        Assert.Equal(3, ngram.GetN());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void NGram_ReadWrite_Test()
    {
        var data = new List<(string[] words, int dummy)>()
        {
            (new[] { "Hi", "I", "heard", "about", "Spark" }, 0),
            (new[] { "I", "wish", "Java", "could", "use", "case", "classes" }, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("words", new ArrayType(new StringType(), true), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var ngram = new NGram(Spark);
        ngram.SetInputCol("words");
        ngram.SetOutputCol("ngrams");
        ngram.SetN(2);

        var savePath = $"/tmp/ngram-{Guid.NewGuid()}";
        ngram.Save(savePath);

        var loadedNgram = NGram.Load(savePath, Spark);

        var result = loadedNgram.Transform(df);
        result.Show(truncate: 100);
    }
}
