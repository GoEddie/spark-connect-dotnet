using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class RegexTokenizerTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void RegexTokenizer_Test()
    {
        var data = new List<(string sentence, int dummy)>()
        {
            ("Hi I heard about Spark", 0),
            ("I wish Java could use case classes", 0),
            ("Logistic,regression,models,are,neat", 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("sentence", new StringType(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show(truncate: 100);

        var tokenizer = new RegexTokenizer(Spark);
        tokenizer.SetInputCol("sentence");
        tokenizer.SetOutputCol("words");
        tokenizer.SetPattern("\\W"); // Split on non-word characters

        var result = tokenizer.Transform(df);
        result.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void RegexTokenizer_Parameters_Test()
    {
        var tokenizer = new RegexTokenizer(Spark);

        tokenizer.SetInputCol("input");
        tokenizer.SetOutputCol("output");
        tokenizer.SetPattern(",");
        tokenizer.SetGaps(true);
        tokenizer.SetMinTokenLength(2);
        tokenizer.SetToLowercase(false);

        Assert.Equal("input", tokenizer.GetInputCol());
        Assert.Equal("output", tokenizer.GetOutputCol());
        Assert.Equal(",", tokenizer.GetPattern());
        Assert.True(tokenizer.GetGaps());
        Assert.Equal(2, tokenizer.GetMinTokenLength());
        Assert.False(tokenizer.GetToLowercase());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void RegexTokenizer_ReadWrite_Test()
    {
        var data = new List<(string sentence, int dummy)>()
        {
            ("Hi I heard about Spark", 0),
            ("I wish Java could use case classes", 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("sentence", new StringType(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var tokenizer = new RegexTokenizer(Spark);
        tokenizer.SetInputCol("sentence");
        tokenizer.SetOutputCol("words");
        tokenizer.SetPattern("\\W");

        var savePath = $"/tmp/regex-tokenizer-{Guid.NewGuid()}";
        tokenizer.Save(savePath);

        var loadedTokenizer = RegexTokenizer.Load(savePath, Spark);

        var result = loadedTokenizer.Transform(df);
        result.Show(truncate: 100);
    }
}
