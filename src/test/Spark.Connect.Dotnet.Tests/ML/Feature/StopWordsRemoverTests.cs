using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class StopWordsRemoverTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void StopWordsRemover_Test()
    {
        var data = new List<(string[] raw, int dummy)>()
        {
            (new[] { "I", "saw", "the", "red", "balloon" }, 0),
            (new[] { "Mary", "had", "a", "little", "lamb" }, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("raw", new ArrayType(new StringType(), true), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show(truncate: 100);

        var remover = new StopWordsRemover(Spark);
        remover.SetInputCol("raw");
        remover.SetOutputCol("filtered");

        var result = remover.Transform(df);
        result.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void StopWordsRemover_Parameters_Test()
    {
        var remover = new StopWordsRemover(Spark);

        remover.SetInputCol("input");
        remover.SetOutputCol("output");
        remover.SetCaseSensitive(true);
        remover.SetStopWords(new[] { "a", "the", "an" });

        Assert.Equal("input", remover.GetInputCol());
        Assert.Equal("output", remover.GetOutputCol());
        Assert.True(remover.GetCaseSensitive());
        Assert.Equal(new[] { "a", "the", "an" }, remover.GetStopWords());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void StopWordsRemover_ReadWrite_Test()
    {
        var data = new List<(string[] raw, int dummy)>()
        {
            (new[] { "I", "saw", "the", "red", "balloon" }, 0),
            (new[] { "Mary", "had", "a", "little", "lamb" }, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("raw", new ArrayType(new StringType(), true), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var remover = new StopWordsRemover(Spark);
        remover.SetInputCol("raw");
        remover.SetOutputCol("filtered");

        var savePath = $"/tmp/stopwords-remover-{Guid.NewGuid()}";
        remover.Save(savePath);

        var loadedRemover = StopWordsRemover.Load(savePath, Spark);

        var result = loadedRemover.Transform(df);
        result.Show(truncate: 100);
    }
}
