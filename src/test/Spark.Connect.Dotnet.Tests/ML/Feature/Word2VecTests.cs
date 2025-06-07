using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class Word2VecTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Word2Vec_Test()
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

        var documentDF = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var word2Vec = new Word2Vec();
        word2Vec.SetInputCol("words");
        word2Vec.SetOutputCol("result");
        word2Vec.SetVectorSize(2);
        word2Vec.SetStepSize(0.1);
        word2Vec.SetMinCount(2);
        Assert.Equal(2, word2Vec.GetMinCount());

        var model = word2Vec.Fit(documentDF);
        var result = model.Transform(documentDF);
        result.Show(3, 1000);
        result.PrintSchema();
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Word2Vec_ReadWrite_Test()
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

        var documentDF = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var word2Vec = new Word2Vec();
        word2Vec.SetInputCol("words");
        word2Vec.SetOutputCol("result");
        word2Vec.SetVectorSize(2);

        var model = word2Vec.Fit(documentDF);
        model.Save("/tmp/transformers-word2vec");

        var loaded = Word2VecModel.Load("/tmp/transformers-word2vec", Spark);
        var result = loaded.Transform(documentDF);
        result.Show(3, 1000);
        result.PrintSchema();
    }
}
