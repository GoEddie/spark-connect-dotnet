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
            (0, new[] { "ab", "bc", "cd" }),
            (1, new[] { "de", "ef", "fg" })
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
        word2Vec.SetMinCount(1);
        Assert.Equal(1, word2Vec.GetMinCount());

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
            (0, new[] { "ab", "bc", "cd" }),
            (1, new[] { "de", "ef", "fg" })
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
        word2Vec.SetMinCount(1);
        

        var model = word2Vec.Fit(documentDF);
        model.Save("/tmp/transformers-word2vec");

        var loaded = Word2VecModel.Load("/tmp/transformers-word2vec", Spark);
        var result = loaded.Transform(documentDF);
        result.Show(3, 1000);
        result.PrintSchema();
    }
    
    [Trait("SparkMinVersion", "4")]
    public void Word2VecModel_Params_Test()
    {
        var data = new List<(int id, string[] words)>()
        {
            (0, new[] { "ab", "bc", "cd" }),
            (1, new[] { "de", "ef", "fg" })
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
        word2Vec.SetMinCount(1);

        var model = word2Vec.Fit(documentDF);

        model.SetInputCol("words");
        Assert.Equal("words", model.GetInputCol());
        model.SetOutputCol("features");
        Assert.Equal("features", model.GetOutputCol());
        model.SetVectorSize(3);
        Assert.Equal(3, model.GetVectorSize());
        model.SetMinCount(2);
        Assert.Equal(2, model.GetMinCount());
        model.SetNumPartitions(2);
        Assert.Equal(2, model.GetNumPartitions());
        model.SetStepSize(0.2);
        Assert.Equal(0.2, model.GetStepSize());
        model.SetMaxIter(2);
        Assert.Equal(2, model.GetMaxIter());
        model.SetSeed(42L);
        Assert.Equal(42L, model.GetSeed());
        model.SetWindowSize(10);
        Assert.Equal(10, model.GetWindowSize());
        model.SetMaxSentenceLength(500);
        Assert.Equal(500, model.GetMaxSentenceLength());

        var result = model.Transform(documentDF);
        result.Show(3, 1000);
        result.PrintSchema();
    }
}
