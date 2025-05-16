using System.Runtime.CompilerServices;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;
using StringType = Apache.Arrow.Types.StringType;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class TokenizerTests : E2ETestBase
{
    public TokenizerTests(ITestOutputHelper logger) : base(logger)
    {
    }

    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void TokenizerTest()
    {
        var tokenizer = new Tokenizer(Spark);

        tokenizer.SetInputCol("text");
        tokenizer.SetOutputCol("words-go-here");

        var dfWords = Spark.CreateDataFrame((new List<(int id, string text)>()
        {
            (1, "This is a test"), (2, "This is another test")
        }).Cast<ITuple>(), new Spark.Connect.Dotnet.Sql.Types.StructType((List<StructField>) [new StructField("id", new Int32Type(), false), new StructField("text", new StringType(), false)]));

        var tokens = tokenizer.Transform(dfWords, tokenizer.ParamMap.Update(new Dictionary<string, dynamic>(){{"outputCol", "override-output-col"}}));
        tokens.Show(3, 10000);
        tokens.PrintSchema();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void Tokenizer_ReadWrite_Test()
    {
        var tokenizer = new Tokenizer(Spark);

        tokenizer.SetInputCol("text");
        tokenizer.SetOutputCol("words-go-here");

        var dfWords = Spark.CreateDataFrame((new List<(int id, string text)>()
        {
            (1, "This is a test"), (2, "This is another test")
        }).Cast<ITuple>(), new Spark.Connect.Dotnet.Sql.Types.StructType((List<StructField>) [new StructField("id", new Int32Type(), false), new StructField("text", new StringType(), false)]));

       tokenizer.Save("/tmp/transformers-tokenizer");
       var tokenizerFromDisk = Tokenizer.Load("/tmp/transformers-tokenizer", Spark);
        
       var tokens = tokenizerFromDisk.Transform(dfWords, tokenizer.ParamMap.Update(new Dictionary<string, dynamic>(){{"outputCol", "override-output-col"}}));
       tokens.Show(3, 10000);
       tokens.PrintSchema();
    }
}