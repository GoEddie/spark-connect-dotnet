using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class OneHotEncoderTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void OneHotEncoder_Test()
    {
        var data = new List<(double categoryIndex, int dummy)>()
        {
            (0.0, 0),
            (1.0, 0),
            (2.0, 0),
            (0.0, 0),
            (0.0, 0),
            (2.0, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("categoryIndex", new DoubleType(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var encoder = new OneHotEncoder();
        encoder.SetInputCol("categoryIndex");
        encoder.SetOutputCol("categoryVec");
        encoder.SetDropLast(false);

        var model = encoder.Fit(df);
        var encoded = model.Transform(df);
        encoded.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void OneHotEncoder_Parameters_Test()
    {
        var encoder = new OneHotEncoder();

        encoder.SetInputCol("input");
        encoder.SetOutputCol("output");
        encoder.SetDropLast(true);
        encoder.SetHandleInvalid("keep");

        Assert.Equal("input", encoder.GetInputCol());
        Assert.Equal("output", encoder.GetOutputCol());
        Assert.True(encoder.GetDropLast());
        Assert.Equal("keep", encoder.GetHandleInvalid());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void OneHotEncoder_ReadWrite_Test()
    {
        var data = new List<(double categoryIndex, int dummy)>()
        {
            (0.0, 0),
            (1.0, 0),
            (2.0, 0),
            (0.0, 0),
            (0.0, 0),
            (2.0, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("categoryIndex", new DoubleType(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var encoder = new OneHotEncoder();
        encoder.SetInputCol("categoryIndex");
        encoder.SetOutputCol("categoryVec");

        var model = encoder.Fit(df);

        var savePath = $"/tmp/onehot-encoder-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = OneHotEncoderModel.Load(savePath, Spark);

        var encoded = loadedModel.Transform(df);
        encoded.Show(truncate: 100);
    }
}
