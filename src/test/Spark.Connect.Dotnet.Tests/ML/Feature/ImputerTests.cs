using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class ImputerTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void Imputer_Test()
    {
        var data = new List<(double a, double b, int dummy)>()
        {
            (1.0, double.NaN, 0),
            (2.0, double.NaN, 0),
            (double.NaN, 3.0, 0),
            (4.0, 4.0, 0),
            (5.0, 5.0, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("a", new DoubleType(), true),
            new StructField("b", new DoubleType(), true),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var imputer = new Imputer();
        imputer.SetInputCols(new[] { "a", "b" });
        imputer.SetOutputCols(new[] { "out_a", "out_b" });
        imputer.SetStrategy("mean");

        var model = imputer.Fit(df);
        var imputed = model.Transform(df);
        imputed.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void Imputer_Parameters_Test()
    {
        var imputer = new Imputer();

        imputer.SetInputCols(new[] { "a", "b" });
        imputer.SetOutputCols(new[] { "out_a", "out_b" });
        imputer.SetStrategy("median");
        imputer.SetMissingValue(-1.0);

        Assert.Equal(new[] { "a", "b" }, imputer.GetInputCols());
        Assert.Equal(new[] { "out_a", "out_b" }, imputer.GetOutputCols());
        Assert.Equal("median", imputer.GetStrategy());
        Assert.Equal(-1.0, imputer.GetMissingValue());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void Imputer_ReadWrite_Test()
    {
        var data = new List<(double a, double b, int dummy)>()
        {
            (1.0, double.NaN, 0),
            (2.0, double.NaN, 0),
            (double.NaN, 3.0, 0),
            (4.0, 4.0, 0),
            (5.0, 5.0, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("a", new DoubleType(), true),
            new StructField("b", new DoubleType(), true),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var imputer = new Imputer();
        imputer.SetInputCols(new[] { "a", "b" });
        imputer.SetOutputCols(new[] { "out_a", "out_b" });
        imputer.SetStrategy("mean");

        var model = imputer.Fit(df);

        var savePath = $"/tmp/imputer-model-{Guid.NewGuid()}";
        model.Save(savePath);

        var loadedModel = ImputerModel.Load(savePath, Spark);

        var imputed = loadedModel.Transform(df);
        imputed.Show(truncate: 100);
    }
}
