using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class IndexToStringTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void IndexToString_Test()
    {
        var data = new List<(double index, int dummy)>()
        {
            (0.0, 0),
            (1.0, 0),
            (2.0, 0),
            (0.0, 0),
            (1.0, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("index", new DoubleType(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        df.Show();

        var indexToString = new IndexToString(Spark);
        indexToString.SetInputCol("index");
        indexToString.SetOutputCol("originalCategory");
        indexToString.SetLabels(new[] { "a", "b", "c" });

        var result = indexToString.Transform(df);
        result.Show(truncate: 100);
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void IndexToString_Parameters_Test()
    {
        var indexToString = new IndexToString(Spark);

        indexToString.SetInputCol("input");
        indexToString.SetOutputCol("output");
        indexToString.SetLabels(new[] { "cat", "dog", "bird" });

        Assert.Equal("input", indexToString.GetInputCol());
        Assert.Equal("output", indexToString.GetOutputCol());
        Assert.Equal(new[] { "cat", "dog", "bird" }, indexToString.GetLabels());
    }

    [Fact]
    [Trait("Category", "ML")]
    [Trait("SparkMinVersion", "4")]
    public void IndexToString_ReadWrite_Test()
    {
        var data = new List<(double index, int dummy)>()
        {
            (0.0, 0),
            (1.0, 0),
            (2.0, 0),
            (0.0, 0)
        };

        var schema = new StructType(new[]
        {
            new StructField("index", new DoubleType(), false),
            new StructField("dummy", new IntegerType(), false)
        });

        var df = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);

        var indexToString = new IndexToString(Spark);
        indexToString.SetInputCol("index");
        indexToString.SetOutputCol("originalCategory");
        indexToString.SetLabels(new[] { "a", "b", "c" });

        var savePath = $"/tmp/index-to-string-{Guid.NewGuid()}";
        indexToString.Save(savePath);

        var loadedIndexToString = IndexToString.Load(savePath, Spark);

        var result = loadedIndexToString.Transform(df);
        result.Show(truncate: 100);
    }
}
