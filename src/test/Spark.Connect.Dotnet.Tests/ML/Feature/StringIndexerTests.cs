using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;

public class StringIndexer_Tests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void StringIndexer_Test()
    {
        var data = new List<(double, string)>()
        {
            (1.0, "hello there"),
            (0.0, "oooh friend")
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("str", new StringType(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        training.Show();

        var indexer = new StringIndexer();
        indexer.SetInputCol("str");
        indexer.SetOutputCol("indexed");
        Logger.WriteLine($"indexer uuid: {indexer.Uid}");
        this.Logger.WriteLine($"indexer uuid: {indexer.Uid}, for param map: {indexer.ParamMap.GetHashCode()}");
        var model = indexer.Fit(training);
        var dfOutput = model.Transform(training);
        dfOutput.Show(3, 10000);
        dfOutput.PrintSchema();

    }
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void StringIndexer_MultipleColumns_Test()
    {
        var data = new List<(double, string, string)>()
        {
            (1.0, "hello there", "ooh friend, hello there"),
            (0.0, "oooh friend", "ooh friend, hello there")
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("str", new StringType(), false),
            new StructField("str2", new StringType(), false)
        });

        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        training.Show();

        var indexer = new StringIndexer();
        Logger.WriteLine($"indexer uuid: {indexer.Uid}");
        indexer.SetInputCols(["str", "str2"]);
        indexer.SetOutputCols(["indexed", "also_indexed"]);
        
        this.Logger.WriteLine($"indexer uuid: {indexer.Uid}, for param map: {indexer.ParamMap.GetHashCode()}");
        var model = indexer.Fit(training);
        var dfOutput = model.Transform(training);
        dfOutput.Show(3, 10000);
        dfOutput.PrintSchema();
    }
    
    
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void StringIndexer_ReadWrite_Test()
    {
        var data = new List<(double, string)>()
        {
            (1.0, "hello there"),
            (0.0, "oooh friend")
        };

        var schema = new  StructType(new[]
        {
            new StructField("label", new DoubleType(), false),
            new StructField("str", new StringType(), false)
        });
        
        var training = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        training.Show();

        var indexer = new StringIndexer();
        indexer.SetInputCol("str");
        indexer.SetOutputCol("indexed");
        Logger.WriteLine($"indexer uuid: {indexer.Uid}");
        this.Logger.WriteLine($"indexer uuid: {indexer.Uid}, for param map: {indexer.ParamMap.GetHashCode()}");
        var model = indexer.Fit(training);
        model.Save("/tmp/string-indexer-model");
        var modelFromDisk = StringIndexerModel.Load("/tmp/string-indexer-model", Spark);
        
        var dfOutput = modelFromDisk.Transform(training);
        dfOutput.Show(3, 10000);
        dfOutput.PrintSchema();
    }
}