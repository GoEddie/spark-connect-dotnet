using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.Sql.Types;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.ML.Feature;


public class VectorAssemblerTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Fact]
    [Trait("SparkMinVersion", "4")]
    public void VectorAssembler_Test()
    {
        var assembler = new VectorAssembler(Spark);
        assembler.SetInputCols(new []{"col1", "col2"});
        assembler.SetOutputCol("output");
        
        var data = new List<(double, double, double)>()
        {
            (1.0, 2.0, 3.0), 
            (11.0, 2.0, 3.0), 
            (111.0, 2.0, 3.0), 
            (1111.0, 2.0, -3.0), 
            (11111.0, 2.0, -3.0), 
            (111111.0, 2.0, -3.0), 
        };

        var schema = new  StructType(new[]
        {
            new StructField("col1", new DoubleType(), false),
            new StructField("col99", new DoubleType(), false),
            new StructField("col2", new DoubleType(), false),
            
        });
        
        var sourceDF = Spark.CreateDataFrame(data.Cast<ITuple>(), schema);
        
        // var resultDF = assembler.Transform(sourceDF);
        // resultDF.Show(3, 10000);
        // resultDF.PrintSchema();
    }   
    
}