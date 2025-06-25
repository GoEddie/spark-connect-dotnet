using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Pipelines;
using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.Pipelines;

public class PipelineTests : E2ETestBase
{
    public PipelineTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
    }

    [Fact]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_ExampleDeclarativePipeline_Declarative_Pipeline_Test()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        Spark.Sql("CREATE DATABASE IF NOT EXISTS dltfun").Show();
        Spark.Sql("CREATE DATABASE IF NOT EXISTS a_schema").Show();
        
        var runner = new PipelineRunner();
        var graph = runner.Run([typeof(ExampleDeclarativePipeline)], Spark);
        
        Spark.Read.Table("dltfun.ADifferentStructSchema").Show();
        Spark.Read.Table("dltfun.ADifferentHiveFromat").Show();
        Spark.Read.Table("dltfun.ThirdTable").Show();
        Spark.Read.Table("dltfun.MatViewOne").Show();
        Spark.Read.Table("dltfun.TempViewOne").Show();
        Spark.Sql("SHOW TBLPROPERTIES dltfun.ADifferentStructSchema").Show(10000, 10000);
        
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_SimplestDeclarativePipeline_Declarative_Pipeline_Test()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        
        var runner = new PipelineRunner();
        var graphs = runner.Run([typeof(SimplestDeclarativePipeline)], Spark);
        
        Spark.Read.Table("GoldOutputTable").Show();
    }
    
    [Fact]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_AllDeclarativePipeline_Declarative_Pipeline_Test()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        Spark.Sql("CREATE DATABASE IF NOT EXISTS dltfun").Show();
        
        var runner = new PipelineRunner();
        var graph = runner.Run(typeof(SimplestDeclarativePipeline).Assembly, Spark);
        Spark.Read.Table("GoldOutputTable").Show();

    }
    
    [Fact]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_Tables_Using_Graph()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        var graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);
        
        graph.AddTable("ABC99_first_table", Spark.Range(100).WithColumn("FirstTableName", Functions.Lit("one")));
        graph.AddTable("ABC99_second_table", Spark.Range(100).WithColumn("SecondTableName", Functions.Lit("two")).Drop("id"));
        graph.AddTable("ABC99_third_table", Spark.Read.Table("ABC99_first_table").CrossJoin(Spark.Read.Table("ABC99_second_table")));
        
        graph.StartRun();
        graph.DropRun();
        
        graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);
        
        graph.AddTable("ABC99_first_table", Spark.Range(100).WithColumn("FirstTableName", Functions.Lit("one")));
        graph.AddTable("ABC99_second_table", Spark.Range(100).WithColumn("SecondTableName", Functions.Lit("two")).Drop("id"));
        graph.AddTable("ABC99_third_table", Spark.Read.Table("ABC99_first_table").CrossJoin(Spark.Read.Table("ABC99_second_table")));
        
        graph.StartRun();
        graph.DropRun();
        
        Spark.Read.Table("ABC99_third_table").Show();
    }
}