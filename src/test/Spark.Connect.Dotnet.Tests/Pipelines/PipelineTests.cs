using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Pipelines;
using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.Pipelines;

public class PipelineTests : E2ETestBase
{
    private const string TestStorage = "file:///tmp/spark-pipelines-test";

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
    
    [Fact(Skip = "SimplestDeclarativePipeline has streaming tables which require streaming support")]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_SimplestDeclarativePipeline_Declarative_Pipeline_Test()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");

        var runner = new PipelineRunner();
        var graphs = runner.Run([typeof(SimplestDeclarativePipeline)], Spark);

        Spark.Read.Table("GoldOutputTable").Show();
    }
    
    [Fact(Skip = "Includes AdvancedFeaturesDeclarativePipeline which uses 'once' option (DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED)")]
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
        
        graph.AddMaterializedView("ABC99_first_table", Spark.Range(100).WithColumn("FirstTableName", Functions.Lit("one")));
        graph.AddMaterializedView("ABC99_second_table", Spark.Range(100).WithColumn("SecondTableName", Functions.Lit("two")).Drop("id"));
        graph.AddMaterializedView("ABC99_third_table", Spark.Read.Table("ABC99_first_table").CrossJoin(Spark.Read.Table("ABC99_second_table")));

        graph.StartRun(storage: TestStorage);
        graph.DropRun();

        graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);

        graph.AddMaterializedView("ABC99_first_table", Spark.Range(100).WithColumn("FirstTableName", Functions.Lit("one")));
        graph.AddMaterializedView("ABC99_second_table", Spark.Range(100).WithColumn("SecondTableName", Functions.Lit("two")).Drop("id"));
        graph.AddMaterializedView("ABC99_third_table", Spark.Read.Table("ABC99_first_table").CrossJoin(Spark.Read.Table("ABC99_second_table")));

        graph.StartRun(storage: TestStorage);
        graph.DropRun();
        
        Spark.Read.Table("ABC99_third_table").Show();
    }
    
    [Fact(Skip = "Requires streaming support - server missing StreamingRelation class")]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_Tables_Using_Graph_StreamingTable_And_MaterializedView()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        var graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);

        graph.AddTable("A_Streaming_Table", Spark.ReadStream().Format("rate").Option("rowsPerSecond", "1").Load());
        graph.AddMaterializedView("MatViewFromStreamingTable", Spark.Read.Table("A_Streaming_Table"));
        graph.StartRun(storage: TestStorage);

        Thread.Sleep(10000);

        graph.DropRun();

        Spark.Read.Table("A_Streaming_Table").Show();
        Spark.Read.Table("MatViewFromStreamingTable").Show();
    }

    [Fact(Skip = "Requires streaming support - server missing StreamingRelation class")]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_Table_With_ClusteringColumns()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        var graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);

        // Create a table with clustering columns for optimized query performance
        graph.AddTable(
            tableName: "Clustered_Table_Test",
            source: Spark.ReadStream().Format("rate").Option("rowsPerSecond", "1").Load()
                .WithColumn("event_date", Functions.CurrentDate())
                .WithColumn("event_type", Functions.Lit("test")),
            clusteringColumns: new[] { "event_date", "event_type" },
            comment: "Table with clustering columns");

        graph.StartRun(storage: TestStorage);
        Thread.Sleep(5000);
        graph.DropRun();

        Spark.Read.Table("Clustered_Table_Test").Show();
    }

    [Fact(Skip = "Server returns DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED - 'once' option not yet implemented")]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_Table_With_Once_Flag()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        var graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);

        // Create a one-time table (backfill scenario)
        graph.AddTable(
            tableName: "OneTime_Backfill_Table",
            source: Spark.Range(100).WithColumn("backfill", Functions.Lit(true)),
            once: true,
            comment: "One-time backfill table");

        graph.StartRun(storage: TestStorage);
        graph.DropRun();

        Spark.Read.Table("OneTime_Backfill_Table").Show();
    }

    [Fact]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_With_StartRun_Options()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        var graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);

        graph.AddMaterializedView("StartRun_Options_Table1", Spark.Range(50));
        graph.AddMaterializedView("StartRun_Options_Table2", Spark.Range(100));

        // Use dry run to validate without executing
        graph.StartRun(dry: true, storage: TestStorage);
        graph.DropRun();

        // Create a new graph for the actual run since DropRun destroys the graph
        graph = new PipelineGraph(Spark);
        graph.AddMaterializedView("StartRun_Options_Table1", Spark.Range(50));
        graph.AddMaterializedView("StartRun_Options_Table2", Spark.Range(100));

        // Now actually run with refresh selection
        graph.StartRun(refreshSelection: new[] { "StartRun_Options_Table1" }, storage: TestStorage);
        graph.DropRun();
    }

    [Fact(Skip = "Requires streaming support - server missing StreamingRelation class")]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_Sink_Output()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        var graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);

        // Create a source table first
        graph.AddTable(
            tableName: "Sink_Source_Table",
            source: Spark.ReadStream().Format("rate").Option("rowsPerSecond", "1").Load());

        // Create a sink that writes to a destination
        graph.AddSink(
            sinkName: "Test_Sink",
            source: Spark.Read.Table("Sink_Source_Table"),
            format: "delta",
            comment: "Test sink output");

        graph.StartRun(storage: TestStorage);
        Thread.Sleep(5000);
        graph.DropRun();
    }

    [Fact]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_With_SourceCodeLocation()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        var graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);

        // Create a table with explicit source code location for debugging
        graph.AddMaterializedView(
            viewName: "SourceLocation_Test_Table",
            source: Spark.Range(100),
            sourceCodeFileName: "PipelineTests.cs",
            sourceCodeLineNumber: 200,
            sourceCodeDefinitionPath: "Spark.Connect.Dotnet.Tests.Pipelines.PipelineTests");

        graph.StartRun(storage: TestStorage);
        graph.DropRun();

        Spark.Read.Table("SourceLocation_Test_Table").Show();
    }

    [Fact(Skip = "Server returns DEFINE_FLOW_ONCE_OPTION_NOT_SUPPORTED - 'once' option not yet implemented")]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void Deploy_AdvancedFeaturesDeclarativePipeline()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        Spark.Sql("CREATE DATABASE IF NOT EXISTS advanced_demo").Show();

        var runner = new PipelineRunner();
        var graphs = runner.Run([typeof(AdvancedFeaturesDeclarativePipeline)], Spark);

        // Wait for streaming to produce some data
        Thread.Sleep(5000);

        // Verify tables were created
        Spark.Read.Table("advanced_demo.clustered_events").Show();
        Spark.Read.Table("advanced_demo.historical_backfill").Show();
    }

    [Fact]
    [Trait("SparkMinVersion", "4-pipelines")]
    public void DefineSqlGraphElements_With_SqlText()
    {
        Spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
        var graph = new PipelineGraph(Spark);
        Logger.WriteLine(graph.GraphId);

        // Define pipeline elements using SQL text
        var sqlText = @"
            CREATE MATERIALIZED VIEW sql_defined_view AS
            SELECT 1 as id, 'test' as name;
        ";

        graph.DefineSqlGraphElements(sqlText: sqlText);
        graph.StartRun(storage: TestStorage);
        graph.DropRun();
    }
}