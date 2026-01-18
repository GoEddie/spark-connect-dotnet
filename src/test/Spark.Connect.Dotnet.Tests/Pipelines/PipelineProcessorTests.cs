using Spark.Connect;
using Spark.Connect.Dotnet.Pipelines;
using Spark.Connect.Dotnet.Pipelines.Attributes;
using Xunit;

namespace Spark.Connect.Dotnet.Tests.Pipelines;

/// <summary>
/// Unit tests for pipeline processor proto message construction.
/// These tests verify that the proto messages are constructed correctly without needing a Spark server.
/// </summary>
public class PipelineProcessorTests
{
    #region StartRun Options Tests

    [Fact]
    public void StartRun_Plan_Contains_FullRefreshSelection()
    {
        // Arrange
        var startRun = new PipelineCommand.Types.StartRun()
        {
            DataflowGraphId = "test-graph-id"
        };
        startRun.FullRefreshSelection.AddRange(new[] { "table1", "table2" });

        // Act & Assert
        Assert.Equal("test-graph-id", startRun.DataflowGraphId);
        Assert.Equal(2, startRun.FullRefreshSelection.Count);
        Assert.Contains("table1", startRun.FullRefreshSelection);
        Assert.Contains("table2", startRun.FullRefreshSelection);
    }

    [Fact]
    public void StartRun_Plan_Contains_FullRefreshAll()
    {
        // Arrange & Act
        var startRun = new PipelineCommand.Types.StartRun()
        {
            DataflowGraphId = "test-graph-id",
            FullRefreshAll = true
        };

        // Assert
        Assert.True(startRun.FullRefreshAll);
    }

    [Fact]
    public void StartRun_Plan_Contains_RefreshSelection()
    {
        // Arrange
        var startRun = new PipelineCommand.Types.StartRun()
        {
            DataflowGraphId = "test-graph-id"
        };
        startRun.RefreshSelection.AddRange(new[] { "view1", "view2", "view3" });

        // Act & Assert
        Assert.Equal(3, startRun.RefreshSelection.Count);
        Assert.Contains("view1", startRun.RefreshSelection);
    }

    [Fact]
    public void StartRun_Plan_Contains_DryRun()
    {
        // Arrange & Act
        var startRun = new PipelineCommand.Types.StartRun()
        {
            DataflowGraphId = "test-graph-id",
            Dry = true
        };

        // Assert
        Assert.True(startRun.Dry);
    }

    [Fact]
    public void StartRun_Plan_Contains_Storage()
    {
        // Arrange & Act
        var startRun = new PipelineCommand.Types.StartRun()
        {
            DataflowGraphId = "test-graph-id",
            Storage = "/path/to/storage"
        };

        // Assert
        Assert.Equal("/path/to/storage", startRun.Storage);
    }

    #endregion

    #region SourceCodeLocation Tests

    [Fact]
    public void SourceCodeLocation_Contains_AllFields()
    {
        // Arrange & Act
        var location = new SourceCodeLocation()
        {
            FileName = "MyPipeline.cs",
            LineNumber = 42,
            DefinitionPath = "MyNamespace.MyPipeline"
        };

        // Assert
        Assert.Equal("MyPipeline.cs", location.FileName);
        Assert.Equal(42, location.LineNumber);
        Assert.Equal("MyNamespace.MyPipeline", location.DefinitionPath);
    }

    [Fact]
    public void DefineOutput_Contains_SourceCodeLocation()
    {
        // Arrange & Act
        var defineOutput = new PipelineCommand.Types.DefineOutput()
        {
            OutputName = "test_table",
            OutputType = OutputType.Table,
            DataflowGraphId = "test-graph-id",
            SourceCodeLocation = new SourceCodeLocation()
            {
                FileName = "TestFile.cs",
                LineNumber = 100
            }
        };

        // Assert
        Assert.NotNull(defineOutput.SourceCodeLocation);
        Assert.Equal("TestFile.cs", defineOutput.SourceCodeLocation.FileName);
        Assert.Equal(100, defineOutput.SourceCodeLocation.LineNumber);
    }

    [Fact]
    public void DefineFlow_Contains_SourceCodeLocation()
    {
        // Arrange & Act
        var defineFlow = new PipelineCommand.Types.DefineFlow()
        {
            FlowName = "test_flow",
            TargetDatasetName = "test_table",
            DataflowGraphId = "test-graph-id",
            SourceCodeLocation = new SourceCodeLocation()
            {
                FileName = "FlowFile.cs",
                DefinitionPath = "MyNamespace.MyFlow"
            }
        };

        // Assert
        Assert.NotNull(defineFlow.SourceCodeLocation);
        Assert.Equal("FlowFile.cs", defineFlow.SourceCodeLocation.FileName);
        Assert.Equal("MyNamespace.MyFlow", defineFlow.SourceCodeLocation.DefinitionPath);
    }

    #endregion

    #region DefineFlow.Once Tests

    [Fact]
    public void DefineFlow_Contains_Once_True()
    {
        // Arrange & Act
        var defineFlow = new PipelineCommand.Types.DefineFlow()
        {
            FlowName = "backfill_flow",
            TargetDatasetName = "target_table",
            DataflowGraphId = "test-graph-id",
            Once = true
        };

        // Assert
        Assert.True(defineFlow.Once);
    }

    [Fact]
    public void DefineFlow_Contains_Once_False_ByDefault()
    {
        // Arrange & Act
        var defineFlow = new PipelineCommand.Types.DefineFlow()
        {
            FlowName = "streaming_flow",
            TargetDatasetName = "target_table",
            DataflowGraphId = "test-graph-id"
        };

        // Assert
        Assert.False(defineFlow.Once);
    }

    #endregion

    #region DefineFlow.ClientId Tests

    [Fact]
    public void DefineFlow_Contains_ClientId()
    {
        // Arrange & Act
        var defineFlow = new PipelineCommand.Types.DefineFlow()
        {
            FlowName = "test_flow",
            TargetDatasetName = "target_table",
            DataflowGraphId = "test-graph-id",
            ClientId = "client-123-abc"
        };

        // Assert
        Assert.Equal("client-123-abc", defineFlow.ClientId);
    }

    #endregion

    #region ClusteringColumns Tests

    [Fact]
    public void TableDetails_Contains_ClusteringColumns()
    {
        // Arrange
        var tableDetails = new PipelineCommand.Types.DefineOutput.Types.TableDetails();
        tableDetails.ClusteringColumns.AddRange(new[] { "col1", "col2", "col3" });

        // Act & Assert
        Assert.Equal(3, tableDetails.ClusteringColumns.Count);
        Assert.Contains("col1", tableDetails.ClusteringColumns);
        Assert.Contains("col2", tableDetails.ClusteringColumns);
        Assert.Contains("col3", tableDetails.ClusteringColumns);
    }

    [Fact]
    public void DefineOutput_Table_With_ClusteringColumns()
    {
        // Arrange
        var tableDetails = new PipelineCommand.Types.DefineOutput.Types.TableDetails();
        tableDetails.ClusteringColumns.AddRange(new[] { "date", "region" });
        tableDetails.PartitionCols.AddRange(new[] { "year", "month" });

        var defineOutput = new PipelineCommand.Types.DefineOutput()
        {
            OutputName = "clustered_table",
            OutputType = OutputType.Table,
            DataflowGraphId = "test-graph-id",
            TableDetails = tableDetails
        };

        // Assert
        Assert.Equal(2, defineOutput.TableDetails.ClusteringColumns.Count);
        Assert.Equal(2, defineOutput.TableDetails.PartitionCols.Count);
    }

    #endregion

    #region Sink Output Type Tests

    [Fact]
    public void DefineOutput_Sink_Type()
    {
        // Arrange & Act
        var defineOutput = new PipelineCommand.Types.DefineOutput()
        {
            OutputName = "kafka_sink",
            OutputType = OutputType.Sink,
            DataflowGraphId = "test-graph-id"
        };

        // Assert
        Assert.Equal(OutputType.Sink, defineOutput.OutputType);
    }

    [Fact]
    public void SinkDetails_Contains_Options()
    {
        // Arrange
        var sinkDetails = new PipelineCommand.Types.DefineOutput.Types.SinkDetails()
        {
            Format = "kafka"
        };
        sinkDetails.Options.Add("kafka.bootstrap.servers", "localhost:9092");
        sinkDetails.Options.Add("topic", "my-topic");

        // Act & Assert
        Assert.Equal("kafka", sinkDetails.Format);
        Assert.Equal(2, sinkDetails.Options.Count);
        Assert.Equal("localhost:9092", sinkDetails.Options["kafka.bootstrap.servers"]);
        Assert.Equal("my-topic", sinkDetails.Options["topic"]);
    }

    [Fact]
    public void DefineOutput_Sink_With_SinkDetails()
    {
        // Arrange
        var sinkDetails = new PipelineCommand.Types.DefineOutput.Types.SinkDetails()
        {
            Format = "delta"
        };
        sinkDetails.Options.Add("path", "/output/path");

        var defineOutput = new PipelineCommand.Types.DefineOutput()
        {
            OutputName = "delta_sink",
            OutputType = OutputType.Sink,
            DataflowGraphId = "test-graph-id",
            SinkDetails = sinkDetails
        };

        // Assert
        Assert.Equal(OutputType.Sink, defineOutput.OutputType);
        Assert.NotNull(defineOutput.SinkDetails);
        Assert.Equal("delta", defineOutput.SinkDetails.Format);
        Assert.Equal("/output/path", defineOutput.SinkDetails.Options["path"]);
    }

    #endregion

    #region DefineSqlGraphElements Tests

    [Fact]
    public void DefineSqlGraphElements_With_SqlFilePath()
    {
        // Arrange & Act
        var defineSql = new PipelineCommand.Types.DefineSqlGraphElements()
        {
            DataflowGraphId = "test-graph-id",
            SqlFilePath = "/path/to/pipeline.sql"
        };

        // Assert
        Assert.Equal("test-graph-id", defineSql.DataflowGraphId);
        Assert.Equal("/path/to/pipeline.sql", defineSql.SqlFilePath);
    }

    [Fact]
    public void DefineSqlGraphElements_With_SqlText()
    {
        // Arrange
        var sqlText = @"
            CREATE STREAMING TABLE bronze AS
            SELECT * FROM stream_source;

            CREATE MATERIALIZED VIEW silver AS
            SELECT * FROM bronze WHERE valid = true;
        ";

        // Act
        var defineSql = new PipelineCommand.Types.DefineSqlGraphElements()
        {
            DataflowGraphId = "test-graph-id",
            SqlText = sqlText
        };

        // Assert
        Assert.Equal("test-graph-id", defineSql.DataflowGraphId);
        Assert.Contains("CREATE STREAMING TABLE bronze", defineSql.SqlText);
        Assert.Contains("CREATE MATERIALIZED VIEW silver", defineSql.SqlText);
    }

    #endregion

    #region GetQueryFunctionExecutionSignalStream Tests

    [Fact]
    public void GetQueryFunctionExecutionSignalStream_Contains_ClientId()
    {
        // Arrange & Act
        var getSignalStream = new PipelineCommand.Types.GetQueryFunctionExecutionSignalStream()
        {
            DataflowGraphId = "test-graph-id",
            ClientId = "client-456"
        };

        // Assert
        Assert.Equal("test-graph-id", getSignalStream.DataflowGraphId);
        Assert.Equal("client-456", getSignalStream.ClientId);
    }

    #endregion

    #region DefineFlowQueryFunctionResult Tests

    [Fact]
    public void DefineFlowQueryFunctionResult_Contains_FlowName()
    {
        // Arrange & Act
        var defineResult = new PipelineCommand.Types.DefineFlowQueryFunctionResult()
        {
            FlowName = "my_flow",
            DataflowGraphId = "test-graph-id"
        };

        // Assert
        Assert.Equal("my_flow", defineResult.FlowName);
        Assert.Equal("test-graph-id", defineResult.DataflowGraphId);
    }

    #endregion

    #region ResolvedIdentifier Tests

    [Fact]
    public void ResolvedIdentifier_Contains_AllFields()
    {
        // Arrange & Act
        var resolved = new ResolvedIdentifier()
        {
            CatalogName = "my_catalog",
            TableName = "my_table"
        };
        resolved.Namespace.Add("my_schema");

        // Assert
        Assert.Equal("my_catalog", resolved.CatalogName);
        Assert.Single(resolved.Namespace);
        Assert.Equal("my_schema", resolved.Namespace[0]);
        Assert.Equal("my_table", resolved.TableName);
    }

    [Fact]
    public void DefineOutputResult_Contains_ResolvedIdentifier()
    {
        // Arrange & Act
        var resolved = new ResolvedIdentifier()
        {
            CatalogName = "catalog",
            TableName = "output_table"
        };

        var result = new PipelineCommandResult.Types.DefineOutputResult()
        {
            ResolvedIdentifier = resolved
        };

        // Assert
        Assert.NotNull(result.ResolvedIdentifier);
        Assert.Equal("catalog", result.ResolvedIdentifier.CatalogName);
        Assert.Equal("output_table", result.ResolvedIdentifier.TableName);
    }

    [Fact]
    public void DefineFlowResult_Contains_ResolvedIdentifier()
    {
        // Arrange & Act
        var resolved = new ResolvedIdentifier()
        {
            CatalogName = "catalog",
            TableName = "flow_target"
        };
        resolved.Namespace.Add("schema");

        var result = new PipelineCommandResult.Types.DefineFlowResult()
        {
            ResolvedIdentifier = resolved
        };

        // Assert
        Assert.NotNull(result.ResolvedIdentifier);
        Assert.Equal("catalog", result.ResolvedIdentifier.CatalogName);
        Assert.Equal("schema", result.ResolvedIdentifier.Namespace[0]);
        Assert.Equal("flow_target", result.ResolvedIdentifier.TableName);
    }

    #endregion

    #region Attribute Tests

    [Fact]
    public void StreamingTableAttribute_Has_ClusteringColumns_Property()
    {
        // Arrange & Act
        var attr = new StreamingTableAttribute()
        {
            Name = "test_table",
            ClusteringColumns = new[] { "col1", "col2" },
            Once = true
        };

        // Assert
        Assert.Equal("test_table", attr.Name);
        Assert.NotNull(attr.ClusteringColumns);
        Assert.Equal(2, attr.ClusteringColumns.Length);
        Assert.True(attr.Once);
    }

    [Fact]
    public void MaterializedViewAttribute_Has_ClusteringColumns_Property()
    {
        // Arrange & Act
        var attr = new MaterializedViewAttribute()
        {
            Name = "test_view",
            ClusteringColumns = new[] { "date", "region" },
            Once = true,
            Format = "delta"
        };

        // Assert
        Assert.Equal("test_view", attr.Name);
        Assert.NotNull(attr.ClusteringColumns);
        Assert.Equal(2, attr.ClusteringColumns.Length);
        Assert.True(attr.Once);
        Assert.Equal("delta", attr.Format);
    }

    [Fact]
    public void TemporaryViewAttribute_Has_Once_Property()
    {
        // Arrange & Act
        var attr = new TemporaryViewAttribute()
        {
            Name = "temp_view",
            Comment = "A temporary view",
            Once = true
        };

        // Assert
        Assert.Equal("temp_view", attr.Name);
        Assert.Equal("A temporary view", attr.Comment);
        Assert.True(attr.Once);
    }

    [Fact]
    public void SinkAttribute_Has_AllProperties()
    {
        // Arrange & Act
        var attr = new SinkAttribute()
        {
            Name = "kafka_sink",
            Comment = "Writes to Kafka",
            Format = "kafka",
            Once = false
        };

        // Assert
        Assert.Equal("kafka_sink", attr.Name);
        Assert.Equal("Writes to Kafka", attr.Comment);
        Assert.Equal("kafka", attr.Format);
        Assert.False(attr.Once);
    }

    #endregion

    #region OutputType Enum Tests

    [Fact]
    public void OutputType_Has_AllExpectedValues()
    {
        // Assert
        Assert.Equal(0, (int)OutputType.Unspecified);
        Assert.Equal(1, (int)OutputType.MaterializedView);
        Assert.Equal(2, (int)OutputType.Table);
        Assert.Equal(3, (int)OutputType.TemporaryView);
        Assert.Equal(4, (int)OutputType.Sink);
    }

    #endregion
}
