using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Pipelines;

public class PipelineGraph
{
    private readonly SparkSession _spark;
    public string GraphId { get; set; }
    
    public Plan GraphPlan { get; set; }
    public List<PipelineTableProcessor> Tables { get; set; } = [];
    public List<PipelineMaterializedViewProcessor> MaterializedViews { get; set; } = [];
    public List<PipelineTemporaryViewProcessor> TemporaryViews { get; set; } = [];
    public List<PipelineSinkProcessor> Sinks { get; set; } = [];
    
    public PipelineGraph(SparkSession spark, string? defaultCatalog = null, string? defaultDatabase = null, IDictionary<string, string>? options = null)
    {
        _spark = spark;
        
        GraphPlan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    CreateDataflowGraph = new PipelineCommand.Types.CreateDataflowGraph()
                    {
                        
                    }
                }
            }
        };

        if (!string.IsNullOrEmpty(defaultCatalog))
        {
            GraphPlan.Command.PipelineCommand.CreateDataflowGraph.DefaultCatalog = defaultCatalog;
        }
        
        if (!string.IsNullOrEmpty(defaultDatabase))
        {
            GraphPlan.Command.PipelineCommand.CreateDataflowGraph.DefaultDatabase = defaultDatabase;
        }
        
        if(options is not null)
        {
            GraphPlan.Command.PipelineCommand.CreateDataflowGraph.SqlConf.Add(options);
        }
        
        var requestExecutor = new RequestExecutor(spark, GraphPlan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();

        if (requestExecutor.GetPipelineEventResult() != null)
        {
            Logger.WriteLine(requestExecutor.GetPipelineEventResult().Event.Message);
        }
        
        if (requestExecutor.GetPipelineCommandResult() != null)
        {
            GraphId = requestExecutor.GetPipelineCommandResult().CreateDataflowGraphResult.DataflowGraphId;
            return;
        }
        
        throw new InvalidOperationException("Unable to create graph");
    }

    public void AddTable(
        string tableName,
        DataFrame source,
        SparkDataType? schema = null,
        IDictionary<string, string>? options = null,
        IDictionary<string, string>? sqlConfs = null,
        string? format = null,
        string? comment = null,
        string[]? partitionCols = null,
        string[]? clusteringColumns = null,
        bool? once = null,
        string? clientId = null,
        string? sourceCodeFileName = null,
        int? sourceCodeLineNumber = null,
        string? sourceCodeDefinitionPath = null)
    {
        var processor = new PipelineTableProcessor(_spark, this, tableName, source, schema, options, sqlConfs, format, comment, partitionCols, clusteringColumns, once, clientId, sourceCodeFileName, sourceCodeLineNumber, sourceCodeDefinitionPath);
        processor.Create();
        Tables.Add(processor);
    }


    public void AddMaterializedView(
        string viewName,
        DataFrame source,
        SparkDataType? schema = null,
        IDictionary<string, string>? options = null,
        IDictionary<string, string>? sqlConfs = null,
        string? format = null,
        string? comment = null,
        string[]? partitionCols = null,
        string[]? clusteringColumns = null,
        bool? once = null,
        string? clientId = null,
        string? sourceCodeFileName = null,
        int? sourceCodeLineNumber = null,
        string? sourceCodeDefinitionPath = null)
    {
        var processor = new PipelineMaterializedViewProcessor(_spark, this, viewName, source, schema, options, sqlConfs, format, comment, partitionCols, clusteringColumns, once, clientId, sourceCodeFileName, sourceCodeLineNumber, sourceCodeDefinitionPath);
        processor.Create();
        MaterializedViews.Add(processor);
    }

    public void AddTemporaryView(
        string viewName,
        DataFrame source,
        SparkDataType? schema = null,
        IDictionary<string, string>? options = null,
        IDictionary<string, string>? sqlConfs = null,
        string? comment = null,
        string[]? partitionCols = null,
        bool? once = null,
        string? clientId = null,
        string? sourceCodeFileName = null,
        int? sourceCodeLineNumber = null,
        string? sourceCodeDefinitionPath = null)
    {
        var processor = new PipelineTemporaryViewProcessor(_spark, this, viewName, source, schema, options, sqlConfs, comment, partitionCols, once, clientId, sourceCodeFileName, sourceCodeLineNumber, sourceCodeDefinitionPath);
        processor.Create();
        TemporaryViews.Add(processor);
    }

    public void AddSink(
        string sinkName,
        DataFrame source,
        IDictionary<string, string>? options = null,
        IDictionary<string, string>? sqlConfs = null,
        string? format = null,
        string? comment = null,
        bool? once = null,
        string? clientId = null,
        string? sourceCodeFileName = null,
        int? sourceCodeLineNumber = null,
        string? sourceCodeDefinitionPath = null)
    {
        var processor = new PipelineSinkProcessor(_spark, this, sinkName, source, options, sqlConfs, format, comment, once, clientId, sourceCodeFileName, sourceCodeLineNumber, sourceCodeDefinitionPath);
        processor.Create();
        Sinks.Add(processor);
    }

    public void StartRun(
        IEnumerable<string>? fullRefreshSelection = null,
        bool? fullRefreshAll = null,
        IEnumerable<string>? refreshSelection = null,
        bool? dry = null,
        string? storage = null)
    {
        var startRun = new PipelineCommand.Types.StartRun()
        {
            DataflowGraphId = GraphId
        };

        if (fullRefreshSelection != null)
        {
            startRun.FullRefreshSelection.AddRange(fullRefreshSelection);
        }

        if (fullRefreshAll.HasValue)
        {
            startRun.FullRefreshAll = fullRefreshAll.Value;
        }

        if (refreshSelection != null)
        {
            startRun.RefreshSelection.AddRange(refreshSelection);
        }

        if (dry.HasValue)
        {
            startRun.Dry = dry.Value;
        }

        if (!string.IsNullOrEmpty(storage))
        {
            startRun.Storage = storage;
        }
        else
        {
            startRun.Storage = $"file://{Path.GetTempPath()}";
        }

        var plan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    StartRun = startRun
                }
            }
        };

        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
    }

    public void DropRun()
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    DropDataflowGraph = new PipelineCommand.Types.DropDataflowGraph()
                    {
                        DataflowGraphId = GraphId,
                    }
                }
            }
        };

        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
    }

    /// <summary>
    /// Parses a SQL file and registers all datasets and flows defined in it.
    /// </summary>
    /// <param name="sqlFilePath">The full path to the SQL file. Can be relative or absolute.</param>
    /// <param name="sqlText">The contents of the SQL file. If not provided, the file will be read from sqlFilePath.</param>
    public void DefineSqlGraphElements(string? sqlFilePath = null, string? sqlText = null)
    {
        var defineSql = new PipelineCommand.Types.DefineSqlGraphElements()
        {
            DataflowGraphId = GraphId
        };

        if (!string.IsNullOrEmpty(sqlFilePath))
        {
            defineSql.SqlFilePath = sqlFilePath;
        }

        if (!string.IsNullOrEmpty(sqlText))
        {
            defineSql.SqlText = sqlText;
        }

        var plan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    DefineSqlGraphElements = defineSql
                }
            }
        };

        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
    }

    /// <summary>
    /// Gets the plan for query function execution signal stream for this graph.
    /// Used for deferred flow evaluation where the server requests the client to evaluate query functions.
    /// Note: This returns the Plan for advanced use cases. The actual streaming execution
    /// requires integration with the gRPC streaming response handling.
    /// </summary>
    /// <param name="clientId">Identifier for the client requesting the stream.</param>
    /// <returns>The Plan that can be used to initiate the signal stream.</returns>
    public Plan GetQueryFunctionExecutionSignalStreamPlan(string clientId)
    {
        var getSignalStream = new PipelineCommand.Types.GetQueryFunctionExecutionSignalStream()
        {
            DataflowGraphId = GraphId,
            ClientId = clientId
        };

        return new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    GetQueryFunctionExecutionSignalStream = getSignalStream
                }
            }
        };
    }

    /// <summary>
    /// Updates the flow function evaluation result for a previously un-analyzed flow.
    /// </summary>
    /// <param name="flowName">The fully qualified name of the flow being updated.</param>
    /// <param name="relation">The relation that defines the dataset's flow.</param>
    public void DefineFlowQueryFunctionResult(string flowName, Relation relation)
    {
        var defineResult = new PipelineCommand.Types.DefineFlowQueryFunctionResult()
        {
            FlowName = flowName,
            DataflowGraphId = GraphId,
            Relation = relation
        };

        var plan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    DefineFlowQueryFunctionResult = defineResult
                }
            }
        };

        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
    }
}