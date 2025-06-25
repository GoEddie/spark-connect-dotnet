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

    public void AddTable(string tableName, DataFrame source, SparkDataType? schema = null, IDictionary<string, string>? options = null, IDictionary<string, string>? sqlConfs = null, bool? once = null, string? format = null, string? comment = null, string[]? partitionCols = null)
    {
        var processor = new PipelineTableProcessor(_spark, this, tableName, source, schema, options, sqlConfs, once, format, comment, partitionCols);
        processor.Create();
        Tables.Add(processor);
    }
    
    
    public void AddMaterializedView(string viewName, DataFrame source, SparkDataType? schema = null, IDictionary<string, string>? options = null, IDictionary<string, string>? sqlConfs = null, bool? once = null, string? format = null, string? comment = null, string[]? partitionCols = null)
    {
        var processor = new PipelineMaterializedViewProcessor(_spark, this, viewName, source, schema, options, sqlConfs, once, format, comment, partitionCols);
        processor.Create();
        MaterializedViews.Add(processor);
    }
    
    public void AddTemporaryView(string viewName, DataFrame source, SparkDataType? schema = null, IDictionary<string, string>? options = null, IDictionary<string, string>? sqlConfs = null, bool? once = null, string? format = null, string? comment = null, string[]? partitionCols = null)
    {
        var processor = new PipelineTemporaryViewProcessor(_spark, this, viewName, source, schema, options, sqlConfs, once, comment, partitionCols);
        processor.Create();
        TemporaryViews.Add(processor);
    }

    public void StartRun()
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    StartRun = new PipelineCommand.Types.StartRun()
                    {
                        DataflowGraphId = GraphId
                    }
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
}