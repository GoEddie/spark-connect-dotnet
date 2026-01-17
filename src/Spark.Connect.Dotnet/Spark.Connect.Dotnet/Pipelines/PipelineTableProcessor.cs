using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Pipelines;

public class PipelineTableProcessor
{
    private readonly SparkSession _spark;

    public PipelineTableProcessor(
        SparkSession spark,
        PipelineGraph graph,
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
        _spark = spark;

        var tableDetails = new PipelineCommand.Types.DefineOutput.Types.TableDetails();

        if (schema != null)
        {
            tableDetails.SchemaDataType = schema.ToDataType();
        }

        if (options != null)
        {
            tableDetails.TableProperties.Add(options);
        }

        if (!string.IsNullOrEmpty(format))
        {
            tableDetails.Format = format;
        }

        if (partitionCols != null)
        {
            tableDetails.PartitionCols.AddRange(partitionCols);
        }

        if (clusteringColumns != null)
        {
            tableDetails.ClusteringColumns.AddRange(clusteringColumns);
        }

        var defineOutput = new PipelineCommand.Types.DefineOutput()
        {
            OutputName = tableName,
            OutputType = OutputType.Table,
            DataflowGraphId = graph.GraphId,
            TableDetails = tableDetails
        };

        if (!string.IsNullOrEmpty(comment))
        {
            defineOutput.Comment = comment;
        }

        if (sourceCodeFileName != null || sourceCodeLineNumber != null || sourceCodeDefinitionPath != null)
        {
            var sourceCodeLocation = new SourceCodeLocation();
            if (!string.IsNullOrEmpty(sourceCodeFileName))
            {
                sourceCodeLocation.FileName = sourceCodeFileName;
            }
            if (sourceCodeLineNumber.HasValue)
            {
                sourceCodeLocation.LineNumber = sourceCodeLineNumber.Value;
            }
            if (!string.IsNullOrEmpty(sourceCodeDefinitionPath))
            {
                sourceCodeLocation.DefinitionPath = sourceCodeDefinitionPath;
            }
            defineOutput.SourceCodeLocation = sourceCodeLocation;
        }

        OutputPlan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    DefineOutput = defineOutput
                }
            }
        };

        var defineFlow = new PipelineCommand.Types.DefineFlow()
        {
            TargetDatasetName = tableName,
            DataflowGraphId = graph.GraphId,
            RelationFlowDetails = new PipelineCommand.Types.DefineFlow.Types.WriteRelationFlowDetails()
            {
                Relation = source.Relation
            },
            FlowName = $"flow_{tableName.Replace(".", "__")}"
        };

        if (once.HasValue)
        {
            defineFlow.Once = once.Value;
        }

        if (!string.IsNullOrEmpty(clientId))
        {
            defineFlow.ClientId = clientId;
        }

        if (sourceCodeFileName != null || sourceCodeLineNumber != null || sourceCodeDefinitionPath != null)
        {
            var sourceCodeLocation = new SourceCodeLocation();
            if (!string.IsNullOrEmpty(sourceCodeFileName))
            {
                sourceCodeLocation.FileName = sourceCodeFileName;
            }
            if (sourceCodeLineNumber.HasValue)
            {
                sourceCodeLocation.LineNumber = sourceCodeLineNumber.Value;
            }
            if (!string.IsNullOrEmpty(sourceCodeDefinitionPath))
            {
                sourceCodeLocation.DefinitionPath = sourceCodeDefinitionPath;
            }
            defineFlow.SourceCodeLocation = sourceCodeLocation;
        }

        if (sqlConfs != null)
        {
            defineFlow.SqlConf.Add(sqlConfs);
        }

        FlowPlan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    DefineFlow = defineFlow
                }
            }
        };
    }
    
    public readonly Plan OutputPlan;
    public readonly Plan FlowPlan;

    public ResolvedIdentifier? OutputResolvedIdentifier { get; private set; }
    public ResolvedIdentifier? FlowResolvedIdentifier { get; private set; }

    public void Create()
    {
        var requestExecutor = new RequestExecutor(_spark, OutputPlan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();

        var outputResult = requestExecutor.GetPipelineCommandResult();
        if (outputResult?.DefineOutputResult?.ResolvedIdentifier != null)
        {
            OutputResolvedIdentifier = outputResult.DefineOutputResult.ResolvedIdentifier;
        }

        requestExecutor = new RequestExecutor(_spark, FlowPlan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();

        var flowResult = requestExecutor.GetPipelineCommandResult();
        if (flowResult?.DefineFlowResult?.ResolvedIdentifier != null)
        {
            FlowResolvedIdentifier = flowResult.DefineFlowResult.ResolvedIdentifier;
        }
    }
}