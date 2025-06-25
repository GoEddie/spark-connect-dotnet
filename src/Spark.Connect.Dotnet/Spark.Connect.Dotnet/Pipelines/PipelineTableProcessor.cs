using System.Data.Common;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Pipelines;

public class PipelineTableProcessor
{
    private readonly SparkSession _spark;

    public PipelineTableProcessor(SparkSession spark, PipelineGraph graph, string tableName, DataFrame source, SparkDataType? schema = null, IDictionary<string, string>? options = null, IDictionary<string, string>? sqlConfs = null, bool? once = null, string? format = null, string? comment = null, string[]? partitionCols = null)
    {
        _spark = spark;
        
        var datasetName = tableName;
        if (datasetName.Contains("."))
        {
            datasetName = datasetName.Split(".").Last();
        }

        
        DatasetPlan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    DefineDataset = new PipelineCommand.Types.DefineDataset()
                    {
                        DatasetName = datasetName,
                        DatasetType = DatasetType.Table,
                        DataflowGraphId = graph.GraphId
                    }
                }
            }
        };

        if (schema != null)
        {
            DatasetPlan.Command.PipelineCommand.DefineDataset.Schema = schema.ToDataType();
        }

        if (options != null)
        {
            DatasetPlan.Command.PipelineCommand.DefineDataset.TableProperties.Add(options);
        }

        if (!string.IsNullOrEmpty(format))
        {
            DatasetPlan.Command.PipelineCommand.DefineDataset.Format = format;
        }

        if (!string.IsNullOrEmpty(comment))
        {
            DatasetPlan.Command.PipelineCommand.DefineDataset.Comment = comment;
        }

        if (partitionCols != null)
        {   
            DatasetPlan.Command.PipelineCommand.DefineDataset.PartitionCols.AddRange(partitionCols);
        }
        
        FlowPlan = new Plan()
        {
            Command = new Command()
            {
                PipelineCommand = new PipelineCommand()
                {
                    DefineFlow = new PipelineCommand.Types.DefineFlow()
                    {
                        TargetDatasetName = tableName,
                        DataflowGraphId = graph.GraphId,
                        Plan = source.Relation,
                        FlowName = $"flow_{tableName.Replace(".", "__")}"
                    }
                }
            }
        };

        if (once.HasValue)
        {
            FlowPlan.Command.PipelineCommand.DefineFlow.Once = once.Value;
        }

        if (sqlConfs != null)
        {
            FlowPlan.Command.PipelineCommand.DefineFlow.SqlConf.Add(sqlConfs);
        }
    }
    
    public readonly Plan DatasetPlan;
    public readonly Plan FlowPlan;
    
    public void Create()
    {
        var requestExecutor = new RequestExecutor(_spark, DatasetPlan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
        
        requestExecutor = new RequestExecutor(_spark, FlowPlan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
    }
}