using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql;

public class DataFrameWriterV2
{
    private readonly List<Expression> _partitionedBy = new();
    private readonly SparkSession _session;
    private readonly string _tableName;
    private readonly DataFrame _what;

    public DataFrameWriterV2(string tableName, SparkSession session, DataFrame what)
    {
        _tableName = tableName;
        _session = session;
        _what = what;
    }

    public DataFrameWriterV2 PartitionedBy(Column col)
    {
        _partitionedBy.Add(col.Expression);
        return this;
    }

    public void CreateOrReplace()
    {
        Task.Run(CreateOrReplaceAsync).Wait();
    }

    public async Task CreateOrReplaceAsync()
    {
        var plan = new Plan
        {
            Command = new Command
            {
                WriteOperationV2 = new WriteOperationV2
                {
                    Input = _what.Relation, TableName = _tableName, Mode = WriteOperationV2.Types.Mode.Create, PartitioningColumns = { _partitionedBy }
                }
            }
        };
        
        var executor = new RequestExecutor(_session, plan);
        await executor.ExecAsync();
    }
}