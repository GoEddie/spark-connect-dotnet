using Delta.Connect;
using Google.Protobuf.WellKnownTypes;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.DeltaLake;

public class DeltaOptimizeBuilder(SparkSession spark, DeltaTable table)
{
    private List<string> _partitionFilters = new List<string>();
    
    public DeltaOptimizeBuilder Where(string partitionFilter)
    {
        _partitionFilters.Add(partitionFilter);
        return this;
    }

    public DataFrame ExecuteCompaction()
    {
        var optimizeTable = new OptimizeTable()
        {
            Table = table.ProtoDeltaTable
        };

       
        if (_partitionFilters.Any())
        {
            optimizeTable.PartitionFilters.AddRange(_partitionFilters);
        }

        var relation = new DeltaRelation()
        {
            OptimizeTable = optimizeTable
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };

        var plan = new Plan()
        {
            Root = sparkRelation
        };
        
        var requestExecutor = new RequestExecutor(spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();

        return new DataFrame(spark, requestExecutor.GetRelation());
    }
    
    public DataFrame ExecuteZOrderBy(params string[] zOrderBy)
    {
        var optimizeTable = new OptimizeTable()
        {
            Table = table.ProtoDeltaTable
        };

        if (_partitionFilters.Any())
        {
            optimizeTable.PartitionFilters.AddRange(_partitionFilters);
        }

        optimizeTable.ZorderColumns.AddRange(zOrderBy);
        
        var relation = new DeltaRelation()
        {
            OptimizeTable = optimizeTable
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };

        var plan = new Plan()
        {
            Root = sparkRelation
        };
        
        var requestExecutor = new RequestExecutor(spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();

        return new DataFrame(spark, requestExecutor.GetRelation());
    }
    
}