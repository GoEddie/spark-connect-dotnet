using Delta.Connect;
using Google.Protobuf.WellKnownTypes;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.DeltaLake;

public class DeltaTableBuilder(SparkSession spark)
{
    private string _tableName = string.Empty;
    private string _location = string.Empty;
    private List<CreateDeltaTable.Types.Column> _columns = new List<CreateDeltaTable.Types.Column>();
    private List<string> _partitionColumns = new List<string>();
    private List<string> _clusterColumns = new List<string>();
    private IDictionary<string, string> _properties = new Dictionary<string, string>();
    private CreateDeltaTable.Types.Mode _mode;
   
    public DeltaTableBuilder Mode(CreateDeltaTable.Types.Mode mode)
    {
        _mode = mode;
        return this;
    }
    
    public DeltaTableBuilder TableName(string tableName)
    {
        _tableName = tableName;
        return this;
    }
    
    public DeltaTableBuilder Location(string location)
    {
        _location = location;
        return this;
    }

    public DeltaTableBuilder AddColumn(Delta.Connect.CreateDeltaTable.Types.Column column)
    {
        _columns.Add(column);
        return this;
    }

    public DeltaTableBuilder PartitionedBy(params string[] partitionColumns)
    {
        _partitionColumns.AddRange(partitionColumns);
        return this;
    }
    
    public DeltaTableBuilder ClusterBy(params string[] clusterColumns)
    {
        _clusterColumns.AddRange(clusterColumns);
        return this;
    }
    
    public DeltaTableBuilder Property(string key, string value)
    {
        _properties.Add(key, value);
        return this;
    }

    public DeltaTable Execute()
    {
        if(string.IsNullOrEmpty(_tableName) && string.IsNullOrEmpty(_location))
        {
            throw new Exception("Table name or location must be specified");
        }

        var relation = new CreateDeltaTable()
        {
            Mode = _mode,
        };

        if (!string.IsNullOrEmpty(_tableName))
        {
            relation.TableName = _tableName;
        }
        else
        {
            relation.Location = _location;
        }

        if (_partitionColumns.Any())
        {
            relation.PartitioningColumns.AddRange(_partitionColumns);
        }
        
        if (_clusterColumns.Any())
        {
            relation.ClusteringColumns.AddRange(_clusterColumns);
        }

        foreach (var property in _properties)
        {
            relation.Properties.Add(property.Key, property.Value);
        }

        relation.Columns.AddRange(_columns);

        var command = new DeltaCommand()
        {
            CreateDeltaTable = relation
        };
        
        var sparkCommand = new Command()
        {
            Extension = Any.Pack(command)
        };

        var plan = new Plan()
        {
            Command = sparkCommand
        };
        
        var requestExecutor = new RequestExecutor(spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();

        if (!string.IsNullOrEmpty(_tableName))
        {
            return DeltaTable.ForName(spark, _tableName);
        }
        else
        {
            return DeltaTable.ForPath(spark, _location);
        }
    }
}