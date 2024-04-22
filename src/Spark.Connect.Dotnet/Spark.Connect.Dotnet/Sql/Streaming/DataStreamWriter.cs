using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Grpc;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Sql.Streaming;

public class DataStreamWriter
{
    private readonly SparkSession _session;
    private readonly Relation _input;
    
    private readonly MapField<string, string> _options = new();

    private string _format = String.Empty;
    private string _path = String.Empty;
    private string _outputMode = String.Empty;
    private List<string> _paritionBy = new();
    private string _queryName = String.Empty;
    private string _processingTime = String.Empty;
    private bool? _once;
    private string _continuous = String.Empty;
    private bool? _availableNow;
    
    private WriteStreamOperationStart BuildWriteStartOperation()
    {
        var operation = new WriteStreamOperationStart();
        if (!string.IsNullOrEmpty(_format))
        {
            operation.Format = _format;
        }

        if (!string.IsNullOrEmpty(_path))
        {
            operation.Path = _path;
        }

        if (!string.IsNullOrEmpty(_outputMode))
        {
            operation.OutputMode = _outputMode;
        }

        if (_paritionBy.Count > 0)
        {
            operation.PartitioningColumnNames.AddRange(_paritionBy);
        }

        if (!string.IsNullOrEmpty(_queryName))
        {
            operation.QueryName = _queryName;
        }

        if (!string.IsNullOrEmpty(_processingTime))
        {
            operation.ProcessingTimeInterval = _processingTime;
        }

        if (_once.HasValue)
        {
            operation.Once = _once.Value;
        }

        if (!string.IsNullOrEmpty(_continuous))
        {
            operation.ContinuousCheckpointInterval = _continuous;
        }

        if (_availableNow.HasValue)
        {
            operation.AvailableNow = _availableNow.Value;
        }

        if (_options.Count > 0)
        {
            operation.Options.Add(_options);
        }
        
        operation.Input = _input;
        return operation;
    }

    public DataStreamWriter(SparkSession session, Relation input)
    {
        _session = session;
        _input = input;
    }

    public DataStreamWriter Format(string format)
    {
        _format = format;
        return this;
    }

    public DataStreamWriter Option(string key, string value)
    {
        _options.Add(key, value);
        return this;
    }
    
    public DataStreamWriter Options(List<KeyValuePair<string, string>> options)
    {
        foreach (var option in options)
        {
            _options.Add(option.Key, option.Value);
        }
        return this;
    }
    
    public DataStreamWriter Options(Dictionary<string, string> options)
    {
        foreach (var option in options)
        {
            _options.Add(option.Key, option.Value);
        }
        return this;
    }
    
    public DataStreamWriter Path(string path)
    {
        _path = path;
        return this;
    }
    
    public DataStreamWriter OutputMode(string mode)
    {
        _outputMode = mode;
        return this;
    }

    public DataStreamWriter PartitionBy(params string[] cols)
    {
        _paritionBy.AddRange(cols);
        return this;
    }

    public DataStreamWriter QueryName(string name)
    {
        _queryName = name;
        return this;
    }

    public DataStreamWriter Trigger(string processingTime=null, bool? once=null, string continuous=null, bool? availableNow=null)
    {
        _processingTime = processingTime;
        _once = once;
        _continuous = continuous;
        _availableNow = availableNow;
        return this;
    }
    
    public StreamingQuery ToTable(string tableName)
    {
        var writeStreamOperationStart = BuildWriteStartOperation();
        writeStreamOperationStart.TableName = tableName;
        
        var plan = new Plan()
        {
            Command = new Command()
            {
                WriteStreamOperationStart = writeStreamOperationStart
            }
        };
        
        var task = GrpcInternal.ExecStreamingResponse(_session, plan);
        task.Wait();
        return new StreamingQuery(_session, task.Result.Item1, task.Result.Item2);
    }

    public StreamingQuery Start(string? path = null, string? tableName = null, string? format = null, string? outputMode = null, string[]? partitionBy=null, string? queryName=null, Dictionary<string, string>? options = null)
    {
        var writer = this;
        if (!string.IsNullOrEmpty(path))
        {
            writer = Path(path);
        }
        
        if (!string.IsNullOrEmpty(format))
        {
            writer = writer.Format(format);
        }

        if (!string.IsNullOrEmpty(outputMode))
        {
            writer = OutputMode(outputMode);
        }

        if (partitionBy?.Length > 0)
        {
            writer = PartitionBy(partitionBy);
        }

        if (!string.IsNullOrEmpty(queryName))
        {
            writer = QueryName(queryName);
        }

        if (options?.Count > 0)
        {
            writer = Options(options);
        }
        
        if (!string.IsNullOrEmpty(tableName))
        {
            return writer.ToTable(tableName);
        }

        return writer.StartInternal();
    }

    private StreamingQuery StartInternal()
    {
        var writeStreamOperationStart = BuildWriteStartOperation();
        
        var plan = new Plan()
        {
            Command = new Command()
            {
                WriteStreamOperationStart = writeStreamOperationStart
            }
        };
        
        var task = GrpcInternal.ExecStreamingResponse(_session, plan);
        task.Wait();
        return new StreamingQuery(_session, task.Result.Item1, task.Result.Item2);
    }
    
}