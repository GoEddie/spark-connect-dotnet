using Google.Protobuf;
using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql.Streaming;

public class DataStreamWriter
{
    private readonly Relation _input;

    private readonly MapField<string, string> _options = new();
    private readonly SparkSession _session;
    private bool? _availableNow;
    private string _continuous = string.Empty;

    private string _format = string.Empty;
    private bool? _once;
    private string _outputMode = string.Empty;
    private readonly List<string> _paritionBy = new();
    private string _path = string.Empty;
    private string _processingTime = string.Empty;
    private string _queryName = string.Empty;
    private readonly string _udf_base64_function = string.Empty;

    private readonly string _udf_pythonVersion = string.Empty;

    public DataStreamWriter(SparkSession session, Relation input)
    {
        _session = session;
        _input = input;
    }

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

        if (!string.IsNullOrEmpty(_udf_base64_function))
        {
            operation.ForeachWriter = new StreamingForeachFunction
            {
                PythonFunction = new PythonUDF
                {
                    Command = ByteString.FromBase64(_udf_base64_function),
                    PythonVer = _udf_pythonVersion
                }
            };
        }

        operation.Input = _input;

        return operation;
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

    public DataStreamWriter Trigger(string processingTime = null, bool? once = null, string continuous = null,
        bool? availableNow = null)
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

        var plan = new Plan
        {
            Command = new Command
            {
                WriteStreamOperationStart = writeStreamOperationStart
            }
        };

        var task = GrpcInternal.ExecStreamingResponse(_session, plan);
        task.Wait();
        return new StreamingQuery(_session, task.Result.Item1, task.Result.Item2);
    }

    public StreamingQuery Start(string? path = null, string? tableName = null, string? format = null,
        string? outputMode = null, string[]? partitionBy = null, string? queryName = null,
        Dictionary<string, string>? options = null)
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

        var plan = new Plan
        {
            Command = new Command
            {
                WriteStreamOperationStart = writeStreamOperationStart
            }
        };

        var task = GrpcInternal.ExecStreamingResponse(_session, plan);
        task.Wait();

        var sq = new StreamingQuery(_session, task.Result.Item1, task.Result.Item2);
        _session.Streams.Add(sq);
        return sq;
    }

    // public DataStreamWriter ForEach()
    // {
    //     // _udf_base64_function =
    //     //     "gAWVjAQAAAAAAAAojB9weXNwYXJrLmNsb3VkcGlja2xlLmNsb3VkcGlja2xllIwOX21ha2VfZnVuY3Rpb26Uk5QoaACMDV9idWlsdGluX3R5cGWUk5SMCENvZGVUeXBllIWUUpQoSwJLAEsASwNLBEsTQ0KVAZcAfAFEAF0NfQICAIkDfAKmAQAAqwEAAAAAAAAAAAEAjA50AQAAAAAAAAAAAABnAKYBAACrAQAAAAAAAAAAUwCUToWUjARpdGVylIWUjAFflIwIaXRlcmF0b3KUjAF4lIeUjCwvVXNlcnMvZWQvZ2l0L2dvZWRkaWUvcHl0aG9uLy4vZ2V0X3B5dGhvbi5weZSMFGZ1bmNfd2l0aG91dF9wcm9jZXNzlIw5X2NvbnN0cnVjdF9mb3JlYWNoX2Z1bmN0aW9uLjxsb2NhbHM+LmZ1bmNfd2l0aG91dF9wcm9jZXNzlEseQyz4gADYFR3wAAENFfAAAQ0VkAHYEBGQAZAhkQSUBJAEkATdExeYApE4lDiIT5RDAJSMAWaUhZQpdJRSlH2UKIwLX19wYWNrYWdlX1+UTowIX19uYW1lX1+UjAhfX21haW5fX5SMCF9fZmlsZV9flGgQdU5OaACMEF9tYWtlX2VtcHR5X2NlbGyUk5QpUpSFlHSUUpSMJHB5c3BhcmsuY2xvdWRwaWNrbGUuY2xvdWRwaWNrbGVfZmFzdJSMEl9mdW5jdGlvbl9zZXRzdGF0ZZSTlGgjfZR9lChoG2gRjAxfX3F1YWxuYW1lX1+UaBKMD19fYW5ub3RhdGlvbnNfX5R9lIwOX19rd2RlZmF1bHRzX1+UTowMX19kZWZhdWx0c19flE6MCl9fbW9kdWxlX1+UaByMB19fZG9jX1+UTowLX19jbG9zdXJlX1+UaACMCl9tYWtlX2NlbGyUk5RoAihoByhLAUsASwBLAUsESwNDKpcAdAEAAAAAAAAAAAAAZAF8AJsAnQKmAQAAqwEAAAAAAAAAAAEAZABTAJROjDIqKioqKioqKioqKioqKioqKioqKioqKioqKioqKiBSb3cgRnJvbSBQeXRob25VREY6IJSGlIwFcHJpbnSUhZSMA3Jvd5SFlGgQjAZteV91ZGaUaDpLCEMfgADdBAnQCkS4c9AKRNAKRNEERdQERdAERdAERdAERZRoFCkpdJRSlGgZTk5OdJRSlGgmaD99lH2UKGgbaDpoKWg6aCp9lGg4jBFweXNwYXJrLnNxbC50eXBlc5SMA1Jvd5STlHNoLE5oLU5oLmgcaC9OaDBOjBdfY2xvdWRwaWNrbGVfc3VibW9kdWxlc5RdlIwLX19nbG9iYWxzX1+UfZR1hpSGUjCFlFKUhZRoRl2UaEh9lHWGlIZSME6ME3B5c3Bhcmsuc2VyaWFsaXplcnOUjBVBdXRvQmF0Y2hlZFNlcmlhbGl6ZXKUk5QpgZR9lCiMCnNlcmlhbGl6ZXKUaFGMFUNsb3VkUGlja2xlU2VyaWFsaXplcpSTlCmBlIwJYmF0Y2hTaXpllEsAjAhiZXN0U2l6ZZRKAAABAHViaFR0lC4=";
    //     // _udf_pythonVersion = "3.11";
    //     //If we have to support udf's - just testing whether we can pass up python code as base64 - seems to work
    //     return this;
    // }
}