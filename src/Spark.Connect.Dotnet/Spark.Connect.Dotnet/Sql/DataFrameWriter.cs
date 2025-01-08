using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql;

public class DataFrameWriter
{
    private readonly MapField<string, string> _options = new();
    private readonly SparkSession _session;
    private readonly DataFrame _what;

    private List<string> _bucketColumnNames = new();
    private string _format = string.Empty;
    private int _numBuckets;
    private List<string> _partitionColumnNames = new();

    private WriteOperation.Types.SaveMode _saveMode;

    protected internal DataFrameWriter(SparkSession session, DataFrame what)
    {
        _session = session;
        _what = what;
    }

    /// <summary>
    ///     Mockable `DataFrameWriter` constructor. If you would like to write unit tests using `DataFrameWriter` and would
    ///     like to mock it then
    ///     you can use this constructor. It is not to be used in the actual code, if you try to then any call is guaranteed to
    ///     fail.
    /// </summary>
#pragma warning disable CS8618
    protected internal DataFrameWriter()
    {
    }
#pragma warning restore CS8618

    public DataFrameWriter Mode(string saveMode)
    {
        _saveMode = SaveModeFromString(saveMode);
        return this;
    }

    private WriteOperation.Types.SaveMode SaveModeFromString(string saveMode)
    {
        return saveMode.ToLower() switch
        {
            "overwrite" => WriteOperation.Types.SaveMode.Overwrite, "append" => WriteOperation.Types.SaveMode.Append, "ignore" => WriteOperation.Types.SaveMode.Ignore
            , "error" or "errorifexists" => WriteOperation.Types.SaveMode.ErrorIfExists, _ => throw new ArgumentException(
                $"'{saveMode}' is not a valid save mode ('Overwrite', 'Append', 'Ignore', 'Error', or 'ErrorIfExists'")
        };
    }

    public DataFrameWriter Option(string key, string value)
    {
        if (_options.ContainsKey(key))
        {
            _options.Remove(key);
        }
        
        _options.Add(key, value);
        return this;
    }

    public DataFrameWriter Options(IDictionary<string, string> options)
    {
        foreach (var option in options)
        {
            Option(option.Key, option.Value);
        }

        return this;
    }

    public DataFrameWriter PartitionBy(params string[] columnNames)
    {
        _partitionColumnNames = columnNames.ToList();
        return this;
    }

    public DataFrameWriter BucketBy(int numBuckets, params string[] columnNames)
    {
        _numBuckets = numBuckets;
        _bucketColumnNames = columnNames.ToList();
        return this;
    }

    public DataFrameWriter Format(string format)
    {
        _format = format;
        return this;
    }

    public void Orc(string path)
    {
        Write("orc", path);
    }

    public void Parquet(string path)
    {
        Write("parquet", path);
    }

    public void Json(string path)
    {
        Write("json", path);
    }

    public void Csv(string path)
    {
        Write("csv", path);
    }

    public void Write(string format, string path)
    {
        Task.Run(() => WriteAsync(format, path, _options)).Wait();
    }

    public void Write(string path)
    {
        Task.Run(() => WriteAsync(_format, path, _options)).Wait();
    }

    public void Save(string format, string path)
    {
        Task.Run(() => WriteAsync(format, path, _options)).Wait();
    }

    public void Save(string path)
    {
        Task.Run(() => WriteAsync(_format, path, _options)).Wait();
    }

    public async Task WriteAsync(string format, string path, MapField<string, string> options)
    {
        var plan = new Plan
        {
            Command = new Command
            {
                WriteOperation = new WriteOperation
                {
                    Mode = _saveMode, Source = format, Options = { options }, Input = _what.Relation, PartitioningColumns = { _partitionColumnNames } ,Path = path
                }
            }
        };

        if (_bucketColumnNames.Any())
        {
            plan.Command.WriteOperation.BucketBy = new WriteOperation.Types.BucketBy
            {
                NumBuckets = _numBuckets, BucketColumnNames = { _bucketColumnNames }
            };
        }

        var executor = new RequestExecutor(_session, plan);
        await executor.ExecAsync();
    }

    public async Task SaveAsTableAsync(string name, string? format, string? mode = null, List<string>? partitions = null, Dictionary<string, string>? options = null)
    {
        if (mode.IsNotNullAndIsNotEmpty())
        {
            Mode(mode);
        }

        if (options != null)
        {
            Options(options);    
        }
        
        var plan = new Plan
        {
            Command = new Command
            {
                WriteOperation = new WriteOperation
                {
                    Mode = _saveMode, 
                    Input = _what.Relation,
                    PartitioningColumns = { _partitionColumnNames }, 
                    Table = new WriteOperation.Types.SaveTable()
                    {
                        TableName = name, SaveMethod = WriteOperation.Types.SaveTable.Types.TableSaveMethod.SaveAsTable
                    }
                }
            }
        };
        
        if (_bucketColumnNames.Any())
        {
            plan.Command.WriteOperation.BucketBy = new WriteOperation.Types.BucketBy
            {
                NumBuckets = _numBuckets, BucketColumnNames = { _bucketColumnNames }
            };
        }
        var executor = new RequestExecutor(_session, plan);
        await executor.ExecAsync();
    }
    
    public void SaveAsTable(string name, string? format, string? mode = null, List<string>? partitions = null, Dictionary<string, string>? options = null)
    {
        Task.Run(() => SaveAsTableAsync(name, format, mode, partitions, options)).Wait();
    }
}