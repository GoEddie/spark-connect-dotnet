using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql;

public class DataFrameWriter
{
    private readonly SparkSession _session;
    private readonly DataFrame _what;
    private readonly MapField<string, string> _options = new MapField<string, string>();
    
    private WriteOperation.Types.SaveMode _saveMode;
    private List<string> _partitionColumnNames = new();
    private string _format = String.Empty;

    protected internal DataFrameWriter(SparkSession session, DataFrame what)
    {
        _session = session;
        _what = what;
    }

    /// <summary>
    /// Mockable `DataFrameWriter` constructor. If you would like to write unit tests using `DataFrameWriter` and would like to mock it then
    ///  you can use this constructor. It is not to be used in the actual code, if you try to then any call is guaranteed to fail.
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

    private WriteOperation.Types.SaveMode SaveModeFromString(string saveMode) => saveMode.ToLower() switch
    {
        "overwrite" => WriteOperation.Types.SaveMode.Overwrite,
        "append" => WriteOperation.Types.SaveMode.Append,
        "ignore" => WriteOperation.Types.SaveMode.Ignore,
        "error" or "errorifexists" => WriteOperation.Types.SaveMode.ErrorIfExists,
        _ => throw new ArgumentException(
            $"'{saveMode}' is not a valid save mode ('Overwrite', 'Append', 'Ignore', 'Error', or 'ErrorIfExists'")
    };
    
    public DataFrameWriter Option(string key, string value)
    {
        _options.Add(key, value);
        return this;
    }

    public DataFrameWriter PartitionBy(params string[] columnNames)
    {
        _partitionColumnNames = columnNames.ToList();
        return this;
    }

    public DataFrameWriter Format(string format)
    {
        _format = format;
        return this;
    }

    public void Orc(string path) => Write("orc", path);
    public void Parquet(string path) => Write("parquet", path);
    public void Json(string path) => Write("json", path);
    public void Csv(string path) => Write("csv", path);
    
    public void Write(string format, string path)
    {
        Task.Run(() => WriteAsync(format, path, _options)).Wait();
    }
    
    public void Write(string path)
    {
        Task.Run(() => WriteAsync(_format, path, _options)).Wait();
    }
    
    public async Task WriteAsync(string format, string path, MapField<string, string> options)
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                WriteOperation = new WriteOperation()
                {
                    Mode = WriteOperation.Types.SaveMode.Overwrite,
                    Source = format,
                    Options = { options },
                    Input = _what.Relation,
                    Path = path
                }
            }
        };
        
        await GrpcInternal.Exec(_session.Client,  _session.Host, _session.SessionId, plan, _session.Headers, _session.UserContext, _session.ClientType);
    } 
}