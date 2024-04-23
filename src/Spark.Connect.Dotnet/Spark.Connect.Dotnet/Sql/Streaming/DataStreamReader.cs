using Google.Protobuf.Collections;

namespace Spark.Connect.Dotnet.Sql.Streaming;

public class DataStreamReader
{
    private readonly MapField<string, string> _options = new();
    private readonly SparkSession _session;
    private string _format = string.Empty;

    private string _schema = string.Empty;

    protected internal DataStreamReader(SparkSession session)
    {
        _session = session;
    }

    /// <summary>
    ///     Mockable `DataFrameReader` constructor. If you would like to write unit tests using `DataFrameReader` and would
    ///     like to mock it then
    ///     you can use this constructor. It is not to be used in the actual code, if you try to then any call is guaranteed to
    ///     fail.
    /// </summary>
#pragma warning disable CS8618
    protected internal DataStreamReader()
    {
    }
#pragma warning restore CS8618

    public DataStreamReader Option(string key, string value)
    {
        _options.Add(key, value);
        return this;
    }

    public DataStreamReader Options(List<KeyValuePair<string, string>> options)
    {
        foreach (var option in options)
        {
            _options.Add(option.Key, option.Value);
        }

        return this;
    }

    public DataStreamReader Options(Dictionary<string, string> options)
    {
        foreach (var option in options)
        {
            _options.Add(option.Key, option.Value);
        }

        return this;
    }

    public DataStreamReader Schema(string schema)
    {
        _schema = schema;
        return this;
    }

    public DataStreamReader Schema(AnalyzePlanRequest.Types.Schema schema)
    {
        throw new NotImplementedException();
    }

    public DataStreamReader Format(string format)
    {
        _format = format;
        return this;
    }

    public DataFrame Text(string path)
    {
        return Read(new[] { path }, "text", _options, _schema);
    }

    public DataFrame Json(string path)
    {
        return Read(new[] { path }, "json", _options, _schema);
    }

    public DataFrame Json(params string[] paths)
    {
        return Read(paths, "json", _options, _schema);
    }

    public DataFrame Csv(string path)
    {
        return Read(new[] { path }, "csv", _options, _schema);
    }

    public DataFrame Csv(params string[] paths)
    {
        return Read(paths, "csv", _options, _schema);
    }

    public DataFrame Orc(string path)
    {
        return Read(new[] { path }, "orc", _options, _schema);
    }

    public DataFrame Orc(params string[] paths)
    {
        return Read(paths, "orc", _options, _schema);
    }

    public DataFrame Parquet(string path)
    {
        return Read(new[] { path }, "parquet", _options, _schema);
    }

    public DataFrame Parquet(params string[] paths)
    {
        return Read(paths, "parquet", _options, _schema);
    }

    public DataFrame Load(string path)
    {
        return Read(new[] { path }, _format, _options, _schema);
    }

    public DataFrame Load(params string[] paths)
    {
        return Read(paths, _format, _options, _schema);
    }

    private DataFrame Read(string[] paths, string format, MapField<string, string> options, string schema)
    {
        var datasource = new Read.Types.DataSource
        {
            Format = format, Paths = { paths }
        };

        if (options.Count > 0)
        {
            datasource.Options.Add(options);
        }

        if (!string.IsNullOrEmpty(schema))
        {
            datasource.Schema = schema;
        }

        var plan = new Plan
        {
            Root = new Relation
            {
                Read = new Read
                {
                    DataSource = datasource, IsStreaming = true
                }
            }
        };

        return new DataFrame(_session, plan.Root);
    }
}