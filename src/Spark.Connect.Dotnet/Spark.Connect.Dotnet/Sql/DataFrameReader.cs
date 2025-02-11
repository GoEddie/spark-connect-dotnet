using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Sql;

public class DataFrameReader
{
    private readonly MapField<string, string> _options = new();
    private readonly SparkSession _session;
    private string _format = string.Empty;

    private string _schema = string.Empty;

    protected internal DataFrameReader(SparkSession session)
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
    protected internal DataFrameReader()
    {
    }
#pragma warning restore CS8618

    /// <summary>
    /// Adds an input option for the underlying data source.
    /// </summary>
    /// <param name="key">The key for the option to set.</param>
    /// <param name="value">The value for the option to set. It can be any primitive type or nullable type.</param>
    /// <returns>A <see cref="DataFrameReader"/> with the option set.</returns>
    public DataFrameReader Option(string key, string value)
    {
        _options.Add(key, value);
        return this;
    }


    /// <summary>
    /// Adds input options for the underlying data source.
    /// </summary>
    /// <param name="options">A list of key-value pairs where each key is a string and each value is a string representing the options to be set.</param>
    /// <returns>A <see cref="DataFrameReader"/> with the specified options set.</returns>
    public DataFrameReader Options(List<KeyValuePair<string, string>> options)
    {
        foreach (var option in options)
        {
            _options.Add(option.Key, option.Value);
        }

        return this;
    }

    /// <summary>
    /// Adds input options for the underlying data source.
    /// </summary>
    /// <param name="options">A dictionary of string keys and string values representing the options to be set.</param>
    /// <returns>A <see cref="DataFrameReader"/> with the specified options set.</returns>
    public DataFrameReader Options(Dictionary<string, string> options)
    {
        foreach (var option in options)
        {
            _options.Add(option.Key, option.Value);
        }

        return this;
    }

    /// <summary>
    /// Sets the schema of the DataFrame using a string representing the schema's DDL (Data Definition Language).
    /// </summary>
    /// <param name="schema">A string representing the schema in DDL format.</param>
    /// <returns>A <see cref="DataFrameReader"/> with the specified schema.</returns>
    public DataFrameReader Schema(string schema)
    {
        _schema = schema;
        return this;
    }

    /// <summary>
    /// Sets the schema of the DataFrame using a <see cref="StructType"/> object.
    /// </summary>
    /// <param name="schema">A <see cref="StructType"/> object representing the schema.</param>
    /// <returns>A <see cref="DataFrameReader"/> with the specified schema.</returns>
    public DataFrameReader Schema(StructType schema)
    {
        _schema = schema.FieldsAsDdl();
        return this;
    }

    public DataFrameReader Schema(AnalyzePlanRequest.Types.Schema schema)
    {
        throw new NotImplementedException();
    }

    /// <summary>
    /// Loads JSON files from the specified path and returns the results as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="path">The path to the JSON file.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded data.</returns>
    public DataFrame Json(string path)
    {
        return Read(new[] { path }, "json", _options, _schema);
    }

    /// <summary>
    /// Loads JSON files from the specified paths and returns the results as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="paths">An array of paths to the JSON files.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded data.</returns>
    public DataFrame Json(params string[] paths)
    {
        return Read(paths, "json", _options, _schema);
    }

    /// <summary>
    /// Loads a CSV file and returns the result as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="path">The path to the CSV file.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded CSV data.</returns>
    public DataFrame Csv(string path)
    {
        return Read(new[] { path }, "csv", _options, _schema);
    }

    /// <summary>
    /// Loads a CSV file from the specified paths and returns the result as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="paths">An array of paths to the CSV files.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded CSV data.</returns>
    public DataFrame Csv(params string[] paths)
    {
        return Read(paths, "csv", _options, _schema);
    }

    /// <summary>
    /// Loads ORC files and returns the result as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="path">The path to the ORC file.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded ORC data.</returns>
    public DataFrame Orc(string path)
    {
        return Read(new[] { path }, "orc", _options, _schema);
    }

    /// <summary>
    /// Loads ORC files from the specified paths and returns the result as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="paths">An array of paths to the ORC files.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded ORC data.</returns>
    public DataFrame Orc(params string[] paths)
    {
        return Read(paths, "orc", _options, _schema);
    }

    /// <summary>
    /// Loads Parquet files and returns the result as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="path">The path to the Parquet file.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded Parquet data.</returns>
    public DataFrame Parquet(string path)
    {
        return Read(new[] { path }, "parquet", _options, _schema);
    }

    /// <summary>
    /// Loads Parquet files from the specified paths and returns the result as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="paths">An array of paths to the Parquet files.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded Parquet data.</returns>
    public DataFrame Parquet(params string[] paths)
    {
        return Read(paths, "parquet", _options, _schema);
    }

    /// <summary>
    /// Loads data from a data source and returns it as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="path">The path to the data source.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded data.</returns>
    public DataFrame Load(string path)
    {
        return Read(new[] { path }, _format, _options, _schema);
    }

    /// <summary>
    /// Loads data from the specified paths and returns it as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="paths">An array of paths to the data sources.</param>
    /// <returns>A <see cref="DataFrame"/> containing the loaded data.</returns>
    public DataFrame Load(params string[] paths)
    {
        return Read(paths, _format, _options, _schema);
    }

    /// <summary>
    /// Reads data from the specified paths and returns it as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="paths">An array of paths to the data sources.</param>
    /// <param name="format">The format of the data source (e.g., "csv", "json", "orc", "parquet").</param>
    /// <param name="options">A map of additional options to customize the data source reading behavior.</param>
    /// <param name="schema">An optional schema for the data. If not provided, schema inference will be performed.</param>
    /// <returns>A <see cref="DataFrame"/> containing the data read from the specified paths.</returns>
    public DataFrame Read(string[] paths, string format, MapField<string, string> options, string schema)
    {
        var datasource = new Read.Types.DataSource
        {
            Format = format,
            Paths = { paths }
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
                    DataSource = datasource
                }
            }
        };
        //TODO: What do we do here??
        return new DataFrame(_session, plan.Root, new DataType());
    }

    /// <summary>
    /// Constructs a <see cref="DataFrame"/> representing the database table accessible via a JDBC URL and connection properties.
    /// </summary>
    /// <param name="paths">The JDBC URL(s) for the data source.</param>
    /// <param name="format">The format of the data source, e.g., "jdbc".</param>
    /// <param name="options">A map of options to customize the JDBC connection (e.g., connection properties, table name, etc.).</param>
    /// <param name="schema">An optional schema for the data. If not provided, schema inference will be performed.</param>
    /// <param name="predicates">A list of predicates for partitioning the data (optional).</param>
    /// <returns>A <see cref="DataFrame"/> representing the database table.</returns>
    public DataFrame Jdbc(string[] paths, string format, MapField<string, string> options, string schema,
        RepeatedField<string> predicates)
    {
        var datasource = new Read.Types.DataSource
        {
            Format = format,
            Paths = { paths }
        };

        if (options.Count > 0)
        {
            datasource.Options.Add(options);
        }

        if (!string.IsNullOrEmpty(schema))
        {
            datasource.Schema = schema;
        }

        foreach (var predicate in predicates)
        {
            datasource.Predicates.Add(predicate);
        }

        var plan = new Plan
        {
            Root = new Relation
            {
                Read = new Read
                {
                    DataSource = datasource
                }
            }
        };

        return new DataFrame(_session, plan.Root);
    }

    /// <summary>
    /// Specifies the input data source format.
    /// </summary>
    /// <param name="format">The name of the data source format, e.g., 'json', 'parquet', 'csv', etc.</param>
    /// <returns>A <see cref="DataFrameReader"/> instance that can be used to read data in the specified format.</returns>
    public DataFrameReader Format(string format)
    {
        _format = format;
        return this;
    }

    /// <summary>
    /// Returns the specified table as a <see cref="DataFrame"/>.
    /// </summary>
    /// <param name="name">The name of the table to load as a DataFrame.</param>
    /// <returns>A <see cref="DataFrame"/> representing the specified table.</returns>
    public DataFrame Table(string name)
    {
        var plan = new Plan
        {
            Root = new Relation
            {
                Read = new Read
                {
                    NamedTable = new Read.Types.NamedTable
                    {
                        UnparsedIdentifier = name
                    }
                }
            }
        };

        return new DataFrame(_session, plan.Root);
    }
}