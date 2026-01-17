namespace Spark.Connect.Dotnet.Pipelines.Attributes;

/// <summary>
/// Specifies the source for a Streaming Table, the `DataFrame` returned must have been generated using `spark.readStream` or another streaming source
/// </summary>
/// <example>
/// <code>
/// [StreamingTable]
/// public DataFrame TableName(SparkSession spark)
/// {
///     return spark
///         .ReadStream()
///         .Format("rate")
///         .Option("rowsPerSecond", "1")
///         .Load();
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class StreamingTableAttribute : Attribute
{
    public string? Name { get; set; }
    public string? Comment { get; set; }
    public string[]? PartitionCols { get; set; }
    public string? Format { get; set; }
    public string[]? ClusteringColumns { get; set; }
    public bool Once { get; set; }
}