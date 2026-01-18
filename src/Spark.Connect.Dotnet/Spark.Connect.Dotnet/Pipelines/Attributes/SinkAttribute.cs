namespace Spark.Connect.Dotnet.Pipelines.Attributes;

/// <summary>
/// Specifies a Sink output that writes streaming data to an external destination.
/// </summary>
/// <example>
/// <code>
/// [Sink(Format = "kafka")]
/// public DataFrame MyKafkaSink(SparkSession spark)
/// {
///     return spark.ReadStream().Table("my_table");
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class SinkAttribute : Attribute
{
    public string? Name { get; set; }
    public string? Comment { get; set; }
    public string? Format { get; set; }
    public bool Once { get; set; }
}
