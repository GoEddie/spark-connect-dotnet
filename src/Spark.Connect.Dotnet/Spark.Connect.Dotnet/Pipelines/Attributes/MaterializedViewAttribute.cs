namespace Spark.Connect.Dotnet.Pipelines.Attributes;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class MaterializedViewAttribute : Attribute
{
    public string? Name { get; set; }
    public string? Comment { get; set; }
    public string[]? PartitionCols { get; set; }
    public string? Format { get; set; }
    public string[]? ClusteringColumns { get; set; }
    public bool Once { get; set; }
}