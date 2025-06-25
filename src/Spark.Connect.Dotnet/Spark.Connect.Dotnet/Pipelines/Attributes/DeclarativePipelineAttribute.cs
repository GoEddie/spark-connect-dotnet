namespace Spark.Connect.Dotnet.Pipelines.Attributes;

[AttributeUsage(AttributeTargets.Class , AllowMultiple = false)]
public class DeclarativePipelineAttribute() : Attribute
{
    public string? DefaultDatabase { get; set; }
    public string? DefaultCatalog { get; set; }
}