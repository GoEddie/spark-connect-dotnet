namespace Spark.Connect.Dotnet.Pipelines.Attributes;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class TemporaryViewAttribute : Attribute
{
    public string? Name { get; set; }
    public string? Comment { get; set; }
    public bool Once { get; set; }
}