namespace Spark.Connect.Dotnet.Pipelines.Attributes;

[AttributeUsage(AttributeTargets.Method , AllowMultiple = false)]
public class SqlConfForAttribute() : Attribute
{
    public string[]? Names { get; set; }
    public string? Name { get; set; }
}