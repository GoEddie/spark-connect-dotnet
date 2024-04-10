namespace Spark.Connect.Dotnet.Databricks;

public class IsDatabricks
{
    public static bool Url(string url) => url.Contains("databricks", StringComparison.OrdinalIgnoreCase);
}