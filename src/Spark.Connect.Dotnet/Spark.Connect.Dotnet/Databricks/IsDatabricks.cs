namespace Spark.Connect.Dotnet.Databricks;

public class IsDatabricks
{
    public static bool Url(string url)
    {
        return url.Contains("databricks", StringComparison.OrdinalIgnoreCase);
    }
}