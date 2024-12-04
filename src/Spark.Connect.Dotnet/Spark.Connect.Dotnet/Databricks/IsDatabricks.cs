namespace Spark.Connect.Dotnet.Databricks;

/// <summary>
/// Are we connecting to Databricks? used to control whether we wait for a connection.
/// </summary>
public class IsDatabricks
{
    /// <summary>
    /// Valiudates whether the url contains the name 'databricks'
    /// </summary>
    /// <param name="url"></param>
    /// <returns></returns>
    public static bool Url(string url)
    {
        return url.Contains("databricks", StringComparison.OrdinalIgnoreCase);
    }
}