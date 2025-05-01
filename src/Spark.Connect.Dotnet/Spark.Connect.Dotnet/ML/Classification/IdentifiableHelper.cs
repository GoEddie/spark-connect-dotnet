namespace Spark.Connect.Dotnet.ML.Classification;

/// <summary>
/// Provides functionality similar to Scala's Identifiable class in Spark.
/// </summary>
public static class IdentifiableHelper
{
    /// <summary>
    /// Generates a random UID with the specified prefix.
    /// </summary>
    /// <param name="prefix">The prefix to use in the UID.</param>
    /// <returns>A string containing the prefix followed by a random UUID.</returns>
    public static string RandomUID(string prefix)
    {
        string uuid = Guid.NewGuid().ToString().Replace("-", "");
        return $"{prefix}_{uuid}";
    }
}