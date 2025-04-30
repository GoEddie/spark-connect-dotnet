namespace Spark.Connect.Dotnet.ML.Classification;

/// <summary>
/// Provides functionality similar to Scala's Identifiable class in Spark.
/// </summary>
public static class IdentifiableHelper
{
    private static readonly Random _random = new Random();
    private static readonly object _lock = new object();
    
    /// <summary>
    /// Generates a random UID with the specified prefix.
    /// </summary>
    /// <param name="prefix">The prefix to use in the UID.</param>
    /// <returns>A string containing the prefix followed by a random UUID.</returns>
    public static string RandomUID(string prefix)
    {
        lock (_lock)
        {
            // Generate a random value similar to Scala's implementation
            string uuid = Guid.NewGuid().ToString().Replace("-", "");
            
            // Return the prefix + randomized part
            return $"{prefix}_{uuid}";
        }
    }
}