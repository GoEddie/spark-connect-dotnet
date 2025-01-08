namespace Spark.Connect.Dotnet;

public static class StringIsNotNullExtensions
{
    public static bool IsNotNullAndIsNotEmpty(this string str) => !string.IsNullOrEmpty(str);
}