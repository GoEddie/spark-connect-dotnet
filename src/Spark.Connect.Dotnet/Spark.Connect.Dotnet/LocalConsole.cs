namespace Spark.Connect.Dotnet;

/// <summary>
/// This is used by our tests to redirect stdout to the console
/// </summary>
public class LocalConsole
{
    /// <summary>
    /// Write to the console
    /// </summary>
    /// <param name="what"></param>
    public virtual void Write(string what)
    {
        Console.Write(what);
    }

    /// <summary>
    /// WriteLine to the console
    /// </summary>
    /// <param name="what"></param>
    public virtual void WriteLine(string what)
    {
        Console.WriteLine(what);
    }
}