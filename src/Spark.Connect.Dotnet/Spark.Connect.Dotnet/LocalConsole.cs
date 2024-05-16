namespace Spark.Connect.Dotnet;

public class LocalConsole
{
    public virtual void Write(string what)
    {
        Console.Write(what);
    }
    
    public virtual void WriteLine(string what)
    {
        Console.WriteLine(what);
    }
}