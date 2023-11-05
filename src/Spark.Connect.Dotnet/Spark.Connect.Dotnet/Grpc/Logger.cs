namespace Spark.Connect.Dotnet.Grpc;

public class Logger
{
    private static int _level = 0;

    public Logger(int level)
    {
        _level = level;
    }
    
    public static void WriteLine(string message)
    {
        if (_level > 1)
        {
            Console.WriteLine(message);
        }
    }
    
    public static void WriteLine(string message, params object[] parameters)
    {
        if (_level > 1)
        {
            Console.WriteLine(message, parameters);
        }
    }
}