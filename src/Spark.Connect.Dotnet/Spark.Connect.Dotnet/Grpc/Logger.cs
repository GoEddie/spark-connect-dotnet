namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Logger
/// </summary>
public class Logger
{
    private static int _level;

    /// <summary>
    /// Create the logger at whatever level you like
    /// </summary>
    /// <param name="level"></param>
    public Logger(int level)
    {
        _level = level;
    }

    /// <summary>
    /// Log
    /// </summary>
    /// <param name="message"></param>
    public static void WriteLine(string message)
    {
        if (_level > 1)
        {
            Console.WriteLine(message);
        }
    }

    /// <summary>
    /// Log
    /// </summary>
    /// <param name="message"></param>
    /// <param name="parameters"></param>
    public static void WriteLine(string message, params object[] parameters)
    {
        if (_level > 1)
        {
            Console.WriteLine(message, parameters);
        }
    }
}