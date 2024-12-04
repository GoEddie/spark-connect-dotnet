namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Used to log gRPC info, needs migrating to a better logger.
/// </summary>
public class GrpcLogger
{
    private readonly GrpcLoggingLevel _level;
    private readonly LocalConsole _console;

    /// <summary>
    /// Create a logger, pass in your own console if you want to redirect output (like in tests!)
    /// </summary>
    /// <param name="level"></param>
    /// <param name="console"></param>
    public GrpcLogger(GrpcLoggingLevel level, LocalConsole? console = null)
    {
        _level = level;
        _console = console ?? new LocalConsole();
        
    }
    

    /// <summary>
    /// Log
    /// </summary>
    /// <param name="level"></param>
    /// <param name="message"></param>
    public virtual void Log(GrpcLoggingLevel level, string message)
    {
        if (level >= _level)
        {
            _console.WriteLine(DateTime.Now + " :: " + message);
        }
    }
    
    /// <summary>
    /// Log
    /// </summary>
    /// <param name="level"></param>
    /// <param name="format"></param>
    /// <param name="args"></param>
    public virtual void Log(GrpcLoggingLevel level, string format, params object[] args)
    {
        if (level >= _level)
        {
            _console.WriteLine( DateTime.Now + " :: " + string.Format(format, args));
        }
    }
}

/// <summary>
/// The default logger that does nothing
/// </summary>
public class GrpcNullLogger : GrpcLogger
{
    /// <summary>
    /// Create the default null logger
    /// </summary>
    /// <param name="level"></param>
    /// <param name="console"></param>
    public GrpcNullLogger(GrpcLoggingLevel level, LocalConsole? console = null) : base(level, console)
    {
    }

    /// <summary>
    /// Log
    /// </summary>
    /// <param name="level"></param>
    /// <param name="format"></param>
    /// <param name="args"></param>
    public override void Log(GrpcLoggingLevel level, string format, params object[] args)
    {
        
    }

    /// <summary>
    /// Log
    /// </summary>
    /// <param name="level"></param>
    /// <param name="message"></param>
    public override void Log(GrpcLoggingLevel level, string message)
    {
        
    }
}

/// <summary>
/// Log Level
/// </summary>
public enum GrpcLoggingLevel
{
    /// <summary>
    /// None
    /// </summary>
    None,
    /// <summary>
    /// Warn
    /// </summary>
    Warn,
    /// <summary>
    /// Verbose
    /// </summary>
    Verbose
}