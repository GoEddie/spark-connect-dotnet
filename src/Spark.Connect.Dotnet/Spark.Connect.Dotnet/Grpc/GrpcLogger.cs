namespace Spark.Connect.Dotnet.Grpc;

public class GrpcLogger
{
    private readonly GrpcLoggingLevel _level;
    private readonly LocalConsole _console;

    public GrpcLogger(GrpcLoggingLevel level, LocalConsole console = null)
    {
        _level = level;
        _console = console;
    }
    

    public virtual void Log(GrpcLoggingLevel level, string message)
    {
        if (level >= _level)
        {
            _console.WriteLine(DateTime.Now + " :: " + message);
        }
    }
    
    public virtual void Log(GrpcLoggingLevel level, string format, params object[] args)
    {
        if (level >= _level)
        {
            _console.WriteLine( DateTime.Now + " :: " + string.Format(format, args));
        }
    }
}

public class GrpcNullLogger : GrpcLogger
{
    public GrpcNullLogger(GrpcLoggingLevel level, LocalConsole console = null) : base(level, console)
    {
    }

    public override void Log(GrpcLoggingLevel level, string format, params object[] args)
    {
        
    }

    public override void Log(GrpcLoggingLevel level, string message)
    {
        
    }
}

public enum GrpcLoggingLevel
{
    None,
    Warn,
    Verbose
}