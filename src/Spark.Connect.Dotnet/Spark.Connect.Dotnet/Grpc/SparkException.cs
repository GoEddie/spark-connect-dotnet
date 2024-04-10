using Grpc.Core;

namespace Spark.Connect.Dotnet.Grpc;

public class SparkExceptionFactory
{
    public static SparkException GetExceptionFromRpcException(RpcException exception)
    {
        var exceptionCodeStopLocation = exception.Status.Detail.IndexOf(']');
        var exceptionCode = exception.Status.Detail.Substring(1, exceptionCodeStopLocation);
        switch (exceptionCode)
        {
            case "MISSING_GROUP_BY":
                return new MissingGroupByException(exception.Status.Detail, exception);
        }

        return new SparkException(exception);
    }
    
    public static SparkException GetExceptionFromRpcException(AggregateException aggException)
    {
        if (aggException.InnerException is RpcException exception)
        {
            var exceptionCodeStopLocation = exception.Status.Detail.IndexOf(']') -1;
            var exceptionCode = exception.Status.Detail.Substring(1, exceptionCodeStopLocation);
            switch (exceptionCode)
            {
                case "MISSING_GROUP_BY":
                    return new MissingGroupByException(exception.Status.Detail, exception);
                case "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE":
                    return new DataTypeMismatchUnexpectedInputType(exception.Status.Detail, exception);
                case "UNRESOLVED_ROUTINE":
                    return new UnresolvedRoutineException(exception.Status.Detail, exception);
                case "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT":
                    return new DataTypeMismatchNonFoldableInput(exception.Status.Detail, exception);
            }
        }
        return new SparkException(aggException);
    }
}

public class SparkException : Exception
{
    private readonly string _overrideMessage = "";

    public SparkException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        if (exception is AggregateException && exception.InnerException is RpcException rpcException)
        {
            _overrideMessage = rpcException.Status.Detail;
        }

        if (exception is RpcException rpcExceptionFromRpc)
        {
            _overrideMessage = rpcExceptionFromRpc.Status.Detail;
        }
    }

    public override string Message
    {
        get => _overrideMessage;
    }

    public SparkException(Exception exception) : base(exception.Message, exception)
    {
        if (exception is AggregateException && exception.InnerException is RpcException rpcException)
        {
            _overrideMessage = rpcException.Status.Detail;
        }
    }
}

public class MissingGroupByException : SparkException{
    public MissingGroupByException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        
    }

    public MissingGroupByException(Exception exception) : base(exception)
    {
        
    }
}

public class DataTypeMismatchUnexpectedInputType : SparkException{
    public DataTypeMismatchUnexpectedInputType(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        
    }

    public DataTypeMismatchUnexpectedInputType(Exception exception) : base(exception)
    {
        
    }
}

public class DataTypeMismatchNonFoldableInput : SparkException{
    //DATATYPE_MISMATCH.NON_FOLDABLE_INPUT
    public DataTypeMismatchNonFoldableInput(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        
    }

    public DataTypeMismatchNonFoldableInput(Exception exception) : base(exception)
    {
        
    }
}

public class UnresolvedRoutineException : SparkException
{
    //"UNRESOLVED_ROUTINE"
    
    public UnresolvedRoutineException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        
    }
}
    
    