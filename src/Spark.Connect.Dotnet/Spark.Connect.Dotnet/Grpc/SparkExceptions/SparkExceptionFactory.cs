using Grpc.Core;

namespace Spark.Connect.Dotnet.Grpc.SparkExceptions;

/// <summary>
/// Used to help map and create exceptions
/// </summary>
public static class SparkExceptionFactory
{
    private static SparkException DetailStringToException(string exceptionCode, string detail, Exception exception)
    {
        return exceptionCode switch
        {
            "MISSING_GROUP_BY" => new MissingGroupByException(detail, exception), "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" => new DataTypeMismatchUnexpectedInputTypeException(detail,
                exception)
            , "UNRESOLVED_ROUTINE" => new UnresolvedRoutineException(detail, exception)
            , "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT" => new DataTypeMismatchNonFoldableInputException(detail, exception)
            , "GROUP_BY_AGGREGATE" => new GroupByAggregateException(detail, exception)
            , "UNRESOLVED_COLUMN.WITH_SUGGESTION" => new UnresolvedColumnWithSuggestionException(detail, exception)
            , "WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE" => new WindowFunctionWithoutOverException(detail, exception), "Internal" => new InternalSparkException(detail, exception)
            , "Unavailable" => new UnavailableException(detail, exception), _ => new SparkException(exception)
        };
    }

    private static string ExceptionCodeFromSparkError(string detail, string statusCode)
    {
        if (detail.IndexOf(']') < 0)
        {
            return statusCode;
        }

        var exceptionCodeStopLocation = detail.IndexOf(']') - 1;
        var exceptionCode = detail.Substring(1, exceptionCodeStopLocation);

        return exceptionCode;
    }

    /// <summary>
    /// Create the appropriate exception
    /// </summary>
    /// <param name="exception"></param>
    /// <returns></returns>
    public static SparkException GetExceptionFromRpcException(RpcException exception)
    {
        var exceptionCode =
            ExceptionCodeFromSparkError(exception.Status.Detail, exception.Status.StatusCode.ToString());
        return DetailStringToException(exceptionCode, exception.Status.Detail, exception);
    }

    /// <summary>
    /// Create the appropriate exception
    /// </summary>
    /// <param name="aggException"></param>
    /// <returns></returns>
    public static SparkException GetExceptionFromRpcException(AggregateException aggException)
    {
        if (aggException.InnerException is RpcException exception)
        {
            return GetExceptionFromRpcException(exception);
        }

        return new SparkException(aggException);
    }
}