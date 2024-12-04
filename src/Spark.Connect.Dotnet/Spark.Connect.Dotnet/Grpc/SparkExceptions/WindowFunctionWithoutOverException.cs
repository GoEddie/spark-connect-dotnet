using System.Text.RegularExpressions;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Wrap WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE
/// </summary>
public class WindowFunctionWithoutOverException : SparkException
{
    //WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE

    /// <summary>
    /// Wrap WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
    public WindowFunctionWithoutOverException(string exceptionMessage, Exception exception) : base(exceptionMessage,
        exception)
    {
        //Format = [WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE] Window function "(.*?)" requires an OVER clause.
        var input = exceptionMessage;
        // Extract the column name
        var windowFunctionPattern = @"Window function ""(.*?)""";
        var windowFunction = Regex.Match(input, windowFunctionPattern).Value;

        OverrideMessage =
            $@"The Window Function `{FunctionsWrapper.CSharpFunctionName(windowFunction)}` requires an OVER clause";
    }

    /// <summary>
    /// Wrap WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE
    /// </summary>
    /// <param name="exception"></param>
    public WindowFunctionWithoutOverException(Exception exception) : base(exception)
    {
    }
}