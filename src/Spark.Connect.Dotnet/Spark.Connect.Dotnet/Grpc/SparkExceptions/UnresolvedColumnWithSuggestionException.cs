using System.Text.RegularExpressions;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Grpc;

public class UnresolvedColumnWithSuggestionException : SparkException
{
    //UNRESOLVED_COLUMN.WITH_SUGGESTION

    public UnresolvedColumnWithSuggestionException(string exceptionMessage, Exception exception) : base(
        exceptionMessage, exception)
    {
        //Format = "UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `idarray` cannot be resolved. Did you mean one of the following? [`collect_list(idarray)`, `col2`]"
        var input = exceptionMessage;
        // Extract the column name
        var columnNamePattern = @"with name `(.*?)`";
        var columnNameMatch = Regex.Match(input, columnNamePattern);
        var columnName = columnNameMatch.Groups[1].Value;

        // Extract the list of existing columns
        var existingColumnsPattern = @"following\?.\[(.*)?\]";
        var existingColumnsMatches = Regex.Match(input, existingColumnsPattern);

        var existingColumns = existingColumnsMatches.Groups[1].Value;
        OverrideMessage = $@"The Column `{columnName}` was not found, did you mean one of: {existingColumns}?";
    }

    public UnresolvedColumnWithSuggestionException(Exception exception) : base(exception)
    {
    }
}

public class WindowFunctionWithoutOverException : SparkException
{
    //WINDOW_FUNCTION_WITHOUT_OVER_CLAUSE

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

    public WindowFunctionWithoutOverException(Exception exception) : base(exception)
    {
    }
}