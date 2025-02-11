using System.Text.RegularExpressions;

namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Wrap UNRESOLVED_COLUMN.WITH_SUGGESTION
/// </summary>
public class UnresolvedColumnWithSuggestionException : SparkException
{
    //UNRESOLVED_COLUMN.WITH_SUGGESTION

    /// <summary>
    /// Wrap UNRESOLVED_COLUMN.WITH_SUGGESTION
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
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

    /// <summary>
    /// Wrap UNRESOLVED_COLUMN.WITH_SUGGESTION
    /// </summary>
    /// <param name="exception"></param>
    public UnresolvedColumnWithSuggestionException(Exception exception) : base(exception)
    {
    }
}