namespace Spark.Connect.Dotnet.Sql;

public partial class Functions
{
    /// <summary>Returns null when the input is zero.</summary>
    public static Column NullIfZero(Column col) => new(CreateExpression("nullifzero", false, col));
    public static Column NullIfZero(string col) => NullIfZero(Col(col));

    // Nullifzero follows Spark's spelling but looks awful in C#.
    // Keep both spellings so callers can choose the more readable NullIfZero.
    /// <summary>Alias for NullIfZero using Spark's spelling.</summary>
    public static Column Nullifzero(Column col) => NullIfZero(col);
    /// <summary>Alias for NullIfZero using Spark's spelling.</summary>
    public static Column Nullifzero(string col) => NullIfZero(col);

    /// <summary>Returns zero when the input is null.</summary>
    public static Column Zeroifnull(Column col) => new(CreateExpression("zeroifnull", false, col));
    public static Column Zeroifnull(string col) => Zeroifnull(Col(col));

    /// <summary>Returns whether the input is valid UTF-8.</summary>
    public static Column IsValidUtf8(Column col) => new(CreateExpression("is_valid_utf8", false, col));
    public static Column IsValidUtf8(string col) => IsValidUtf8(Col(col));
}
