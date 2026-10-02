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

    /// <summary>Replaces invalid UTF-8 sequences with the Unicode replacement character.</summary>
    public static Column MakeValidUtf8(Column col) => new(CreateExpression("make_valid_utf8", false, col));
    public static Column MakeValidUtf8(string col) => MakeValidUtf8(Col(col));

    /// <summary>Returns valid UTF-8 input, or raises an error for invalid input.</summary>
    public static Column ValidateUtf8(Column col) => new(CreateExpression("validate_utf8", false, col));
    public static Column ValidateUtf8(string col) => ValidateUtf8(Col(col));

    /// <summary>Returns valid UTF-8 input, or null for invalid input.</summary>
    public static Column TryValidateUtf8(Column col) => new(CreateExpression("try_validate_utf8", false, col));
    public static Column TryValidateUtf8(string col) => TryValidateUtf8(Col(col));

    /// <summary>Concatenates non-null string or binary values. Result order is unspecified.</summary>
    public static Column Listagg(Column col, Column? delimiter = null) =>
        new(delimiter is null ? CreateExpression("listagg", false, col)
            : CreateExpression("listagg", false, col, delimiter));
    public static Column Listagg(string col, Column? delimiter = null) => Listagg(Col(col), delimiter);
    /// <summary>Concatenates values using a literal string delimiter.</summary>
    public static Column Listagg(Column col, string delimiter) => Listagg(col, Lit(delimiter));
    public static Column Listagg(string col, string delimiter) => Listagg(Col(col), Lit(delimiter));
    /// <summary>Concatenates binary values using a literal binary delimiter.</summary>
    public static Column Listagg(Column col, byte[] delimiter) => Listagg(col, Lit(delimiter));
    public static Column Listagg(string col, byte[] delimiter) => Listagg(Col(col), Lit(delimiter));

    /// <summary>Concatenates distinct non-null string or binary values. Result order is unspecified.</summary>
    public static Column ListaggDistinct(Column col, Column? delimiter = null) =>
        new(delimiter is null ? CreateExpression("listagg_distinct", false, col)
            : CreateExpression("listagg_distinct", false, col, delimiter));
    public static Column ListaggDistinct(string col, Column? delimiter = null) => ListaggDistinct(Col(col), delimiter);
    /// <summary>Concatenates values using a literal string delimiter.</summary>
    public static Column ListaggDistinct(Column col, string delimiter) => ListaggDistinct(col, Lit(delimiter));
    public static Column ListaggDistinct(string col, string delimiter) => ListaggDistinct(Col(col), Lit(delimiter));
    /// <summary>Concatenates binary values using a literal binary delimiter.</summary>
    public static Column ListaggDistinct(Column col, byte[] delimiter) => ListaggDistinct(col, Lit(delimiter));
    public static Column ListaggDistinct(string col, byte[] delimiter) => ListaggDistinct(Col(col), Lit(delimiter));

    /// <summary>Concatenates non-null string or binary values. Result order is unspecified.</summary>
    public static Column StringAgg(Column col, Column? delimiter = null) =>
        new(delimiter is null ? CreateExpression("string_agg", false, col)
            : CreateExpression("string_agg", false, col, delimiter));
    public static Column StringAgg(string col, Column? delimiter = null) => StringAgg(Col(col), delimiter);
    /// <summary>Concatenates values using a literal string delimiter.</summary>
    public static Column StringAgg(Column col, string delimiter) => StringAgg(col, Lit(delimiter));
    public static Column StringAgg(string col, string delimiter) => StringAgg(Col(col), Lit(delimiter));
    /// <summary>Concatenates binary values using a literal binary delimiter.</summary>
    public static Column StringAgg(Column col, byte[] delimiter) => StringAgg(col, Lit(delimiter));
    public static Column StringAgg(string col, byte[] delimiter) => StringAgg(Col(col), Lit(delimiter));

    /// <summary>Concatenates distinct non-null string or binary values. Result order is unspecified.</summary>
    public static Column StringAggDistinct(Column col, Column? delimiter = null) =>
        new(delimiter is null ? CreateExpression("string_agg_distinct", false, col)
            : CreateExpression("string_agg_distinct", false, col, delimiter));
    public static Column StringAggDistinct(string col, Column? delimiter = null) => StringAggDistinct(Col(col), delimiter);
    /// <summary>Concatenates values using a literal string delimiter.</summary>
    public static Column StringAggDistinct(Column col, string delimiter) => StringAggDistinct(col, Lit(delimiter));
    public static Column StringAggDistinct(string col, string delimiter) => StringAggDistinct(Col(col), Lit(delimiter));
    /// <summary>Concatenates binary values using a literal binary delimiter.</summary>
    public static Column StringAggDistinct(Column col, byte[] delimiter) => StringAggDistinct(col, Lit(delimiter));
    public static Column StringAggDistinct(string col, byte[] delimiter) => StringAggDistinct(Col(col), Lit(delimiter));
}
