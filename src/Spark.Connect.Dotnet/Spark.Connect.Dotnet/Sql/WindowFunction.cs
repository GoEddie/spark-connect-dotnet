namespace Spark.Connect.Dotnet.Sql;

/// <summary>
///     This function name clashes with the Window Spec so including it as a separate class if you want to use it
/// </summary>
public class WindowFunction : Functions
{
    /// <summary>
    ///     Example:
    ///     ```csharp
    ///     var df = Spark.Sql("SELECT current_timestamp as date, 1 as val");
    ///     var w = df.GroupBy(WindowFunction.Window("date", "5 seconds")).Agg(Sum("val").Alias("sum"));
    ///     w.Select(
    ///     w["Window"]["start"].Cast("string").Alias("start"),
    ///     w["Window"]["end"].Cast("string").Alias("end"),
    ///     Col("sum")
    ///     ).Show();
    ///     ```
    ///     Note the WindowFunction.Window to get to the function and then the special window.start column, the schema looks
    ///     like:
    ///     ```
    ///     root
    ///     |-- window: struct (nullable = false)
    ///     |    |-- start: timestamp (nullable = true)
    ///     |    |-- end: timestamp (nullable = true)
    ///     |-- sum: long (nullable = true)
    ///     ```
    /// </summary>
    /// <param name="timeColumn"></param>
    /// <param name="windowDuration"></param>
    /// <param name="slideDuration"></param>
    /// <param name="startTime"></param>
    /// <returns>Column of struct with two timestamps</returns>
    public static Column Window(Column timeColumn, string windowDuration, string? slideDuration = null,
        string? startTime = null)
    {
        if (!string.IsNullOrEmpty(slideDuration) && !string.IsNullOrEmpty(startTime))
        {
            return new Column(FunctionWrappedCall("window", false, timeColumn, Lit(windowDuration), Lit(slideDuration),
                Lit(startTime)));
        }

        if (!string.IsNullOrEmpty(slideDuration))
        {
            return new Column(FunctionWrappedCall("window", false, timeColumn, Lit(windowDuration),
                Lit(slideDuration)));
        }

        if (!string.IsNullOrEmpty(startTime))
        {
            return new Column(FunctionWrappedCall("window", false, timeColumn, Lit(windowDuration), Lit(startTime)));
        }

        return new Column(FunctionWrappedCall("window", false, timeColumn, Lit(windowDuration)));
    }

    public static Column Window(string timeColumn, string windowDuration, string? slideDuration = null,
        string? startTime = null)
    {
        return Window(Col(timeColumn), windowDuration, slideDuration, startTime);
    }
}