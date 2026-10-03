using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.FunctionsTests;

public class StringAggregationPlanTests
{
    internal static Func<Column, Column?, Column> Aggregator(string name) => name switch
    {
        "listagg" => Listagg,
        "listagg_distinct" => ListaggDistinct,
        "string_agg" => StringAgg,
        "string_agg_distinct" => StringAggDistinct,
        _ => throw new ArgumentOutOfRangeException(nameof(name))
    };

    [Theory]
    [InlineData("listagg", "Listagg")]
    [InlineData("listagg_distinct", "ListaggDistinct")]
    [InlineData("string_agg", "StringAgg")]
    [InlineData("string_agg_distinct", "StringAggDistinct")]
    public void DelimitersAreLiteralsAndOmittedWhenUnspecified(string sqlName, string methodName)
    {
        var aggregate = Aggregator(sqlName);
        var function = aggregate(Col("value"), null).Expression.UnresolvedFunction;
        var distinct = sqlName.EndsWith("_distinct", StringComparison.Ordinal);
        Assert.Equal(distinct ? sqlName[..^"_distinct".Length] : sqlName, function.FunctionName);
        Assert.Equal(distinct, function.IsDistinct);
        Assert.Equal("value", Assert.Single(function.Arguments).UnresolvedAttribute.UnparsedIdentifier);

        // Exercise every public overload, including string column names and binary delimiters.
        foreach (var input in new object[] { Col("value"), "value" })
        {
            foreach (var delimiter in new object[] { "|", new byte[] { 0x7c }, Lit("|") })
            {
                var method = typeof(Functions).GetMethod(methodName, new[] { input.GetType(), delimiter.GetType() })!;
                var column = (Column)method.Invoke(null, new[] { input, delimiter })!;
                var expected = delimiter is byte[] bytes ? Lit(bytes) : Lit("|");
                Assert.Equal(function.FunctionName, column.Expression.UnresolvedFunction.FunctionName);
                Assert.Equal(distinct, column.Expression.UnresolvedFunction.IsDistinct);
                Assert.Equal(aggregate(Col("value"), expected).Expression, column.Expression);
            }
        }
        var namedDefault = typeof(Functions).GetMethod(methodName, new[] { typeof(string), typeof(Column) })!;
        Assert.Equal(aggregate(Col("value"), null).Expression,
            ((Column)namedDefault.Invoke(null, new object?[] { "value", null })!).Expression);
    }
}

public class StringAggregationResultsTests(ITestOutputHelper logger) : E2ETestBase(logger)
{
    [Theory]
    [InlineData("listagg", false)]
    [InlineData("listagg_distinct", true)]
    [InlineData("string_agg", false)]
    [InlineData("string_agg_distinct", true)]
    public void AggregatesValuesAndHandlesNullsAndEmptyInput(string name, bool distinct)
    {
        var aggregate = StringAggregationPlanTests.Aggregator(name);
        var source = Spark.Sql("SELECT * FROM VALUES ('b'), ('a'), ('a'), (NULL) AS t(value)");
        var row = source.Select(aggregate(Col("value"), Lit("|")),
            aggregate(Col("value"), null), aggregate(Col("value"), Lit(""))).Collect()[0];
        // Spark does not guarantee aggregate ordering, so compare sorted values.
        Assert.Equal(distinct ? new[] { "a", "b" } : new[] { "a", "a", "b" },
            ((string)row[0]).Split('|').OrderBy(value => value).ToArray());
        var expected = distinct ? "ab" : "aab";
        Assert.Equal(expected, string.Concat(((string)row[1]).OrderBy(value => value)));
        Assert.Equal(expected, string.Concat(((string)row[2]).OrderBy(value => value)));
        Assert.Null(source.Filter("value IS NULL").Select(aggregate(Col("value"), null)).Collect()[0][0]);
        Assert.Null(source.Limit(0).Select(aggregate(Col("value"), null)).Collect()[0][0]);

        var binary = Spark.Sql("SELECT * FROM VALUES (unhex('61')), (unhex('61')), (CAST(NULL AS BINARY)) AS t(value)");
        var bytes = binary.Select(aggregate(Col("value"), Lit(new byte[] { 0x7c }))).Collect()[0][0];
        Assert.Equal(distinct ? new byte[] { 0x61 } : new byte[] { 0x61, 0x7c, 0x61 }, (byte[])bytes);
    }
}
