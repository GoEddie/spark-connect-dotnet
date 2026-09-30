using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests;

[Trait("Category", "Smoke")]
public class ColumnIsInTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Column_Arguments_Are_Not_Also_Converted_To_Literals(bool useListOverload)
    {
        var source = Col("value");
        var candidate = Col("candidate");

        var result = useListOverload
            ? source.IsIn(new List<object> { candidate })
            : source.IsIn(candidate);

        var function = result.Expression.UnresolvedFunction;
        Assert.Equal("in", function.FunctionName);
        Assert.Collection(function.Arguments,
            argument => Assert.Equal(source.Expression, argument),
            argument => Assert.Equal(candidate.Expression, argument));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Mixed_Arguments_Preserve_Their_Types_And_Order(bool useListOverload)
    {
        var source = Col("value");
        var candidate = Col("candidate");

        var result = useListOverload
            ? source.IsIn(new List<object> { "first", candidate, 42 })
            : source.IsIn("first", candidate, 42);

        Assert.Collection(result.Expression.UnresolvedFunction.Arguments,
            argument => Assert.Equal(source.Expression, argument),
            argument => Assert.Equal(Lit("first").Expression, argument),
            argument => Assert.Equal(candidate.Expression, argument),
            argument => Assert.Equal(Lit(42).Expression, argument));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Literal_Arguments_Remain_Literals(bool useListOverload)
    {
        var source = Col("value");

        var result = useListOverload
            ? source.IsIn(new List<object> { "Bob", "Mike" })
            : source.IsIn("Bob", "Mike");

        Assert.Collection(result.Expression.UnresolvedFunction.Arguments,
            argument => Assert.Equal(source.Expression, argument),
            argument => Assert.Equal(Lit("Bob").Expression, argument),
            argument => Assert.Equal(Lit("Mike").Expression, argument));
    }
}
