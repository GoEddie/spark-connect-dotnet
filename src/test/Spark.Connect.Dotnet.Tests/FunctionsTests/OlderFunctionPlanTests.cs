using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.FunctionsTests;

public class OlderFunctionPlanTests
{
    [Fact]
    public void SumDistinctUsesDistinctSum()
    {
        var function = SumDistinct("value").Expression.UnresolvedFunction;
        Assert.Equal("sum", function.FunctionName);
        Assert.True(function.IsDistinct);
        Assert.Equal("value", Assert.Single(function.Arguments).UnresolvedAttribute.UnparsedIdentifier);
        Assert.Equal(SumDistinct(Col("value")).Expression, SumDistinct("value").Expression);
    }

    [Fact]
    public void IlikePreservesPatternAndEscapeExpressions()
    {
        var function = Ilike(Col("value"), Lit("%HELLO%"), Lit("/")).Expression.UnresolvedFunction;
        Assert.Equal("ilike", function.FunctionName);
        Assert.Equal(3, function.Arguments.Count);
        Assert.Equal("%HELLO%", function.Arguments[1].Literal.String);
        Assert.Equal("/", function.Arguments[2].Literal.String);
        var named = Ilike("value", "pattern").Expression.UnresolvedFunction;
        Assert.Equal(2, named.Arguments.Count);
        Assert.Equal("pattern", named.Arguments[1].UnresolvedAttribute.UnparsedIdentifier);
    }

    [Fact]
    public void RegexpInstrHandlesOptionalIndexAndColumnNames()
    {
        var function = RegexpInstr(Col("value"), Lit("[0-9]+"), 0).Expression.UnresolvedFunction;
        Assert.Equal("regexp_instr", function.FunctionName);
        Assert.Equal(3, function.Arguments.Count);
        Assert.Equal("[0-9]+", function.Arguments[1].Literal.String);
        Assert.Equal(0, function.Arguments[2].Literal.Integer);
        var named = RegexpInstr("value", "pattern").Expression.UnresolvedFunction;
        Assert.Equal(2, named.Arguments.Count);
        Assert.Equal("pattern", named.Arguments[1].UnresolvedAttribute.UnparsedIdentifier);
    }

    [Theory]
    [InlineData(null, null, 2)]
    [InlineData("5 seconds", null, 3)]
    [InlineData(null, "2 seconds", 4)]
    [InlineData("5 seconds", "2 seconds", 4)]
    public void WindowOffsetOccupiesFourthArgument(string? slide, string? start, int count)
    {
        var function = WindowFunction.Window("timestamp", "10 seconds", slide, start).Expression.UnresolvedFunction;
        Assert.Equal("window", function.FunctionName);
        Assert.Equal(count, function.Arguments.Count);
        if (count >= 3) Assert.Equal(slide ?? "10 seconds", function.Arguments[2].Literal.String);
        if (count == 4) Assert.Equal(start, function.Arguments[3].Literal.String);
    }
}
