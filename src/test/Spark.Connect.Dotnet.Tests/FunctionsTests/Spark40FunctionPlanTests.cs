using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.FunctionsTests;

public class Spark40FunctionPlanTests
{
    [Fact]
    public void NullHelpersPreserveColumnReferences()
    {
        foreach (var (name, column) in new[]
                 { ("nullifzero", Nullifzero("value")), ("zeroifnull", Zeroifnull("value")) })
        {
            var function = column.Expression.UnresolvedFunction;
            Assert.Equal(name, function.FunctionName);
            Assert.Equal("value", Assert.Single(function.Arguments).UnresolvedAttribute.UnparsedIdentifier);
        }
        Assert.Equal(Nullifzero(Col("value")).Expression, Nullifzero("value").Expression);
        Assert.Equal(NullIfZero(Col("value")).Expression, Nullifzero(Col("value")).Expression);
        Assert.Equal(NullIfZero("value").Expression, Nullifzero("value").Expression);
        Assert.Equal(Zeroifnull(Col("value")).Expression, Zeroifnull("value").Expression);
    }

    [Fact]
    public void Utf8ValidationPreservesBinaryLiteralAndColumnReference()
    {
        var function = IsValidUtf8(Lit(new byte[] { 0xff })).Expression.UnresolvedFunction;
        Assert.Equal("is_valid_utf8", function.FunctionName);
        Assert.Equal(new byte[] { 0xff }, Assert.Single(function.Arguments).Literal.Binary.ToByteArray());
        Assert.Equal("value", Assert.Single(IsValidUtf8("value").Expression.UnresolvedFunction.Arguments)
            .UnresolvedAttribute.UnparsedIdentifier);
        Assert.Equal(IsValidUtf8(Col("value")).Expression, IsValidUtf8("value").Expression);
    }
}
