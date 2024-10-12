namespace Spark.Connect.Dotnet.Sql;

public class FunctionsInternal
{
    protected internal static Expression FunctionCall(string function, Column param1, bool isDistinct = false)
    {
        return new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = function, IsDistinct = isDistinct, IsUserDefinedFunction = false, Arguments =
                {
                    new List<Expression> { param1.Expression }
                }
            }
        };
    }
}