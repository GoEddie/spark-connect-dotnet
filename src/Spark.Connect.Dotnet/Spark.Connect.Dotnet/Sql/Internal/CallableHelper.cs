using System.Linq.Expressions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Sql;

public class CallableHelper
{
    private static int parameterNumber = 0; //session scopped??? does it matter??
    
    public Expression GetLambdaExpression(Expression<Func<Column, Column>> lambda)
    {
        var x = new Expression.Types.UnresolvedNamedLambdaVariable()
        {
            NameParts =
            {
                FreshLambdaName(lambda.Parameters[0].Name)
            }
        };
        
        var method = lambda.Compile();
        var result = method(new Column(new Expression{ UnresolvedNamedLambdaVariable = x}));

        var lambdaExpression = new Expression.Types.LambdaFunction()
        {
            Function = result.Expression,
            Arguments = { x }
        };

        return new Expression()
        {
            LambdaFunction = lambdaExpression
        };
    }

    public Expression GetLambdaExpression(Expression<Func<Column, Column, Column>> lambda)
    {
        var x = new Expression.Types.UnresolvedNamedLambdaVariable()
        {
            NameParts =
            {
                FreshLambdaName(lambda.Parameters[0].Name)
            }
        };

        var y = new Expression.Types.UnresolvedNamedLambdaVariable()
        {
            NameParts =
            {
                FreshLambdaName(lambda.Parameters[1].Name)
            }
        };

        var method = lambda.Compile();
        var result = method(new Column(new Expression{ UnresolvedNamedLambdaVariable = x}), new Column(new Expression{ UnresolvedNamedLambdaVariable = y}));

        var lambdaExpression = new Expression.Types.LambdaFunction()
        {
            Function = result.Expression,
            Arguments = { x, y }
        };

        return new Expression()
        {
            LambdaFunction = lambdaExpression
        };
    }
    
    public Expression GetLambdaExpression(Expression<Func<Column, Column, Column, Column>> lambda)
    {
        var k = new Expression.Types.UnresolvedNamedLambdaVariable()
        {
            NameParts =
            {
                FreshLambdaName(lambda.Parameters[0].Name)
            }
        };

        var v1 = new Expression.Types.UnresolvedNamedLambdaVariable()
        {
            NameParts =
            {
                FreshLambdaName(lambda.Parameters[1].Name)
            }
        };
        
        var v2 = new Expression.Types.UnresolvedNamedLambdaVariable()
        {
            NameParts =
            {
                FreshLambdaName(lambda.Parameters[2].Name)
            }
        };

        var method = lambda.Compile();
        var result = method(new Column(new Expression{ UnresolvedNamedLambdaVariable = k}), new Column(new Expression{ UnresolvedNamedLambdaVariable = v1}), new Column(new Expression{ UnresolvedNamedLambdaVariable = v2}));

        var lambdaExpression = new Expression.Types.LambdaFunction()
        {
            Function = result.Expression,
            Arguments = { k, v1, v2}
        };

        return new Expression()
        {
            LambdaFunction = lambdaExpression
        };
    }
    
    private string FreshLambdaName(string? name)
    {
        return $"{name}_{parameterNumber++}";
    }

    public Plan GetPlan(Expression<Func<Column, Column, Column, Column>> lambda)
    {
        throw new NotImplementedException();
    }

    public Expression GetLambdaExpression(Expression<Func<object>> lambda)
    {
        var method = lambda.Compile();
        var result = Lit(method());

        var lambdaExpression = new Expression.Types.LambdaFunction()
        {
            Function = result.Expression,
            Arguments = {  }
        };

        return new Expression()
        {
            LambdaFunction = lambdaExpression
        };
    }
}