namespace Spark.Connect.Dotnet.Sql;

public class Functions
{
    public static Expression Lit(string value)
    {
        return new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                String = value
            }
        };
    }
    
    public static Expression Lit(bool value)
    {
        return new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                Boolean = value
            }
        };
    }
    
    public static Expression Lit(double value)
    {
        return new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                Double = value
            }
        };
    }

    public static SparkColumn Min(string name)
    {
        return new SparkColumn(FunctionCall("min", name));
    }
    
    public static SparkColumn Max(string name)
    {
        return new SparkColumn(FunctionCall("min", name));
    }

    protected internal static Expression FunctionCall(string function, string param1)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = true,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    new Expression()
                    {
                        UnresolvedAttribute = new Expression.Types.UnresolvedAttribute()
                        {
                            UnparsedIdentifier = param1
                        }
                    }
                }
            }
        };
    }
    
    public Expression Alias(string name)
    {
        throw new NotImplementedException();
    }

    public static SparkColumn Col(string name) => Column(name);

    public static SparkColumn Column(string name) => new (name);
    
    
}