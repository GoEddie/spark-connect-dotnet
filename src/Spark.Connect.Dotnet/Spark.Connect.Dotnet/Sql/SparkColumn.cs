namespace Spark.Connect.Dotnet.Sql;

public class SparkColumn
{
    private bool Equals(SparkColumn other)
    {
        return Expression.Equals(other.Expression);
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        return obj.GetType() == this.GetType() && Equals((SparkColumn)obj);
    }

    public override int GetHashCode()
    {
        return Expression.GetHashCode();
    }

    protected internal readonly Expression Expression;

    public SparkColumn(Expression expression)
    {
        Expression = expression;
    }

    public SparkColumn(string name)
    {
        Expression = new Expression()
        {   //TODO - is it always an Unresolvedattribute?
            UnresolvedAttribute = new Expression.Types.UnresolvedAttribute()
            {
                UnparsedIdentifier = name
            }
        };
    }

    public SparkColumn Alias(string name)
    {
        var expression = new Expression()
        {
            Alias = new Expression.Types.Alias()
            {
                Expr = Expression, Name = { name }
            }
        };

        return new SparkColumn(expression);
    }

    public static SparkColumn operator &(SparkColumn src, bool value) => src.And(value);
    
    public SparkColumn And(bool value)
    {
        return BinaryOperation(value, "and");
    }
    
    public static SparkColumn operator |(SparkColumn src, bool value) => src.Or(value);
    
    public SparkColumn Or(bool value)
    {
        return BinaryOperation(value, "or");
    }
    
    public static SparkColumn operator !(SparkColumn src) => src.Not();
    
    public SparkColumn Not()
    {
        return BinaryOperation("not");
    }
    
    public static SparkColumn operator *(SparkColumn src, int value) => src.Multiply(value);
    
    public SparkColumn Multiply(int value)
    {
        return BinaryOperation(value, "*");
    }
    
    public static SparkColumn operator ==(SparkColumn src, int value) => src.EqualTo(value);
    
    public SparkColumn EqualTo(int value)
    {
        return BinaryOperation(value, "==");
    }
    
    public static SparkColumn operator !=(SparkColumn src, int value) => src.NotEqualTo(value);
    
    public SparkColumn NotEqualTo(int value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }
    
    public SparkColumn RMultiply(int value)
    {
        return BinaryOperation(value, "*", true);
    }
    
    public static SparkColumn operator / (SparkColumn src, int value) => src.Divide(value);
    
    public SparkColumn Divide(int value)
    {
        return BinaryOperation(value, "/");
    }
    
    public SparkColumn RDivide(int value)
    {
        return BinaryOperation(value, "/", true);
    }
    
    public static SparkColumn operator + (SparkColumn src, int value) => src.Add(value);
    
    public SparkColumn Add(int value)
    {
        return BinaryOperation(value, "+");
    }
    
    public SparkColumn RAdd(int value)
    {
        return BinaryOperation(value, "+", true);
    }
    
    public static SparkColumn operator - (SparkColumn src, int value) => src.Minus(value);
    
    public SparkColumn Minus(int value)
    {
        return BinaryOperation(value, "-");
    }
    
    public SparkColumn RMinus(int value)
    {
        return BinaryOperation(value, "-", true);
    }
    
    public static SparkColumn operator % (SparkColumn src, int value) => src.Mod(value);
    
    public SparkColumn Mod(int value)
    {
        return BinaryOperation(value, "%");
    }
    
    public SparkColumn RMod(int value)
    {
        return BinaryOperation(value, "%", true);
    }
    
    public SparkColumn Pow(int value)
    {
        return BinaryOperation(value, "power");
    }
    
    public SparkColumn RPow(int value)
    {
        return BinaryOperation(value, "power", true);
    }
    
    public SparkColumn Gt(int value)
    {
        return BinaryOperation(value, ">");
    }
    
    public SparkColumn Lt(int value)
    {
        return BinaryOperation(value, "<");
    }
    
    public SparkColumn Ge(int value)
    {
        return BinaryOperation(value, ">=");
    }
    
    public SparkColumn Le(int value)
    {
        return BinaryOperation(value, "<=");
    }

    private SparkColumn BinaryOperation(string functionName)
    {
        var expression = new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = functionName,
                IsUserDefinedFunction = false,
                IsDistinct = false,
            }
        };

        expression.UnresolvedFunction.Arguments.Add(this.Expression);
        
        return new SparkColumn(expression);
    }

    private SparkColumn NotOperation(SparkColumn equalsOperator)
    {
        var expression = new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = "not", IsUserDefinedFunction = false, IsDistinct = false
            }
        };
        
        expression.UnresolvedFunction.Arguments.Add(equalsOperator.Expression);
        
        return new SparkColumn(expression);
    }
    
    private SparkColumn BinaryOperation(int value, string functionName, bool reverse = false)
    {
        var expression = new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = functionName,
                IsUserDefinedFunction = false,
                IsDistinct = false,
            }
        };

        var literal = new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                Integer = value
            }
        };
        
        if (reverse)
        {
            expression.UnresolvedFunction.Arguments.Add(literal);
            expression.UnresolvedFunction.Arguments.Add(this.Expression);
        }
        else
        {
            expression.UnresolvedFunction.Arguments.Add(this.Expression);   
            expression.UnresolvedFunction.Arguments.Add(literal);    
        }
        
        return new SparkColumn(expression);
    }
    
    private SparkColumn BinaryOperation(bool value, string functionName, bool reverse = false)
    {
        var expression = new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = functionName,
                IsUserDefinedFunction = false,
                IsDistinct = false,
            }
        };

        var literal = new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                Boolean = value
            }
        };
        
        if (reverse)
        {
            expression.UnresolvedFunction.Arguments.Add(literal);
            expression.UnresolvedFunction.Arguments.Add(this.Expression);
        }
        else
        {
            expression.UnresolvedFunction.Arguments.Add(this.Expression);   
            expression.UnresolvedFunction.Arguments.Add(literal);    
        }
        
        return new SparkColumn(expression);
    }

    public Expression Over(Window window)
    {
        return window.ToExpression(this.Expression);
    }
}