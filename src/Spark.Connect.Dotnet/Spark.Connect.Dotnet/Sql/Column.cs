using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Sql;

public class Column
{
    private readonly string _name;
    public readonly Expression Expression;

    public Column(Expression expression)
    {
        Expression = expression;
    }

    public Column(string name)
    {
        _name = name;
        Expression = new Expression
        {
            UnresolvedAttribute = new Expression.Types.UnresolvedAttribute
            {
                UnparsedIdentifier = name
            }
        };
    }

    private bool Equals(Column other)
    {
        return Expression.Equals(other.Expression);
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        return obj.GetType() == GetType() && Equals((Column)obj);
    }

    public override int GetHashCode()
    {
        return Expression.GetHashCode();
    }

    public Column Alias(string name)
    {
        var expression = new Expression
        {
            Alias = new Expression.Types.Alias
            {
                Expr = Expression, Name = { name }
            }
        };

        return new Column(expression);
    }

    public Column Asc()
    {
        return new Column(new Expression
        {
            SortOrder = new Expression.Types.SortOrder
            {
                Child = Expression,
                Direction = Expression.Types.SortOrder.Types.SortDirection.Ascending
            }
        });
    }

    public Column Desc()
    {
        return new Column(new Expression
        {
            SortOrder = new Expression.Types.SortOrder
            {
                Child = Expression,
                Direction = Expression.Types.SortOrder.Types.SortDirection.Descending
            }
        });
    }

    public static Column operator &(Column src, bool value)
    {
        return src.And(value);
    }

    public Column And(bool value)
    {
        return BinaryOperation(value, "and");
    }
    
    public Column And(Column value)
    {
        return BinaryOperation(value, "and");
    }

    public static Column operator |(Column src, bool value)
    {
        return src.Or(value);
    }

    public Column Or(bool value)
    {
        return BinaryOperation(value, "or");
    }
    
    public Column Or(Column value)
    {
        return BinaryOperation(value, "or");
    }

    public static Column operator !(Column src)
    {
        return src.Not();
    }

    public Column Not()
    {
        return BinaryOperation("not");
    }

    public static Column operator *(Column src, int value)
    {
        return src.Multiply(value);
    }
    
    public static Column operator *(Column src, Column value)
    {
        return src.Multiply(value);
    }

    public Column Multiply(int value)
    {
        return BinaryOperation(value, "*");
    }
    
    public Column Multiply(Column value)
    {
        return BinaryOperation(value, "*");
    }

    public static Column operator ==(Column src, int value)
    {
        return src.EqualTo(value);
    }
    
    public static Column operator ==(Column src, Column value)
    {
        return src.EqualTo(value);
    }

    public Column EqualTo(int value)
    {
        return BinaryOperation(value, "==");
    }
    
    public Column EqualTo(Column value)
    {
        return BinaryOperation(value, "==");
    }

    public static Column operator !=(Column src, int value)
    {
        return src.NotEqualTo(value);
    }
    
    public static Column operator !=(Column src, Column value)
    {
        return src.NotEqualTo(value);
    }

    public Column NotEqualTo(int value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }
    
    public Column NotEqualTo(Column value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    public Column RMultiply(int value)
    {
        return BinaryOperation(value, "*", true);
    }
    
    public Column RMultiply(Column value)
    {
        return BinaryOperation(value, "*", true);
    }

    public static Column operator /(Column src, int value)
    {
        return src.Divide(value);
    }
    
    public static Column operator /(Column src, Column value)
    {
        return src.Divide(value);
    }

    public Column Divide(int value)
    {
        return BinaryOperation(value, "/");
    }
    
    public Column Divide(Column value)
    {
        return BinaryOperation(value, "/");
    }

    public Column RDivide(int value)
    {
        return BinaryOperation(value, "/", true);
    }
    
    public Column RDivide(Column value)
    {
        return BinaryOperation(value, "/", true);
    }

    public static Column operator +(Column src, int value)
    {
        return src.Add(value);
    }
    
    public static Column operator +(Column src, Column value)
    {
        return src.Add(value);
    }

    public Column Add(int value)
    {
        return BinaryOperation(value, "+");
    }
    
    public Column Add(Column value)
    {
        return BinaryOperation(value, "+");
    }

    public Column RAdd(int value)
    {
        return BinaryOperation(value, "+", true);
    }
    
    public Column RAdd(Column value)
    {
        return BinaryOperation(value, "+", true);
    }

    public static Column operator -(Column src, int value)
    {
        return src.Minus(value);
    }
    
    public static Column operator -(Column src, Column value)
    {
        return src.Minus(value);
    }

    public Column Minus(int value)
    {
        return BinaryOperation(value, "-");
    }

    public Column Minus(Column value)
    {
        return BinaryOperation(value, "-");
    }

    public Column RMinus(int value)
    {
        return BinaryOperation(value, "-", true);
    }
    
    public Column RMinus(Column value)
    {
        return BinaryOperation(value, "-", true);
    }

    public static Column operator %(Column src, int value)
    {
        return src.Mod(value);
    }
    
    public static Column operator %(Column src, Column value)
    {
        return src.Mod(value);
    }

    public Column Mod(int value)
    {
        return BinaryOperation(value, "%");
    }
    
    public Column Mod(Column value)
    {
        return BinaryOperation(value, "%");
    }

    public Column RMod(int value)
    {
        return BinaryOperation(value, "%", true);
    }
    
    public Column RMod(Column value)
    {
        return BinaryOperation(value, "%", true);
    }

    public Column Pow(int value)
    {
        return BinaryOperation(value, "power");
    }
    
    public Column Pow(Column value)
    {
        return BinaryOperation(value, "power");
    }

    public Column RPow(int value)
    {
        return BinaryOperation(value, "power", true);
    }
    
    public Column RPow(Column value)
    {
        return BinaryOperation(value, "power", true);
    }
    
    public static Column operator > (Column col, int value)
    {
        return col.Gt(value);
    }
    
    public static Column operator > (Column col, Column value)
    {
        return col.Gt(value);
    }
    public static Column operator < (Column col, int value)
    {
        return col.Lt(value);
    }
    
    public static Column operator < (Column col, Column value)
    {
        return col.Lt(value);
    }
    
    public static Column operator <= (Column col, Column value)
    {
        return col.Le(value);
    }
    
    public static Column operator <= (Column col, int value)
    {
        return col.Le(value);
    }
    
    public static Column operator >= (Column col, Column value)
    {
        return col.Ge(value);
    }
    
    public static Column operator >= (Column col, int value)
    {
        return col.Ge(value);
    }
    
    
    public Column Gt(int value)
    {
        return BinaryOperation(value, ">");
    }
    
    public Column Gt(Column value)
    {
        return BinaryOperation(value, ">");
    }

    public Column Lt(int value)
    {
        return BinaryOperation(value, "<");
    }
    
    public Column Lt(Column value)
    {
        return BinaryOperation(value, "<");
    }

    public Column Ge(int value)
    {
        return BinaryOperation(value, ">=");
    }
    
    public Column Ge(Column value)
    {
        return BinaryOperation(value, ">=");
    }

    public Column Le(int value)
    {
        return BinaryOperation(value, "<=");
    }
    
    public Column Le(Column value)
    {
        return BinaryOperation(value, "<=");
    }

    private Column BinaryOperation(string functionName)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = functionName,
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        return new Column(expression);
    }

    private Column NotOperation(Column equalsOperator)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "not", IsUserDefinedFunction = false, IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(equalsOperator.Expression);

        return new Column(expression);
    }

    private Column BinaryOperation(int value, string functionName, bool reverse = false)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = functionName,
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        var literal = new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Integer = value
            }
        };

        if (reverse)
        {
            expression.UnresolvedFunction.Arguments.Add(literal);
            expression.UnresolvedFunction.Arguments.Add(Expression);
        }
        else
        {
            expression.UnresolvedFunction.Arguments.Add(Expression);
            expression.UnresolvedFunction.Arguments.Add(literal);
        }

        return new Column(expression);
    }
    
    private Column BinaryOperation(Column value, string functionName, bool reverse = false)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = functionName,
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };
        

        if (reverse)
        {
            expression.UnresolvedFunction.Arguments.Add(value.Expression);
            expression.UnresolvedFunction.Arguments.Add(Expression);
        }
        else
        {
            expression.UnresolvedFunction.Arguments.Add(Expression);
            expression.UnresolvedFunction.Arguments.Add(value.Expression);
        }

        return new Column(expression);
    }

    private Column BinaryOperation(bool value, string functionName, bool reverse = false)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = functionName,
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        var literal = new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Boolean = value
            }
        };

        if (reverse)
        {
            expression.UnresolvedFunction.Arguments.Add(literal);
            expression.UnresolvedFunction.Arguments.Add(Expression);
        }
        else
        {
            expression.UnresolvedFunction.Arguments.Add(Expression);
            expression.UnresolvedFunction.Arguments.Add(literal);
        }

        return new Column(expression);
    }

    public Expression Over(WindowSpec window)
    {
        return window.ToExpression(Expression);
    }

    public Column Cast(SparkDataType type)
    {
        return new Column(new Expression
        {
            Cast = new Expression.Types.Cast
            {
                Expr = Expression,
                Type = type.ToDataType()
            }
        });
    }

    public Column Cast(string type)
    {
        return Cast(SparkDataType.FromString(type));
    }

    public Column IsNull()
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "isnull", IsUserDefinedFunction = false, IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        return new Column(expression);
    }

    public Column EndsWith(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "endswith", IsUserDefinedFunction = false, IsDistinct = false
            }
        };
        
        expression.UnresolvedFunction.Arguments.Add(Expression);
        
        expression.UnresolvedFunction.Arguments.Add(new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                String = other
            }
        });

        return new Column(expression);
    }
    
    public Column StartsWith(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "startswith", IsUserDefinedFunction = false, IsDistinct = false
            }
        };
        
        expression.UnresolvedFunction.Arguments.Add(Expression);
        
        expression.UnresolvedFunction.Arguments.Add(new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                String = other
            }
        });

        return new Column(expression);
    }
    
    public Column Between(Column lowerBound, Column upperBound)
    {
        return (this >= lowerBound).And(this <= upperBound);
    }

    /// <summary>
    ///     Otherwise
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public Column Otherwise(Column value)
    {
        //Otherwise is a special case, we need to retrieve the previous expression and it must be a "when" and then we add ourselves at the
        // end of the arguments AND otherwise must not have already been called
        if (Expression.UnresolvedFunction == null)
        {
            throw new InvalidOperationException(
                "Otherwise() can only be applied on a Column previously generated by When()");
        }

        if (Expression.UnresolvedFunction.FunctionName != "when")
        {
            throw new InvalidOperationException(
                $"Otherwise() can only be applied on a Column previously generated by When(), it looks like the previous function was '{FunctionsWrapper.CSharpFunctionName(Expression.UnresolvedFunction.FunctionName)}'");
        }

        if (Expression.UnresolvedFunction.Arguments.Count > 2)
        {
            throw new InvalidOperationException(
                "Otherwise() can only be applied on a Column previously generated by When(), has Otherwise() already been called on the column?");
        }

        Expression.UnresolvedFunction.Arguments.Add(value.Expression);
        return this;
    }

    public Column DropFields(params string[] fieldNames)
    {
        var lastExpression = Expression;
        
        foreach (var field in fieldNames)
        {
            var expression = new Expression()
            {
                UpdateFields = new Expression.Types.UpdateFields()
                {
                    FieldName = field,
                    StructExpression = lastExpression
                }
            };

            lastExpression = expression;
        }

        return new Column(lastExpression);
    }

    public Column GetField(string name)
    {
        var expression = new Expression()
        {
            ExpressionString = new Expression.Types.ExpressionString()
            {
                Expression = $"{_name}.{name}"
            }
        };

        return new Column(expression);
    }
    
    public Column Like(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "like", IsUserDefinedFunction = false, IsDistinct = false
            }
        };
        
        expression.UnresolvedFunction.Arguments.Add(Expression);
        
        expression.UnresolvedFunction.Arguments.Add(new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                String = other
            }
        });

        return new Column(expression);
    }
    
    public Column ILike(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "ilike", IsUserDefinedFunction = false, IsDistinct = false
            }
        };
        
        expression.UnresolvedFunction.Arguments.Add(Expression);
        
        expression.UnresolvedFunction.Arguments.Add(new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                String = other
            }
        });

        return new Column(expression);
    }
    
    public Column RLike(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "rlike", IsUserDefinedFunction = false, IsDistinct = false
            }
        };
        
        expression.UnresolvedFunction.Arguments.Add(Expression);
        
        expression.UnresolvedFunction.Arguments.Add(new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                String = other
            }
        });

        return new Column(expression);
    }
    
    public Column Substr(int startPos, int length)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "substr", IsUserDefinedFunction = false, IsDistinct = false
            }
        };
        
        expression.UnresolvedFunction.Arguments.Add(Expression);
        
        expression.UnresolvedFunction.Arguments.Add(new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                Integer = startPos
            }
        });

        expression.UnresolvedFunction.Arguments.Add(new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                Integer = length
            }
        });

        return new Column(expression);
    }
}