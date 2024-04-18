using System.Text;
using Apache.Arrow;
using Google.Protobuf.Collections;

namespace Spark.Connect.Dotnet.Sql;

public class FunctionsWrapper : FunctionsInternal
{
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    
                }
            }
        };
    }
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, params Expression[] parameters)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    parameters.ToList()
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Column col, params Expression[] parameters)
    {
        var args = new RepeatedField<Expression>();
        args.Add(col.Expression);
        args.AddRange(parameters);
        
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    args
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, params string[] parameters)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    parameters.Select(p => new Column(p).Expression).ToList()
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, string parameter1, Expression parameter2)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                   new Column(parameter1).Expression, parameter2
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Expression parameter1, string parameter2)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    parameter1, new Column(parameter2).Expression
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Column parameter1, Expression parameter2)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    parameter1.Expression, parameter2
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Expression parameter1, Column parameter2)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    parameter1, parameter2.Expression
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, params Column[] parameters)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    parameters.Select(p => p.Expression).ToList()
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Column col)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    col.Expression
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, string col)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    new Column(col).Expression
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Expression col)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    col
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Column col, string value)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                   col.Expression, new Expression() 
                                    { Literal = new Expression.Types.Literal()
                                       {
                                           String = value
                                       }
                                    }
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, string col, Column value)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    new Expression() 
                    { Literal = new Expression.Types.Literal()
                        {
                            String = col
                        }
                    },
                    value.Expression
                }
            }
        };
    }
    
    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Column col, int value)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    col.Expression, new Expression() 
                    { 
                        Literal = new Expression.Types.Literal()
                        {
                            Integer = value
                        }
                    }
                }
            }
        };
    }

    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Column col, double value)
    {
        return new Expression()
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction()
            {
                FunctionName = function,
                IsDistinct = isDistinct,
                IsUserDefinedFunction = false,
                Arguments =
                {
                    col.Expression, new Expression()
                    {
                        Literal = new Expression.Types.Literal()
                        {
                            Double = value
                        }
                    }
                }
            }
        };
    }

    protected internal static Expression FunctionWrappedCall(string function, bool isDistinct, Column col, float value)
        {
            return new Expression()
            {
                UnresolvedFunction = new Expression.Types.UnresolvedFunction()
                {
                    FunctionName = function,
                    IsDistinct = isDistinct,
                    IsUserDefinedFunction = false,
                    Arguments =
                    {
                        col.Expression, new Expression() 
                        { 
                            Literal = new Expression.Types.Literal()
                            {
                                Float = value
                            }
                        }
                    }
                }
            };
        }
    
    public static string CSharpFunctionName(string functionName) {
        
        var csName = new StringBuilder();
        var parts = functionName.Split('_');
        foreach (var part in parts)
        {
            csName.Append(part[0].ToString().ToUpperInvariant());
            if(part.Length > 1)
                csName.Append(part.Substring(1));
        }

        return csName.ToString();
    }
    
}