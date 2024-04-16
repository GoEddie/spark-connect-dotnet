using Google.Protobuf.WellKnownTypes;

namespace Spark.Connect.Dotnet.Sql;

public class FunctionsInternal
{ 
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
    
    protected internal static Expression FunctionCall(string function, Expression param1, bool isDistinct = false)
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
                    new List<Expression>(){param1}
                }
            }
        };
    }
    
    protected internal static Expression FunctionCall2(string function, bool isDistinct = false)
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
                    new List<Expression>(){}
                }
            }
        };
    }
    
    protected internal static Expression FunctionCall2(string function, bool isDistinct, params Expression[] parameters)
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
                    parameters.ToList<Expression>()
                }
            }
        };
    }

    protected internal static Expression FunctionCall2(string function, bool isDistinct, params SparkColumn[] parameters)
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
    
    private static Expression ColumnNameToExpression(string columnName) => new()
        {
            UnresolvedAttribute = new Expression.Types.UnresolvedAttribute()
            {
                UnparsedIdentifier = columnName
            }
        };
    
    // protected internal static Expression FunctionCall2(string function, bool isDistinct, params string[] parameters) => FunctionCall2(function, isDistinct, parameters.Select(ColumnNameToExpression).ToArray());
    
    protected internal static Expression FunctionCall2(string function, bool isDistinct, string columnName, params object[] parameters)
    {
        var expressions = new List<Expression>();
        expressions.Add(ColumnNameToExpression(columnName));
        expressions.AddRange(parameters.Select(Functions.ObjectToLit));
        return FunctionCall2(function, isDistinct, expressions.ToArray());
    }
    
    protected internal static Expression FunctionCall2(string function, bool isDistinct, SparkColumn column, params object[] parameters)
    {
        var expressions = new List<Expression>();
        expressions.Add(column.Expression);
        expressions.AddRange(parameters.Select(Functions.ObjectToLit));
        return FunctionCall2(function, isDistinct, expressions.ToArray());
    }

    protected internal static Expression FunctionCall2(string function, bool isDistinct, List<string> parameters) => FunctionCall2(function, isDistinct, parameters.Select(ColumnNameToExpression).ToArray());

    protected internal static Expression FunctionCall2(string function, bool isDistinct, List<SparkColumn> parameters) => FunctionCall2(function, isDistinct, parameters.ToArray());

    protected internal static Expression FunctionCallColumnLits(string function, bool isDistinct, SparkColumn column, params object[] parameters)
    {
        var expressions = new List<Expression>();
        expressions.Add(column.Expression);
        expressions.AddRange(parameters.Select(Functions.ObjectToLit));
        return FunctionCall2(function, isDistinct, expressions.ToArray());
    }

    protected internal static Expression FunctionCallColumns(string function, bool isDistinct, List<string> columns)
    {
        var expressions = columns.Select(ColumnNameToExpression);
        return FunctionCall2(function, isDistinct, expressions.ToArray());
    }
    protected internal static Expression FunctionCallColumns(string function, bool isDistinct, Expression expression, string column)
    {
        return FunctionCall2(function, isDistinct, new[]{expression, ColumnNameToExpression(column)});
    }    
    protected internal static Expression FunctionCallColumns(string function, bool isDistinct, Expression expression, SparkColumn column)
    {
        return FunctionCall2(function, isDistinct, new[]{expression, column.Expression});
    }
    protected internal static Expression FunctionCallColumns(string function, bool isDistinct, params string[] columns)
    {
        var expressions = columns.Select( ColumnNameToExpression);
        return FunctionCall2(function, isDistinct, expressions.ToArray());
    }
    protected internal static Expression FunctionCallColumns(string function, bool isDistinct, List<SparkColumn> columns)
    {
        var expressions = columns.Select( p=>p.Expression);
        return FunctionCall2(function, isDistinct, expressions.ToArray());
    }
    protected internal static Expression FunctionCallColumns(string function, bool isDistinct, params SparkColumn[] columns)
    {
        var expressions = columns.Select( p=>p.Expression);
        return FunctionCall2(function, isDistinct, expressions.ToArray());
    }
    
}