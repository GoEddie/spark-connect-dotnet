using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Sql;

/// <summary>
/// A column in a DataFrame.
///
/// This wraps an `Expression` which is what we pass to Spark Connect.
/// </summary>
public class Column
{
    private readonly string _name = string.Empty;
    
    /// <summary>
    /// This is what the Column actually does, it is what is passed to spark
    /// </summary>
    public readonly Expression Expression;

    /// <summary>
    /// Get a column by name. Static version used by the functions and wraps `Column(name)`
    /// </summary>
    /// <param name="name"></param>
    /// <returns>Column</returns>
    public static Column Col(string name)
    {
        return new Column(name);
    }

    /// <summary>
    /// Create a `Column` from an `Expression`
    /// </summary>
    /// <param name="expression"></param>
    public Column(Expression expression)
    {
        Expression = expression;
    }

    /// <summary>
    /// Pass either the name of a column, or *. You can also pass a `DataFrame` to use as the source.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="source"></param>
    public Column(string name, DataFrame? source = null)
    {
        _name = name;

        if (_name == "*")
        {
            Expression = new Expression
            {
                UnresolvedStar = new Expression.Types.UnresolvedStar()
            };

            return;
        }

        if (null != source)
        {
            Expression = new Expression
            {
                UnresolvedAttribute = new Expression.Types.UnresolvedAttribute
                {
                    UnparsedIdentifier = name,
                    PlanId = source.Relation.Common.PlanId
                }
            };

            return;
        }


        Expression = new Expression
        {
            UnresolvedAttribute = new Expression.Types.UnresolvedAttribute
            {
                UnparsedIdentifier = name
            }
        };
    }

    /// <summary>
    ///     Returns the `Column` denoted by name.
    /// </summary>
    /// <param name="name"></param>
    public Column this[string name] =>
        new(new Expression
        {
            UnresolvedExtractValue = new Expression.Types.UnresolvedExtractValue
            {
                Child = Expression,
                Extraction = Functions.Lit(name).Expression
            }
        });

    private bool Equals(Column other)
    {
        return Expression.Equals(other.Expression);
    }

    
    /// <summary>
    /// Does this column equal the passed in object?
    /// </summary>
    /// <param name="obj"></param>
    /// <returns></returns>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj))
        {
            return false;
        }

        if (ReferenceEquals(this, obj))
        {
            return true;
        }

        return obj.GetType() == GetType() && Equals((Column)obj);
    }

    /// <summary>
    /// Get the hash code of the column (hopefully no one needs this, if they do then it sounds like a debugging nightmare)
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode()
    {
        return Expression.GetHashCode();
    }

    /// <summary>
    /// Alias the column to another name
    /// </summary>
    /// <param name="name"></param>
    /// <returns>Column</returns>
    public Column Alias(string name)
    {
        var expression = new Expression
        {
            Alias = new Expression.Types.Alias
            {
                Expr = Expression,
                Name = { name }
            }
        };

        return new Column(expression);
    }

    /// <summary>
    /// Alias the column to another name
    /// </summary>
    /// <param name="name">The new name</param>
    /// <returns>Column</returns>
    public Column As(string name)
    {
        return Alias(name);
    }

    /// <summary>
    /// Order the existing column ascending
    /// </summary>
    /// <returns></returns>
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

    /// <summary>
    /// Order the existing column descending
    /// </summary>
    /// <returns></returns>
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

    /// <summary>
    /// &amp; the column with the passed in bool
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator &(Column src, bool value)
    {
        return src.And(value);
    }

    /// <summary>
    /// &amp; the column with the passed in `Column`
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator &(Column src, Column value)
    {
        return src.And(value);
    }

    /// <summary>
    /// &amp; the column with the passed in bool
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column And(bool value)
    {
        return BinaryOperation(value, "and");
    }

    /// <summary>
    /// &amp; the column with the passed in `Column`
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column And(Column value)
    {
        return BinaryOperation(value, "and");
    }

    /// <summary>
    /// | the column with the passed in bool
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator |(Column src, bool value)
    {
        return src.Or(value);
    }

    /// <summary>
    /// | the column with the passed in `Column`
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator |(Column src, Column value)
    {
        return src.Or(value);
    }

    /// <summary>
    /// | the column with the passed in bool
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Or(bool value)
    {
        return BinaryOperation(value, "or");
    }

    /// <summary>
    /// | the column with the passed in `Column`
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Or(Column value)
    {
        return BinaryOperation(value, "or");
    }

    /// <summary>
    /// ! the column
    /// </summary>
    /// <param name="src"></param>
    /// <returns></returns>
    public static Column operator !(Column src)
    {
        return src.Not();
    }

    /// <summary>
    /// ! the column
    /// </summary>
    /// <returns></returns>
    public Column Not()
    {
        return BinaryOperation("not");
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator *(Column src, int value)
    {
        return src.Multiply(value);
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="src"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Column operator *(Column src, double right)
    {
        return src.Multiply(right);
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="src"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Column operator *(Column src, float right)
    {
        return src.Multiply(right);
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="src"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Column operator *(Column src, long right)
    {
        return src.Multiply(right);
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="src"></param>
    /// <param name="right"></param>
    /// <returns></returns>
    public static Column operator *(Column src, short right)
    {
        return src.Multiply(right);
    }

    /// <summary>
    /// Multiply the column with the other column
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator *(Column src, Column value)
    {
        return src.Multiply(value);
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Multiply(int value)
    {
        return BinaryOperation(value, "*");
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Multiply(float value)
    {
        return BinaryOperation(value, "*");
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Multiply(double value)
    {
        return BinaryOperation(value, "*");
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Multiply(long value)
    {
        return BinaryOperation(value, "*");
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Multiply(short value)
    {
        return BinaryOperation(value, "*");
    }

    /// <summary>
    /// Multiply the column with the value
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Multiply(Column value)
    {
        return BinaryOperation(value, "*");
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator ==(Column src, int value)
    {
        return src.EqualTo(value);
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator ==(Column src, float value)
    {
        return src.EqualTo(value);
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator ==(Column src, double value)
    {
        return src.EqualTo(value);
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator ==(Column src, bool value)
    {
        return src.EqualTo(value);
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator ==(Column src, long value)
    {
        return src.EqualTo(value);
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator ==(Column src, string value)
    {
        return src.EqualTo(value);
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator ==(Column src, Column value)
    {
        return src.EqualTo(value);
    }

    public static Column operator ==(Column src, DateTime value)
    {
        return src.EqualTo(value);
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column EqualTo(string value)
    {
        return BinaryOperation(value, "==");
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column EqualTo(int value)
    {
        return BinaryOperation(value, "==");
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column EqualTo(float value)
    {
        return BinaryOperation(value, "==");
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column EqualTo(double value)
    {
        return BinaryOperation(value, "==");
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column EqualTo(bool value)
    {
        return BinaryOperation(value, "==");
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column EqualTo(long value)
    {
        return BinaryOperation(value, "==");
    }

    /// <summary>
    /// Does the column equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column EqualTo(Column value)
    {
        return BinaryOperation(value, "==");
    }

    public Column EqualTo(DateTime value)
    {
        return BinaryOperation(value, "==");
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator !=(Column src, int value)
    {
        return src.NotEqualTo(value);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator !=(Column src, string value)
    {
        return src.NotEqualTo(value);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator !=(Column src, long value)
    {
        return src.NotEqualTo(value);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator !=(Column src, float value)
    {
        return src.NotEqualTo(value);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator !=(Column src, double value)
    {
        return src.NotEqualTo(value);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator !=(Column src, bool value)
    {
        return src.NotEqualTo(value);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator !=(Column src, Column value)
    {
        return src.NotEqualTo(value);
    }

    public static Column operator !=(Column src, DateTime value)
    {
        return src.NotEqualTo(value);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column NotEqualTo(int value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column NotEqualTo(string value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column NotEqualTo(float value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column NotEqualTo(double value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column NotEqualTo(bool value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column NotEqualTo(long value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    /// <summary>
    /// Does the column not equal the other one?
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column NotEqualTo(Column value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    public Column NotEqualTo(DateTime value)
    {
        var equals = BinaryOperation(value, "==");
        return NotOperation(equals);
    }

    /// <summary>
    /// RMultiply by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMultiply(int value)
    {
        return BinaryOperation(value, "*", true);
    }

    /// <summary>
    /// RMultiply by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMultiply(Column value)
    {
        return BinaryOperation(value, "*", true);
    }
    
    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator /(Column src, int value)
    {
        return src.Divide(value);
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator /(Column src, float value)
    {
        return src.Divide(value);
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator /(Column src, double value)
    {
        return src.Divide(value);
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator /(Column src, short value)
    {
        return src.Divide(value);
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator /(Column src, long value)
    {
        return src.Divide(value);
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator /(Column src, Column value)
    {
        return src.Divide(value);
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Divide(int value)
    {
        return BinaryOperation(value, "/");
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Divide(float value)
    {
        return BinaryOperation(value, "/");
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Divide(double value)
    {
        return BinaryOperation(value, "/");
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Divide(short value)
    {
        return BinaryOperation(value, "/");
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Divide(long value)
    {
        return BinaryOperation(value, "/");
    }

    /// <summary>
    /// Divide by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Divide(Column value)
    {
        return BinaryOperation(value, "/");
    }

    /// <summary>
    /// RDivide by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RDivide(int value)
    {
        return BinaryOperation(value, "/", true);
    }

    /// <summary>
    /// RDivide by
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RDivide(Column value)
    {
        return BinaryOperation(value, "/", true);
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator +(Column src, int value)
    {
        return src.Add(value);
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator +(Column src, float value)
    {
        return src.Add(value);
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator +(Column src, double value)
    {
        return src.Add(value);
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator +(Column src, short value)
    {
        return src.Add(value);
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator +(Column src, long value)
    {
        return src.Add(value);
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator +(Column src, Column value)
    {
        return src.Add(value);
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Add(int value)
    {
        return BinaryOperation(value, "+");
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Add(float value)
    {
        return BinaryOperation(value, "+");
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Add(double value)
    {
        return BinaryOperation(value, "+");
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Add(short value)
    {
        return BinaryOperation(value, "+");
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Add(long value)
    {
        return BinaryOperation(value, "+");
    }

    /// <summary>
    /// Add
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Add(Column value)
    {
        return BinaryOperation(value, "+");
    }
    
    /// <summary>
    /// RAdd
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RAdd(int value)
    {
        return BinaryOperation(value, "+", true);
    }

    /// <summary>
    /// RAdd
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RAdd(float value)
    {
        return BinaryOperation(value, "+", true);
    }

    /// <summary>
    /// RAdd
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RAdd(double value)
    {
        return BinaryOperation(value, "+", true);
    }

    /// <summary>
    /// RAdd
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RAdd(short value)
    {
        return BinaryOperation(value, "+", true);
    }

    /// <summary>
    /// RAdd
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RAdd(long value)
    {
        return BinaryOperation(value, "+", true);
    }

    /// <summary>
    /// RAdd
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RAdd(Column value)
    {
        return BinaryOperation(value, "+", true);
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator -(Column src, int value)
    {
        return src.Minus(value);
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator -(Column src, float value)
    {
        return src.Minus(value);
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator -(Column src, double value)
    {
        return src.Minus(value);
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator -(Column src, short value)
    {
        return src.Minus(value);
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator -(Column src, long value)
    {
        return src.Minus(value);
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator -(Column src, Column value)
    {
        return src.Minus(value);
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Minus(int value)
    {
        return BinaryOperation(value, "-");
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Minus(float value)
    {
        return BinaryOperation(value, "-");
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Minus(double value)
    {
        return BinaryOperation(value, "-");
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Minus(short value)
    {
        return BinaryOperation(value, "-");
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Minus(long value)
    {
        return BinaryOperation(value, "-");
    }

    /// <summary>
    /// Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Minus(Column value)
    {
        return BinaryOperation(value, "-");
    }

    /// <summary>
    /// R Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMinus(int value)
    {
        return BinaryOperation(value, "-", true);
    }

    /// <summary>
    /// R Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMinus(float value)
    {
        return BinaryOperation(value, "-", true);
    }
    
    /// <summary>
    /// R Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMinus(double value)
    {
        return BinaryOperation(value, "-", true);
    }

    /// <summary>
    /// R Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMinus(short value)
    {
        return BinaryOperation(value, "-", true);
    }

    /// <summary>
    /// R Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMinus(long value)
    {
        return BinaryOperation(value, "-", true);
    }

    /// <summary>
    /// R Subtract
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMinus(Column value)
    {
        return BinaryOperation(value, "-", true);
    }
    
    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator %(Column src, int value)
    {
        return src.Mod(value);
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator %(Column src, float value)
    {
        return src.Mod(value);
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator %(Column src, double value)
    {
        return src.Mod(value);
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator %(Column src, short value)
    {
        return src.Mod(value);
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator %(Column src, long value)
    {
        return src.Mod(value);
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="src"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator %(Column src, Column value)
    {
        return src.Mod(value);
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Mod(int value)
    {
        return BinaryOperation(value, "%");
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Mod(float value)
    {
        return BinaryOperation(value, "%");
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Mod(double value)
    {
        return BinaryOperation(value, "%");
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Mod(short value)
    {
        return BinaryOperation(value, "%");
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Mod(long value)
    {
        return BinaryOperation(value, "%");
    }

    /// <summary>
    /// Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Mod(Column value)
    {
        return BinaryOperation(value, "%");
    }

    /// <summary>
    /// R Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMod(int value)
    {
        return BinaryOperation(value, "%", true);
    }

    /// <summary>
    /// R Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMod(float value)
    {
        return BinaryOperation(value, "%", true);
    }

    /// <summary>
    /// R Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMod(double value)
    {
        return BinaryOperation(value, "%", true);
    }

    /// <summary>
    /// R Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMod(short value)
    {
        return BinaryOperation(value, "%", true);
    }

    /// <summary>
    /// R Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMod(long value)
    {
        return BinaryOperation(value, "%", true);
    }

    /// <summary>
    /// R Mod
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RMod(Column value)
    {
        return BinaryOperation(value, "%", true);
    }

    /// <summary>
    /// Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Pow(int value)
    {
        return BinaryOperation(value, "power");
    }

    /// <summary>
    /// Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Pow(float value)
    {
        return BinaryOperation(value, "power");
    }

    /// <summary>
    /// Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Pow(double value)
    {
        return BinaryOperation(value, "power");
    }

    /// <summary>
    /// Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Pow(short value)
    {
        return BinaryOperation(value, "power");
    }

    /// <summary>
    /// Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Pow(long value)
    {
        return BinaryOperation(value, "power");
    }

    /// <summary>
    /// Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Pow(Column value)
    {
        return BinaryOperation(value, "power");
    }

    /// <summary>
    /// R Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RPow(int value)
    {
        return BinaryOperation(value, "power", true);
    }

    /// <summary>
    /// R Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RPow(float value)
    {
        return BinaryOperation(value, "power", true);
    }

    /// <summary>
    /// R Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RPow(double value)
    {
        return BinaryOperation(value, "power", true);
    }

    /// <summary>
    /// R Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RPow(short value)
    {
        return BinaryOperation(value, "power", true);
    }

    /// <summary>
    /// R Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RPow(long value)
    {
        return BinaryOperation(value, "power", true);
    }

    /// <summary>
    /// R Pow
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column RPow(Column value)
    {
        return BinaryOperation(value, "power", true);
    }

    /// <summary>
    /// Greater than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >(Column col, int value)
    {
        return col.Gt(value);
    }

    /// <summary>
    /// Greater than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >(Column col, float value)
    {
        return col.Gt(value);
    }

    /// <summary>
    /// Greater than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >(Column col, double value)
    {
        return col.Gt(value);
    }

    /// <summary>
    /// Greater than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >(Column col, short value)
    {
        return col.Gt(value);
    }

    /// <summary>
    /// Greater than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >(Column col, long value)
    {
        return col.Gt(value);
    }

    /// <summary>
    /// Greater than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >(Column col, Column value)
    {
        return col.Gt(value);
    }

    public static Column operator >(Column col, DateTime value)
    {
        return col.Gt(value);
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <(Column col, int value)
    {
        return col.Lt(value);
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <(Column col, float value)
    {
        return col.Lt(value);
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <(Column col, double value)
    {
        return col.Lt(value);
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <(Column col, short value)
    {
        return col.Lt(value);
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <(Column col, long value)
    {
        return col.Lt(value);
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <(Column col, Column value)
    {
        return col.Lt(value);
    }

    public static Column operator <(Column col, DateTime value)
    {
        return col.Lt(value);
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <=(Column col, Column value)
    {
        return col.Le(value);
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <=(Column col, int value)
    {
        return col.Le(value);
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <=(Column col, float value)
    {
        return col.Le(value);
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <=(Column col, double value)
    {
        return col.Le(value);
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <=(Column col, short value)
    {
        return col.Le(value);
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator <=(Column col, long value)
    {
        return col.Le(value);
    }

    public static Column operator <=(Column col, DateTime value)
    {
        return col.Le(value);
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >=(Column col, Column value)
    {
        return col.Ge(value);
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >=(Column col, int value)
    {
        return col.Ge(value);
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >=(Column col, float value)
    {
        return col.Ge(value);
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >=(Column col, double value)
    {
        return col.Ge(value);
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >=(Column col, short value)
    {
        return col.Ge(value);
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="col"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public static Column operator >=(Column col, long value)
    {
        return col.Ge(value);
    }

    public static Column operator >=(Column col, DateTime value)
    {
        return col.Ge(value);
    }

    /// <summary>
    /// Greater Than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Gt(int value)
    {
        return BinaryOperation(value, ">");
    }

    /// <summary>
    /// Greater Than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Gt(float value)
    {
        return BinaryOperation(value, ">");
    }

    /// <summary>
    /// Greater Than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Gt(double value)
    {
        return BinaryOperation(value, ">");
    }

    /// <summary>
    /// Greater Than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Gt(short value)
    {
        return BinaryOperation(value, ">");
    }

    /// <summary>
    /// Greater Than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Gt(long value)
    {
        return BinaryOperation(value, ">");
    }

    /// <summary>
    /// Greater Than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Gt(Column value)
    {
        return BinaryOperation(value, ">");
    }
    
    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Gt(DateTime value)
    {
        return BinaryOperation(value, ">");
    }

    public Column Lt(int value)
    {
        return BinaryOperation(value, "<");
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Lt(float value)
    {
        return BinaryOperation(value, "<");
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Lt(double value)
    {
        return BinaryOperation(value, "<");
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Lt(short value)
    {
        return BinaryOperation(value, "<");
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Lt(long value)
    {
        return BinaryOperation(value, "<");
    }

    /// <summary>
    /// Less than
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Lt(Column value)
    {
        return BinaryOperation(value, "<");
    }
    
    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Lt(DateTime value)
    {
        return BinaryOperation(value, "<");
    }

    public Column Ge(int value)
    {
        return BinaryOperation(value, ">=");
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Ge(float value)
    {
        return BinaryOperation(value, ">=");
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Ge(double value)
    {
        return BinaryOperation(value, ">=");
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Ge(short value)
    {
        return BinaryOperation(value, ">=");
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Ge(long value)
    {
        return BinaryOperation(value, ">=");
    }

    /// <summary>
    /// Greater than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Ge(Column value)
    {
        return BinaryOperation(value, ">=");
    }
    
    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Ge(DateTime value)
    {
        return BinaryOperation(value, ">=");
    }

    public Column Le(int value)
    {
        return BinaryOperation(value, "<=");
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Le(float value)
    {
        return BinaryOperation(value, "<=");
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Le(double value)
    {
        return BinaryOperation(value, "<=");
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Le(short value)
    {
        return BinaryOperation(value, "<=");
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Le(long value)
    {
        return BinaryOperation(value, "<=");
    }

    /// <summary>
    /// Less than or equal
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public Column Le(Column value)
    {
        return BinaryOperation(value, "<=");
    }

    public Column Le(DateTime value)
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
                FunctionName = "not",
                IsUserDefinedFunction = false,
                IsDistinct = false
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

    private Column BinaryOperation(short value, string functionName, bool reverse = false)
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
                Short = value
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

    private Column BinaryOperation(long value, string functionName, bool reverse = false)
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
                Long = value
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

    private Column BinaryOperation(double value, string functionName, bool reverse = false)
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
                Double = value
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

    private Column BinaryOperation(float value, string functionName, bool reverse = false)
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
                Float = value
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

    private Column BinaryOperation(string value, string functionName, bool reverse = false)
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
                String = value
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

    private Column BinaryOperation(DateTime value, string functionName, bool reverse = false)
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
                String = value.ToString("yyyy-MM-dd HH:mm:ss.fff")
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

    /// <summary>
    /// Over
    /// </summary>
    /// <param name="window"></param>
    /// <returns></returns>
    public Column Over(WindowSpec window)
    {
        return window.ToCol(Expression);
    }

    /// <summary>
    /// Cast to the new type 
    /// </summary>
    /// <param name="type"></param>
    /// <returns>Column</returns>
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

    /// <summary>
    /// Cast to the name (string, int, etc)
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public Column Cast(string type)
    {
        return Cast(SparkDataType.FromString(type));
    }

    /// <summary>
    /// Is the column null?
    /// </summary>
    /// <returns></returns>
    public Column IsNull()
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "isnull",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        return new Column(expression);
    }

    /// <summary>
    /// Is it not null?
    /// </summary>
    /// <returns></returns>
    public Column IsNotNull()
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "isnotnull",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        return new Column(expression);
    }

    /// <summary>
    /// Does it end with the string other?
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Column EndsWith(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "endswith",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        expression.UnresolvedFunction.Arguments.Add(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                String = other
            }
        });

        return new Column(expression);
    }

    /// <summary>
    /// Does it start with the string other?
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Column StartsWith(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "startswith",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        expression.UnresolvedFunction.Arguments.Add(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                String = other
            }
        });

        return new Column(expression);
    }

    /// <summary>
    /// Is it between these two other columns?
    /// </summary>
    /// <param name="lowerBound"></param>
    /// <param name="upperBound"></param>
    /// <returns></returns>
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

    /// <summary>
    /// Drop these field names
    /// </summary>
    /// <param name="fieldNames"></param>
    /// <returns></returns>
    public Column DropFields(params string[] fieldNames)
    {
        var lastExpression = Expression;

        foreach (var field in fieldNames)
        {
            var expression = new Expression
            {
                UpdateFields = new Expression.Types.UpdateFields
                {
                    FieldName = field,
                    StructExpression = lastExpression
                }
            };

            lastExpression = expression;
        }

        return new Column(lastExpression);
    }

    /// <summary>
    /// Get a column by field name
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public Column GetField(string name)
    {
        var expression = new Expression
        {
            ExpressionString = new Expression.Types.ExpressionString
            {
                Expression = $"{_name}.{name}"
            }
        };

        return new Column(expression);
    }

    /// <summary>
    /// Is it Like other (remember case sensitive)
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Column Like(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "like",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        expression.UnresolvedFunction.Arguments.Add(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                String = other
            }
        });

        return new Column(expression);
    }

    /// <summary>
    /// Case-insensitive like
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Column ILike(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "ilike",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        expression.UnresolvedFunction.Arguments.Add(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                String = other
            }
        });

        return new Column(expression);
    }

    /// <summary>
    /// R Like
    /// </summary>
    /// <param name="other"></param>
    /// <returns></returns>
    public Column RLike(string other)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "rlike",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        expression.UnresolvedFunction.Arguments.Add(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                String = other
            }
        });

        return new Column(expression);
    }

    /// <summary>
    /// Extract the substring and return a new `Column`
    /// </summary>
    /// <param name="startPos"></param>
    /// <param name="length"></param>
    /// <returns>Column</returns>
    public Column Substr(int startPos, int length)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "substr",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        expression.UnresolvedFunction.Arguments.Add(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Integer = startPos
            }
        });

        expression.UnresolvedFunction.Arguments.Add(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Integer = length
            }
        });

        return new Column(expression);
    }

    /// <summary>
    /// Is the value in the column in the list of values?
    /// </summary>
    /// <param name="cols"></param>
    /// <returns>Column</returns>
    public Column IsIn(params object[] cols)
    {
        return IsIn(cols.ToList());
    }

    /// <summary>
    /// Is the value in the column in the list of values?
    /// </summary>
    /// <param name="cols"></param>
    /// <returns>Column</returns>
    public Column IsIn(List<object> cols)
    {
        var expression = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "in",
                IsUserDefinedFunction = false,
                IsDistinct = false
            }
        };

        expression.UnresolvedFunction.Arguments.Add(Expression);

        foreach (var o in cols)
        {
            if (o is Column col)
            {
                expression.UnresolvedFunction.Arguments.Add(col.Expression);
            }

            expression.UnresolvedFunction.Arguments.Add(Functions.Lit(o).Expression);
        }

        return new Column(expression);
    }
}