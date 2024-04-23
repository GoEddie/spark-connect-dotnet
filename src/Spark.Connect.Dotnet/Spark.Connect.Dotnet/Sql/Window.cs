namespace Spark.Connect.Dotnet.Sql;

public class Window
{
    private readonly List<Expression.Types.SortOrder> _orderSpec = new();
    private readonly List<Expression> _partitionSpec = new();

    public Window PartitionBy(string col)
    {
        _partitionSpec.Add(new Column(col).Expression);
        return this;
    }

    public Window PartitionBy(Column col)
    {
        _partitionSpec.Add(col.Expression);
        return this;
    }

    public Window OrderBy(Column col)
    {
        _orderSpec.Add(new Expression.Types.SortOrder
        {
            Child = col.Expression
        });

        return this;
    }

    public Window OrderBy(string col)
    {
        _orderSpec.Add(new Expression.Types.SortOrder
        {
            Child = new Column(col).Expression
        });

        return this;
    }

    public Expression ToExpression(Expression function)
    {
        return new Expression()
        {
            Window = new Expression.Types.Window
            {
                WindowFunction = new Expression
                {
                    UnresolvedFunction = function.UnresolvedFunction
                },
                OrderSpec = { _orderSpec },
                PartitionSpec = { _partitionSpec }
            }
        };
    }
}