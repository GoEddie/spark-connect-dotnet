namespace Spark.Connect.Dotnet.Sql;

public static class Window
{
    public static WindowSpec PartitionBy(string col) => new WindowSpec().PartitionBy(col);
    
    public static WindowSpec PartitionBy(Column col) => new WindowSpec().PartitionBy(col);

    public static WindowSpec OrderBy(Column col) => new WindowSpec().OrderBy(col);

    public static WindowSpec OrderBy(string col) => new WindowSpec().OrderBy(col);
}

public class WindowSpec
{
    private readonly List<Expression.Types.SortOrder> _orderSpec = new();
    private readonly List<Expression> _partitionSpec = new();

    public WindowSpec PartitionBy(string col)
    {
        _partitionSpec.Add(new Column(col).Expression);
        return this;
    }

    public WindowSpec PartitionBy(Column col)
    {
        _partitionSpec.Add(col.Expression);
        return this;
    }

    public WindowSpec OrderBy(Column col)
    {
        _orderSpec.Add(new Expression.Types.SortOrder
        {
            Child = col.Expression
        });

        return this;
    }

    public WindowSpec OrderBy(string col)
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