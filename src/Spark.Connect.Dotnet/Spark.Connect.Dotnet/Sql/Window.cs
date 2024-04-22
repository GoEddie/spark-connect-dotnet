namespace Spark.Connect.Dotnet.Sql;

public class Window
{
    private List<Expression.Types.SortOrder> _orderSpec = new List<Expression.Types.SortOrder>();
    private List<Expression> _partitionSpec = new List<Expression>();
    
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
        _orderSpec.Add(new Expression.Types.SortOrder()
        {
            Child = col.Expression
        });
        
        return this;
    }
    
    public Window OrderBy(string col)
    {
        _orderSpec.Add(new Expression.Types.SortOrder()
        {
            Child = new Column(col).Expression
        });
        
        return this;
    }
    
    public Window()
    {
        
    }

    public Expression ToExpression(Expression function) => new Expression()
    {
        Window = new Expression.Types.Window()
        {
            WindowFunction = new Expression()
            {
                UnresolvedFunction = function.UnresolvedFunction
            },
            OrderSpec = { _orderSpec },
            PartitionSpec = { _partitionSpec }
        }
    };
}