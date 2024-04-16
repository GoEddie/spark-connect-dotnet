namespace Spark.Connect.Dotnet.Sql;

public class Window
{
    private List<Expression.Types.SortOrder> _orderSpec = new List<Expression.Types.SortOrder>();
    private List<Expression> _partitionSpec = new List<Expression>();
    
    public Window PartitionBy(string col)
    {
        _partitionSpec.Add(new SparkColumn(col).Expression);
        return this;
    }

    public Window PartitionBy(SparkColumn col)
    {
        _partitionSpec.Add(col.Expression);
        return this;
    }
    public Window OrderBy(SparkColumn col)
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
            Child = new SparkColumn(col).Expression
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