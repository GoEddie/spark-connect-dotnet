namespace Spark.Connect.Dotnet.Sql;

public partial class Functions : FunctionsInternal
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

    public static Expression Lit(int value)
    {
        return new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                Integer = value
            }
        };
    }
    
    public static SparkColumn Col(string name) => Column(name);

    public static SparkColumn Column(string name) => new (name);
    
    
    /// <param name="cols">List&lt;String&gt;</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static SparkColumn CountDistinct(List<String> cols) => new(FunctionCall2("count_distinct", false, cols));
    
    /// <param name="cols">List&lt;SparkColumn&gt;</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static SparkColumn CountDistinct(List<SparkColumn> cols) => new(FunctionCall2("count_distinct", false, cols));

    /// <param name="col">String</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static SparkColumn CountDistinct(String col) => new(FunctionCall2("count_distinct", false, col));

    /// <param name="col">SparkColumn</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static SparkColumn CountDistinct(SparkColumn col) => new(FunctionCall2("count_distinct", false, col));

}