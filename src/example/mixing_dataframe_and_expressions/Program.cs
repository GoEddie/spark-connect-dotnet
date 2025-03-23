using Spark.Connect;
using Spark.Connect.Dotnet.Sql;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();
    
    var dataFrame = spark.Range(100);
    
    var newColumnExpression = new Expression.Types.Alias()
    {
        Expr = new Expression()
        {
            Literal = new Expression.Types.Literal()
            {
                String = "This is a string from .NET"
            }
        },
        Name = {"NewColumnFromExpression"}
    };
    
    var withColumnRelation = new Relation()
    {
        WithColumns = new WithColumns()
        {
            Input = dataFrame.Relation,
            Aliases = {  newColumnExpression }
        }
    };
    
    var newDataFrame = new DataFrame(dataFrame.SparkSession, withColumnRelation);
    newDataFrame.Show();