using System.Linq.Expressions;

namespace Spark.Connect.Dotnet.Sql;

public class SparkLambdaFunction
{
    public Plan GetPlan(Expression<Func<Column, Column>> lambda)
    {
        throw new NotImplementedException();
    }
}