using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.Lambda;

public class SparkLambdaTests
{
    [Fact(Skip = "Not Implemented, possible future feature")]
    public void Test1()
    {
        var i = 100;
        var f = new SparkLambdaFunction();
        var expression = f.GetPlan(column => Coalesce(column, Lit(i)));
    }
}