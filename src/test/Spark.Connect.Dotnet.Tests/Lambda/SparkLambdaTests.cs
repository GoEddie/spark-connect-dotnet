using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.Lambda;

public class SparkLambdaTests
{
    [Fact]
    public void Test1()
    {
        var i = 100;
        var f = new SparkLambdaFunction();
        var expression = f.GetPlan(column => Coalesce(column, Lit(i)));
    }
}