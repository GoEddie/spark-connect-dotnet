namespace Spark.Connect.Dotnet.Sql;

public class SparkLambdaFunction
{
    public Plan GetPlan()
    {
        var plan = new Plan();
        var expression = new Expression()
        {
            LambdaFunction = new Expression.Types.LambdaFunction()
            {
                Function = new Expression()
                {

                }
            }
        };

        throw new NotImplementedException();x
    }
}