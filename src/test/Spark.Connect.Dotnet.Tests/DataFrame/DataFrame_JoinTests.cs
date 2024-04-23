using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class DataFrame_JoinTests : E2ETestBase
{
    [Fact]
    public void CrossJoin_Test()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)");
        df.CrossJoin(df).Show();
    }

    [Fact]
    public void Join_Test()
    {
        var df = Spark.Sql("SELECT * from range(100)");

        df.Join(df, new List<string>(), JoinType.Cross).Show();
        df.Join(df, new List<string> { "id" }, JoinType.LeftAnti).Show();
        df.Join(df, new List<string> { "id" }, JoinType.LeftOuter).Show();
        df.Join(df, new List<string> { "id" }, JoinType.LeftSemi).Show();
        df.Join(df, new List<string> { "id" }, JoinType.Inner).Show();
        df.Join(df, new List<string> { "id" }, JoinType.FullOuter).Show();
        df.Join(df, new List<string> { "id" }, JoinType.RightOuter).Show();
    }
}