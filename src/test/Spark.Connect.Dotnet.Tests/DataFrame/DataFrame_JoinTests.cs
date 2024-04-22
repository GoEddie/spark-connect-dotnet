using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;
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
        
        df.Join(df, new List<string>(), how: JoinType.Cross).Show();
        df.Join(df, new List<string>(){"id"}, how: JoinType.LeftAnti).Show();
        df.Join(df, new List<string>(){"id"}, how: JoinType.LeftOuter).Show();
        df.Join(df, new List<string>(){"id"}, how: JoinType.LeftSemi).Show();
        df.Join(df, new List<string>(){"id"}, how: JoinType.Inner).Show();
        df.Join(df, new List<string>(){"id"}, how: JoinType.FullOuter).Show();
        df.Join(df, new List<string>(){"id"}, how: JoinType.RightOuter).Show();
        
    }

    
    
}