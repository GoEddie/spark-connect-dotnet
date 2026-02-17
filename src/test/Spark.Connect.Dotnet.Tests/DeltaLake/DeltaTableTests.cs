using Spark.Connect.Dotnet.DeltaLake;
using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.DeltaLake;

public class DeltaTableTests : E2ETestBase
{
    public DeltaTableTests(ITestOutputHelper logger) : base(logger)
    {
        Spark.Range(100).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/test");
        Spark.Range(100).Write().Mode("overwrite").SaveAsTable("deltatesttable99", "delta", "overwrite");
    }

    [Fact]
    public void ForPathTest()
    {
        
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/test");
        dt.ToDF().Show();
    }
    
    [Fact]
    public void ForNameTest()
    {
        
        
        var dt2 = DeltaTable.ForName(Spark, "deltatesttable99");
        dt2.ToDF().Show();
    }

    [Fact]
    public void AsTest()
    {
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/test");
        var dt2= dt.As("test");
        dt2.ToDF().Select("test.id").Show();
    }

    [Fact]
    public void VaccumTest()
    {
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/test");
        dt.Vacuum(169);
    }

    [Fact]
    public void HistoryTest()
    {
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/test");
        dt.History().Show();
        dt.History(1).Show();
    }

    [Fact]
    public void DetailTest()
    {
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/test");
        dt.Detail().Show();
    }

    [Fact]
    public void DeleteUsingStringExprTest()
    {
        Spark.Range(100).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/deletetest");
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/deletetest");
        Assert.Equal(100,  dt.ToDF().Count());
        dt.Delete("id = 50");
        Assert.Equal(99,  dt.ToDF().Count());
    }
    
    [Fact]
    public void DeleteUsingColumnTest()
    {
        Spark.Range(100).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/deletetest-col");
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/deletetest-col");
        Assert.Equal(100,  dt.ToDF().Count());
        dt.Delete(Functions.Col("id") == 50 | Functions.Col("id") < -50);
        Assert.Equal(99,  dt.ToDF().Count());
    }
    
    [Fact]
    public void UpdateTest()
    {
        Spark.Range(100).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/update-1");
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/update-1");

        var map = new Dictionary<Column, Column> {{Functions.Col("id"), Functions.Lit(-999)}};
        dt.Update(Functions.Col("id") <= 49, map);
        
        var check = DeltaTable.ForPath(Spark, "/tmp/delta99/update-1");
        check.ToDF().Show();
        Assert.Equal(50, check.ToDF().Filter(Functions.Col("id") == -999).Count() );
    }
    
    [Fact]
    public void UpdateColNameTest()
    {
        Spark.Range(100).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/update-2");
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/update-2");

        var map = new Dictionary<string, Column> {{"id", Functions.Lit(-999)}};
        dt.Update(Functions.Col("id") <= 49, map);
        
        var check = DeltaTable.ForPath(Spark, "/tmp/delta99/update-2");
        check.ToDF().Show();
        Assert.Equal(50, check.ToDF().Filter(Functions.Col("id") == -999).Count() );
    }
    
    [Fact]
    public void UpdateColNameNoFilterTest()
    {
        Spark.Range(100).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/update-3");
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/update-3");

        var map = new Dictionary<string, Column> {{"id", Functions.Lit(-999)}};
        dt.Update(map);
        
        var check = DeltaTable.ForPath(Spark, "/tmp/delta99/update-3");
        check.ToDF().Show();
        Assert.Equal(100, check.ToDF().Filter(Functions.Col("id") == -999).Count() );
    }
    
    [Fact]
    public void UpdateColNoFilterTest()
    {
        Spark.Range(100).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/update-4");
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/update-4");

        var map = new Dictionary<Column, Column> {{Functions.Col("id"), Functions.Lit(-999)}};
        dt.Update(map);
        
        var check = DeltaTable.ForPath(Spark, "/tmp/delta99/update-4");
        check.ToDF().Show();
        Assert.Equal(100, check.ToDF().Filter(Functions.Col("id") == -999).Count() );
    }

    [Fact]
    public void MergeUpdateTest()
    {
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/merge-1");
        var target = DeltaTable.ForPath(Spark, "/tmp/delta99/merge-1").As("target");
        var source = Spark.Range(5).WithColumn("name", Functions.Lit("XYZ")).Alias("source");

        target.ToDF().Show();
        
        target
            .Merge(source, "source.id == target.id")
            .WhenMatchedUpdate("target.id = 0", new Dictionary<string, Column>()
            {
                { "name", Functions.Lit("ZEROTH") }
            })
            .WhenMatchedUpdate("target.id = 1", (Functions.Col("target.name"), Functions.Lit("ONEONEONE") ))
            .WhenMatchedUpdate(Functions.Col("target.id") == 2, (Functions.Col("target.name"), Functions.Lit("TWO")))
            .WhenMatchedUpdate(target.ToDF()["id"] == 3, (Functions.Col("target.name"), Functions.Lit("THREE") ))
            .WhenMatchedUpdate(new Dictionary<string, Column>()
            {
                { "name", Functions.Lit("A NEW NAME") }
            })
            .WhenNotMatchedBySourceUpdate("target.id = 6", new Dictionary<string, Column>()
            {
                { "name", Functions.Lit("SIX") }
            })
            .WhenNotMatchedBySourceUpdate(Functions.Col("target.id") == 7, ("target.name", Functions.Lit("SEVEN")))
            .WhenNotMatchedBySourceUpdate(Functions.Col("target.id") == 8, (Functions.Col("name"), Functions.Lit("EIGHT")))
            .WhenNotMatchedBySourceUpdate((Functions.Col("name"), Functions.Lit("REMAINING")))
            .Execute(Spark);
        
        target.ToDF().Show(20);
    }
    
    [Fact]
    public void MergeUpdateAllTest()
    {
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/merge-2");
        var target = DeltaTable.ForPath(Spark, "/tmp/delta99/merge-2").As("target");
        var source = Spark.Range(5).WithColumn("name", Functions.Lit("XYZ")).Alias("source");

        target.ToDF().Show();
        
        target
            .Merge(source, "source.id == target.id")
            .WhenMatchedUpdateAll("target.id = 0")
            .WhenMatchedUpdateAll(Functions.Col("target.id") == 2)
            .Execute(Spark);
        
        target.ToDF().Show(20);
    }
    
    [Fact]
    public void MergeInsertTest()
    {
        Spark.Range(1).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/merge-3");
        var target = DeltaTable.ForPath(Spark, "/tmp/delta99/merge-3").As("target");
        var source = Spark.Range(10).WithColumn("name", Functions.Lit("XYZ")).Alias("source");

        target.ToDF().Show();
        
        target
            .Merge(source, "source.id == target.id")
            .WhenNotMatchedInsert("source.id = 2", 
                            (Functions.Col("target.name"), Functions.Lit("TWO")), 
                            (Functions.Col("target.id"), Functions.Col("source.id")))
            .WhenNotMatchedInsert(
                (Functions.Col("target.name"), Functions.Lit("EVERYTHING ELSE")),
                (Functions.Col("target.id"), Functions.Col("source.id"))
                )
            .Execute(Spark);
        
        target.ToDF().Show(20);
    }
    
    [Fact]
    public void MergeInsertAllTest()
    {
        Spark.Range(1).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/merge-4");
        var target = DeltaTable.ForPath(Spark, "/tmp/delta99/merge-4").As("target");
        var source = Spark.Range(10).WithColumn("name", Functions.Lit("XYZ")).Alias("source");

        target.ToDF().Show();
        
        target
            .Merge(source, "source.id == target.id")
            .WhenNotMatchedInsertAll("source.id = 2")
            .WhenNotMatchedInsertAll()
            .Execute(Spark);
        
        target.ToDF().Show(20);
    }
    
    
    [Fact]
    public void MergeMatchedDeleteTest()
    {
        Spark.Range(20).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/merge-4");
        var target = DeltaTable.ForPath(Spark, "/tmp/delta99/merge-4").As("target");
        var source = Spark.Range(10).WithColumn("name", Functions.Lit("XYZ")).Alias("source");

        target.ToDF().Show();
        
        target
            .Merge(source, "source.id == target.id")
            .WhenMatchedDelete("source.id = 2")
            .WhenMatchedDelete(Functions.Col("source.id") == 5)
            .Execute(Spark);
        
        target.ToDF().Show(20);
        
        target
            .Merge(source, "source.id == target.id")
            .WhenMatchedDelete()
            .Execute(Spark);
        
        target.ToDF().Show(20);
    }
    
    [Fact]
    public void MergeNotMatchedBySourceDeleteTest()
    {
        Spark.Range(20).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/merge-5");
        var target = DeltaTable.ForPath(Spark, "/tmp/delta99/merge-5").As("target");
        var source = Spark.Range(5).WithColumn("name", Functions.Lit("XYZ")).Alias("source");

        target.ToDF().Show();
        
        target
            .Merge(source, "source.id == target.id")
            .WhenNotMatchedBySourceDelete("target.id = 15")
            .WhenNotMatchedBySourceDelete(Functions.Col("target.id")  == 17)
            .Execute(Spark);
        
        target.ToDF().Show(20);
        
        target
            .Merge(source, "source.id == target.id")
            .WhenNotMatchedBySourceDelete()
            .Execute(Spark);
        
        target.ToDF().Show(20);
    }

    [Fact]
    public void CreateDeltaTableTest()
    {
        var deltaTable = DeltaTable.Create(Spark)
            .Location("/tmp/delta99/create-test/" + Guid.NewGuid().ToString())
            .AddColumn(
                new DeltaTableColumnBuilder("abc").DataType("int").Nullable(true).Build())
            .AddColumn(
                new DeltaTableColumnBuilder("def").DataType("int").Nullable(false).GeneratedAlwaysAs("abc * 100").Build()
            ).Execute();
        
        deltaTable.ToDF().Show();
        
        var source = Spark.Range(10).WithColumnRenamed("id", "abc").Alias("source");
        deltaTable
            .As("target")
            .Merge(source, "source.abc = target.abc")
            .WhenNotMatchedInsertAll()
            .Execute(Spark);
        
        deltaTable.ToDF().Show();
    }

    [Fact]
    public void IsDeltaTableTest()
    {
        string location = "/tmp/delta99/isdeltatable/" + Guid.NewGuid().ToString();
        
        var deltaTable = DeltaTable.Create(Spark)
            .Location(location)
            .AddColumn(
                new DeltaTableColumnBuilder("abc").DataType("int").Nullable(true).Build())
            .AddColumn(
                new DeltaTableColumnBuilder("def").DataType("int").Nullable(false).GeneratedAlwaysAs("abc * 100").Build()
            ).Execute();
        
        Assert.True(DeltaTable.IsDeltaTable(Spark, location));
        Assert.False(DeltaTable.IsDeltaTable(Spark, "/tmp/fakeit"));
    }
    
    [Fact]
    public void UpgradeProtocolTest()
    {
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/protoversion");
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/protoversion");
        dt.UpgradeTableProtocol(1,1);
    }
    
    [Fact]
    public void RestoreToVersionTest()
    {
        Spark.Range(100).WithColumn("name", Functions.Lit("VERSION1")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretoversion-99");
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretoversion-99");
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretoversion-99");
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretoversion-99");
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretoversion-99");
        
        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/restoretoversion-99");
        dt.History().Show(20);
        
        dt.RestoreToVersion(0).Show();
        dt.ToDF().Show();
        
        Assert.Equal("VERSION1", dt.ToDF().Select("name").Limit(1).Collect()[0][0]);
    }
    
    [Fact (Skip = "slow, run if you need to test delta restore")]
    public void RestoreToDateTest()
    {
        
        Spark.Range(100).WithColumn("name", Functions.Lit("VERSION1")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretodate-99");
        Thread.Sleep(2000);
        var restoreToHere = DateTime.Now;
        Thread.Sleep(2000);
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretoversion-99");
        Thread.Sleep(2000);
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretoversion-99");
        Thread.Sleep(2000);
        Spark.Range(100).WithColumn("name", Functions.Lit("ABC")).Write().Mode("overwrite").Format("delta").Save("/tmp/delta99/restoretoversion-99");

        var dt = DeltaTable.ForPath(Spark, "/tmp/delta99/restoretodate-99");
        dt.History().Show(20, 10000);
        var ts = (dt.History().Select( Functions.Col("timestamp").Cast("string")).OrderBy(Functions.Desc("timestamp")).Collect()[1][0]).ToString();
        dt.RestoreToTimestamp(ts).Show();
        dt.ToDF().Show();
        Assert.Equal("VERSION1", dt.ToDF().Select("name").Limit(1).Collect()[0][0]);
    }

    [Fact]
    public void DeltaOptimizeTest()
    {
        (Spark.Range(10)
            .Union(Spark.Range(10))
            .Union(Spark.Range(10))
            .Union(Spark.Range(10)))
            .WithColumn("name", Functions.Lit("VERSION1"))
            .WithColumn("site", Functions.Lit("LONDON"))
            .WithColumn("rand", Functions.Rand())
            .Write()
            .PartitionBy("id")
            .Mode("overwrite")
            .Format("delta")
            .Save("/tmp/delta99/optimize-888888");
        
        var dt = DeltaTable
            .ForPath(Spark, "/tmp/delta99/optimize-888888");
        
        dt
            .Optimize()
            .ExecuteZOrderBy("rand")
            .Show(10, 10000);
        
        dt
            .Optimize()
            .ExecuteCompaction()
            .Show(10, 100000);
    }
}