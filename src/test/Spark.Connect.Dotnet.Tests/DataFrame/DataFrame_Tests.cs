using System.Runtime.InteropServices;
using Google.Protobuf.WellKnownTypes;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using static Spark.Connect.Dotnet.Sql.Functions;
using static Spark.Connect.Dotnet.Sql.Types.SparkDataType;

namespace Spark.Connect.Dotnet.Tests.DataFrame;

public class DataFrame_Tests : E2ETestBase
{
    [Fact]
    public void AliasTest()
    {
        var df1 = Spark.Range(0, 5).Alias("bob");
        df1.Select(Col("bob.id")).Show();
    }

    [Fact(Skip = "GH")]
    public void PersistTest()
    {
        var df1 = Spark.Range(0, 5).Cache();
        df1.Show();
    }

    [Fact]
    public void PartitionTest()
    {
        Spark.Range(1).Coalesce().Show();
        Spark.Range(1).Coalesce(1).Show();
        Spark.Range(1).Repartition(10, Col("id")).Show();
        Spark.Range(1).Repartition(Col("id")).Show();
    }

    [Fact]
    public void ColRegexTest()
    {
        var df = Spark.Sql("SELECT 'a' as Col1, id as Col2 from range(100)");
        df.Select(df.ColRegex("`(Col1)?+.+`")).Show();
    }

    [Fact]
    public void ColumnsTest()
    {
        var df = Spark.Sql("SELECT 'a' as Col1, id as Col2 from range(100)");
        Assert.Equal(new List<string> { "Col1", "Col2" }, df.Columns);
    }

    [Fact]
    public void CorrTest()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)");
        Assert.Equal(1F, df.Corr("col1", "col2"));
    }

    [Fact]
    public void CovTest()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)");
        Assert.Equal(841, (int)df.Cov("col1", "col2"));
    }


    [Fact]
    public void CrossTabTest()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)").CrossTab("col1", "col2");
        df.Show();
    }

    [Fact]
    public void CubeTest()
    {
        var df = Spark.Sql("SELECT id-100 as Col1, id as Col2 from range(100)")
            .Cube("col1", "col2")
            .Agg(Count(Col("Col2")));

        df.Show();
    }

    [Fact]
    public void VersionTest()
    {
        var df = Spark.Version();
        Console.WriteLine($"SPARK Version: {df}");
    }

    [Fact]
    public void DistinctTest()
    {
        var df = Spark.Sql("SELECT 1 FROM RANGE(100)").Distinct();
        df.Show();
        Assert.Equal(1, df.Count());
    }

    [Fact]
    public void DropTest()
    {
        var df = Spark.Sql("SELECT 'a' as A, id from range(10)");
        df.Show();
        df = df.Drop(Col("A"));
        df.Show();
        Assert.DoesNotContain(df.Columns, s => s == "A" );
    }
    
    [Fact]
    public void DropDuplicatesTest()
    {
        var df = Spark.Sql("SELECT 'a' as A, id as B, id from range(10)");
        df.Show();
        df = df.DropDuplicates("A", "B");
        df.Show();
        df = df.DropDuplicates("A");
        df.Show();
        df = df.DropDuplicates();
        df.Show();
    }
    
    [Fact]
    public void DropNaTest()
    {
        var df = Spark.Sql("SELECT null as what, null as something from range(10)");
        df.Show();
        var na = df.DropNa("any", null, "what", "something");
        na.Show();
    }
    
    [Fact]
    public void DtypesTest()
    {
        var df = Spark.Sql("SELECT id as what, null as void from range(10)");
        var types = df.Dtypes;
        Assert.Equal(2, types.Count());
    }
    
    [Fact]
    public void ExceptAllTest()
    {
        var data = new List<IList<object>>() { new List<object>() { "a", 1 }, new List<object> { "a", 1 }, new List<object> { "a", 1 }, new List<object> { "a", 2 }, new List<object> { "b", 3 }, new List<object> { "c", 4 } };
        var schema = new StructType(new StructField("C1", StringType(), true), new StructField("C2", IntType(), true));
        var df1 = Spark.CreateDataFrame(data, schema);
        df1.Show();
        
        data = new List<IList<object>>() { new List<object>() { "a", 1 }, new List<object> { "b", 3 } };
        var df2 = Spark.CreateDataFrame(data, schema);
        df1.ExceptAll(df2).Show();
    }
    
    [Fact]
    public void FillNaTest()
    {
        var df = Spark.Sql("SELECT  cast(null as string) as null_string, cast(null as int) as null_int from range(10)");
        df.FillNa(Lit(100L)).Show();
        df.FillNa(Lit("not empty")).Show();
        
        df.FillNa(Lit(100L), "null_int").Show();
        df.FillNa(Lit(100L), "null_string").Show();
        
        df.FillNa(Lit("not empty"), "null_int").Show();
        df.FillNa(Lit("not empty"), "null_string").Show();
    }
    
    [Fact]
    public void FilterTest()
    {
        var df = Spark.Sql("SELECT  id from range(10)");
        df.Filter(Col("id") == 4).Show();
        df.Filter("id == 4").Show();
    }
    
    [Fact]
    public void TakeTest()
    {
        var df = Spark.Sql("SELECT  id from range(10)");
        var rows = df.Take(3);
        
        Assert.Equal(3, rows.Count());
    }
    
    [Fact]
    public void FirstTest()
    {
        var df = Spark.Sql("SELECT  id from range(10)");
        var row = df.First();
        Assert.Equal(0L, row[0]);
    }
    
    [Fact]
    public void FreqItemsTest()
    {
        var df = Spark.Sql("SELECT  id from range(10) union SELECT id from range(12) union SELECT id from range(8)");
        df.FreqItems("id").Show(truncate: 10000);
        df.FreqItems(0.123, "id").Show(truncate: 10000);
    }

    [Fact]
    public void HintTest()
    {
        var df = Spark.Sql("SELECT  id from range(10) union SELECT id from range(12) union SELECT id from range(8)");
        var explained = df.Hint("bradcast").Explain();
    }
    
    [Fact]
    public void InputFilesTest()
    {
        var path = Path.Join(Path.GetTempPath(), Path.GetRandomFileName());
        Spark.Range(1000).Write().Csv(path);

        foreach (var file in Spark.Read.Csv(path).InputFiles())
        {
            Console.WriteLine($"File: {file}");
        }
    }
    
    [Fact]
    public void IntersectTest()
    {
        var df1 = Spark.Range(10);
        var df2 = Spark.Range(122);
        df1.Intersect(df2).Show();
    }
    
    [Fact]
    public void IntersectAllTest()
    {
        var df1 = Spark.Range(10);
        var df2 = Spark.Range(122);
        df1.IntersectAll(df2).Show();
    }

    
    [Fact]
    public void IsLocalTest()
    {
        var df1 = Spark.Range(10);
        Assert.False(df1.IsLocal());

        var df2 = Spark.Sql("SHOW TABLES");
        Assert.True(df2.IsLocal());
    }
    
    [Fact]
    public void IsStreamingTest()
    {
        var df1 = Spark.Range(10);
        Assert.False(df1.IsStreaming());
    }
    
    [Fact]
    public void LimitTest()
    {
        var df1 = Spark.Range(10).Limit(4);
        Assert.Equal(4, df1.Count());
    }
    
    [Fact]
    public void GroupByTest()
    {
        var df1 = Spark.Range(5).WithColumn("name", Lit("ed")).Union(Spark.Range(3).WithColumn("name", Lit("bert"))).WithColumn("earnings", Lit(1234));
        df1.Show();
        var group = df1.GroupBy(Col("id"));
        group.Sum("earnings").Show();
        group.Min("earnings").Show();
        group.Max("earnings").Show();
        group.Count("earnings").Show();
        group.Mean("earnings").Show();
        group.Avg("earnings").Show();
    }
    
    [Fact]
    public void PivotTest()
    {
        var df1 = Spark.Range(5).WithColumn("name", Lit("ed")).Union(Spark.Range(3).WithColumn("name", Lit("bert"))).WithColumn("earnings", Lit(1234));
        df1.Show();
        var group = df1.GroupBy(Col("id")).Pivot("name", Lit("ed"), Lit("bert"), Lit("unknown"));
        group.Sum("earnings").Show();
        group.Min("earnings").Show();
        group.Max("earnings").Show();
        group.Count("earnings").Show();
        group.Mean("earnings").Show();
        group.Avg("earnings").Show();
    }
    
    [Fact]
    public void UnpivotTest()
    {
        var df1 = Spark.Range(5).WithColumn("name", Lit("ed")).Union(Spark.Range(3).WithColumn("name", Lit("bert"))).WithColumn("earnings", Lit(1234));
        df1.Show();
        var group = df1.GroupBy(Col("id")).Pivot("name", Lit("ed"), Lit("bert"), Lit("unknown"));
        group.Sum("earnings").Unpivot(new []{Col("id")}, new Column[]{Lit("ed"), Lit("bert")}).Show();
        group.Sum("earnings").Unpivot(new []{Col("id")}, new Column[]{Lit("ed"), Lit("bert")}, "variableColumn").Show();
        group.Sum("earnings").Unpivot(new []{Col("id")}, new Column[]{Lit("ed"), Lit("bert")}, "variableColumn", "valueColumn").Show();
    }
    
    [Fact]
    public void MeltTest()
    {
        var df1 = Spark.Range(5).WithColumn("name", Lit("ed")).Union(Spark.Range(3).WithColumn("name", Lit("bert"))).WithColumn("earnings", Lit(1234));
        var group = df1.GroupBy(Col("id")).Pivot("name", Lit("ed"), Lit("bert"), Lit("unknown"));
        group.Sum("earnings").Melt(new []{Col("id")}, new Column[]{Lit("ed"), Lit("bert")}).Show();
        group.Sum("earnings").Melt(new []{Col("id")}, new Column[]{Lit("ed"), Lit("bert")}, "variableColumn").Show();
        group.Sum("earnings").Melt(new []{Col("id")}, new Column[]{Lit("ed"), Lit("bert")}, "variableColumn", "valueColumn").Show();
    }
    
    [Fact]
    public void PrintSchemaTest()
    {
        Spark.Range(100).PrintSchema();
    }
    
    [Fact]
    public void RepartitionByRangeTest()
    {
        var df = Spark.CreateDataFrame(new List<IList<object>>()
        {
            new List<object>() { 56, "Tom" },
            new List<object>() { 123, "Bob" },
            new List<object>() { 23, "Alice" }
        }, new StructType(new StructField("age", new IntegerType(), true), new StructField("name", StringType(), true)));
        
        df.RepartitionByRange(2, Col("age")).Show();
    }
    
    [Fact]
    public void ReplaceTest()
    {
        var df = Spark.CreateDataFrame(new List<IList<object>>()
        {
            new List<object>() { 56, "Tom" },
            new List<object>() { 123, "Bob" },
            new List<object>() { 23, "Alice" }
        }, new StructType(new StructField("age", new IntegerType(), true), new StructField("name", StringType(), true)));
        
        df.Replace(Lit("Bob"), Lit("Rob")).Show();
        df.Replace(Lit("Bob"), Lit("Rob"), "name").Show();
        df.Replace(Lit(123L), Lit(999L), "age").Show();
    }
    
    [Fact]
    public void RollupTest()
    {
        var df = Spark.CreateDataFrame(new List<IList<object>>()
        {
            new List<object>() { 5, "Bob" },
            new List<object>() { 2, "Alice" }
        }, new StructType(new StructField("age", new IntegerType(), true), new StructField("name", StringType(), true)));

        df.Rollup("name", "age").Count().OrderBy("name", "age").Show();
        df.Rollup("name", "age").Count().OrderBy(Desc("count(1)")).Show();
        df.Rollup("name", "age").Count().Show();
    }
    
    [Fact]
    public void SampleTest()
    {
        var df = Spark.Range(10000);
        Assert.NotEqual(10000, df.Sample(fraction: 0.3F).Count());
    }
    
    [Fact]
    public void SampleByTest()
    {
        var df = Spark.Range(0, 100).Select((Col("id") % 3).Alias("key"));
        df = df.SampleBy(Col("key"), new Dictionary<int, double>() { { 0, 0.1 }, { 1, 0.2 } }, seed: 0);
        
        df.Show();
        Assert.Equal(11, df.Count());
        
    }

    [Fact] public void SelectExprTest()
    {
        var df = Spark.Range(0, 100);
        df.SelectExpr("id * 2", "abs(id)").Show();
    }
    
    [Fact] public void SortWithinPartitionsTest()
    {
        var df = Spark.Range(0, 100);
        df.SortWithinPartitions("id").Show();
    }
    
    [Fact] public void StorageLevelTest()
    {
        var df = Spark.Range(0, 100);
        var level = df.StorageLevel();
        Console.WriteLine(level);
    }
    
    [Fact] public void ToTest()
    {
        var df = Spark.Range(0, 100);
        var toSchema = new StructType(new StructField("id", ShortType(), false));
        df.To(toSchema).Show();
    }
    
    [Fact] public void WithColumnRenamedTest()
    {
        var df = Spark.Range(0, 100);
        df.WithColumnRenamed("id", "no longer id").Show();
    }
}