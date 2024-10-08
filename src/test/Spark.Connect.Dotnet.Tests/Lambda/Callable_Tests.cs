using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;
using Column = Spark.Connect.Dotnet.Sql.Column;

namespace Spark.Connect.Dotnet.Tests.Lambda;

public class Callable_Tests : E2ETestBase
{
    public Callable_Tests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
    }

    [Fact]
    public void Test_ArraySort()
    {
        var zero = 99;
        var df = Spark.Sql("SELECT array(\"foo\", \"foobar\", NULL, \"bar\") as data");
        df.Select(
                ArraySort("data", (x, y) => When(x.IsNull() | y.IsNull(), Lit(zero)).Otherwise(Length(y) - Length(x))))
            .Show();
    }

    [Fact]
    public void Test_ForAll()
    {
        var inputToCheckFor = "foo";

        var df = Spark.Sql(@"SELECT 1 as key, array('bar') as values
                                        UNION
                                       SELECT 2 as key, array('foo', 'bar') as values
                                        UNION
                                       SELECT 3 as key, array('foobar', 'foo') as values                                        

");
        df.Select(ForAll("values", column => column.RLike(inputToCheckFor)).Alias("all_foo")).Show();
    }

    [Fact]
    public void Transform_Unary_Test()
    {
        var multiplier = DateTime.Now.Millisecond;
        var df = Spark.Sql(@"SELECT 1 key, array(1, 2, 3, 4) values");
        df.Select(Col("key"), Col("values"), Transform("values", x => x * multiplier)).Show();
    }

    [Fact]
    public void Transform_Binary_Test()
    {
        var df = Spark.Sql(@"SELECT 1 key, array(1, 2, 3, 4) values");
        df.Select(Col("key"), Col("values"), Transform("values", (x, i) => When(i % 2 == 0, x).Otherwise(Lit(0) - x)))
            .Show();
    }

    [Fact]
    public void Exists_Test()
    {
        var df = Spark.Sql(@"SELECT 1 key, array(1, 2, 3, 4) values union SELECT 2 key, array(1, -3, 4) ");
        df.Select(Col("key"), Col("values"), Exists("values", x => x < 0).Alias("any_negative")).Show();
    }

    [Fact]
    public void Filter_Unary_Test()
    {
        var df = Spark.Sql(
            @"SELECT 1 key, array(""2018-09-20"",  ""2019-02-03"", ""2019-07-01"", ""2020-06-01"") values");
        df.Select(Col("key"), Col("values"),
                Filter("values", amazingColumnOfFun => Month(ToDate(amazingColumnOfFun, null)) > 6))
            .Show(vertical: true, truncate: 50);
    }

    [Fact]
    public void Filter_Binary_Test()
    {
        var df = Spark.Sql(
            @"SELECT 1 key, array(""2018-09-20"",  ""2019-02-03"", ""2019-07-01"", ""2020-06-01"") values");
        df.Select(Col("key"), Col("values"),
                Filter("values", (a, b) => ((Month(ToDate(a, null)) > 2) | (b == 1)) & (b > 999)).Alias("filturd"))
            .Show(vertical: true, truncate: 50);
        df.Select(Col("key"), Col("values"), Filter("values", (a, b) => (b == 1) & (b < 10)).Alias("filturd"))
            .Show(vertical: true, truncate: 50);
    }

    [Fact]
    public void AggregateBinary_Callable_Test()
    {
        var df = Spark.Sql("SELECT 1 id, array(20.0, 4.0, 2.0, 6.0, 10.0) values");

        df.Select(Aggregate(
                Col("values"),
                Struct(Lit(0).Alias("count"), Lit(0.0).Alias("sum")),
                (acc, x) => Struct((acc["count"] + 1).Alias("count"), (acc["sum"] + x).Alias("sum")),
                acc => acc["sum"] / acc["count"]).Alias("mean")
        ).Show();
    }

    [Fact]
    public void AggregateUnary_Callable_Test()
    {
        var df = Spark.Sql("SELECT 1 id, array(20.0, 4.0, 2.0, 6.0, 10.0) values");

        df.Select(Aggregate(
                Col("values"),
                Lit(0.0),
                (acc, x) => acc + x).Alias("sum")
        ).Show();
    }

    [Fact]
    public void Zipwith_Test()
    {
        var df = Spark.Sql("SELECT 1 id, array(1, 3, 5, 8) xs, array(0, 2, 4, 6) ys");

        df.Select(
            ZipWith(Col("xs"), Col("ys"), (x, y) => x.Pow(y)).Alias("powers")
        ).Show(10, 1000, true);
    }

    [Fact]
    public void TransformKeys_Test()
    {
        var df = Spark.Sql("SELECT 1 as id, map('foo', 2.0, 'bar', 2.0) as data");

        df.Select(
            TransformKeys(Col("data"), (k, _) => Upper(k)).Alias("data_upper")).Show(
            10, 1000, true);
    }

    [Fact]
    public void TransformValues_Test()
    {
        var df = Spark.Sql("SELECT 1 as id, map(\"IT\", 10.0, \"SALES\", 2.0, \"OPS\", 24.0) as data");

        df.Select(
                TransformValues(Col("data"),
                    (k, v) => When(k.IsIn("IT", "OPS"), v + 1000.0).Otherwise(v)
                ).Alias("new_data"))
            .Show(
                10, 100, true);
    }

    [Fact]
    public void MapFilter_Test()
    {
        var df = Spark.Sql("SELECT 1 as id, map(\"foo\", 42.0, \"bar\", 1.0, \"baz\", 32.0) as data");

        df.Select(
                MapFilter(Col("data"),
                    (_, v) => v > 30.0
                ).Alias("data_filtered"))
            .Show(
                10, 100, true);
    }

    [Fact]
    public void MapZipWith_Test()
    {
        var df = Spark.Sql(
            "SELECT 1 as id, map(\"IT\", 24.0, \"SALES\", 12.00) as base, map(\"IT\", 2.0, \"SALES\", 1.4) ratio");

        df.Select(
                MapZipWith(Col("base"), Col("ratio"),
                    (_, v1, v2) => Round(v1 * v2, 2)
                ).Alias("updated_data"))
            .Show(
                10, 100, true);
    }

    private static Column Merge(Column acc, Column x)
    {
        var count = acc["count"] + 1;
        var sum = acc["sum"] + x;

        return Struct(count.Alias("count"), sum.Alias("sum"));
    }

    [Fact]
    public void Reduce_Test()
    {
        var df = Spark.Sql("SELECT 1 id, array(20.0, 4.0, 2.0, 6.0, 10.0) values");

        df.Select(
                Reduce("values", Lit(0.0), (acc, x) => acc + x).Alias("sum")
            )
            .Show(
                10, 100, true);

        var df2 = df.Select(
            Reduce("values",
                Struct(Lit(0).Alias("count"), Lit(0.0).Alias("sum")),
                (a, b) => Merge(a, b),
                column => column["sum"] / column["count"]).Alias("mean")
        );

        df2.Show();
        Logger.WriteLine(df2.Relation.ToString());
    }
}