using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests;

public class ReadWriteTests : E2ETestBase
{
    
    public ReadWriteTests(ITestOutputHelper logger) : base(logger)
    {
        
    }
    
    [Fact]
    public async Task Read_And_Write_Json()
    {
        var df = Spark.Range(10)
            .WithColumn("double_col", Lit(10.0))
            .WithColumn("bool_col", Lit(false))
            .WithColumn("string_col", Lit("hello friend"));

        var path = Path.Join(OutputPath, "json");
        df.Write().Mode("overwrite").Format("json").Write(path);

        var df2 = Spark.Read.Json(path);
        var result = await df2.CollectAsync();

        Assert.Equal(10, result.Count);
        Assert.Equal(false, result[0][0]);
        Assert.Equal(10.0, result[0][1]);
        Assert.Equal(0L, result[0][2]);
        Assert.Equal("hello friend", result[0][3]);
    }

    [Fact]
    public async Task Read_And_Write_Parquet()
    {
        var df = Spark.Range(10)
            .WithColumn("double_col", Lit(10.0))
            .WithColumn("bool_col", Lit(false))
            .WithColumn("string_col", Lit("hello friend"));

        var path = Path.Join(OutputPath, "parquet");
        df.Write().Mode("overwrite").Format("parquet").Write(path);

        var df2 = Spark.Read.Parquet(path);
        var result = await df2.CollectAsync();

        Assert.Equal(10, result.Count);
        Assert.Equal(0L, result[0][0]);
        Assert.Equal(10.0, result[0][1]);
        Assert.Equal(false, result[0][2]);
        Assert.Equal("hello friend", result[0][3]);
    }

    [Fact]
    public async Task Read_And_Write_Orc()
    {
        var df = Spark.Range(10)
            .WithColumn("double_col", Lit(10.0))
            .WithColumn("bool_col", Lit(false))
            .WithColumn("string_col", Lit("hello friend"));

        var path = Path.Join(OutputPath, "orc");
        df.Write().Mode("overwrite").Format("orc").Write(path);

        var df2 = Spark.Read.Orc(path);
        var result = await df2.CollectAsync();

        Assert.Equal(10, result.Count);
        Assert.Equal(0L, result[0][0]);
        Assert.Equal(10.0, result[0][1]);
        Assert.Equal(false, result[0][2]);
        Assert.Equal("hello friend", result[0][3]);
    }

    [Fact]
    public async Task Read_And_Write_Csv()
    {
        var df = Spark.Range(10)
            .WithColumn("double_col", Lit(10.0))
            .WithColumn("bool_col", Lit(false))
            .WithColumn("string_col", Lit("hello friend"));

        var path = Path.Join(OutputPath, "csv");
        df.Write().Mode("overwrite").Format("csv").Write(path);

        var df2 = Spark.Read.Csv(path);
        var result = await df2.CollectAsync();

        Assert.Equal(10, result.Count);
        Assert.Equal("0", result[0][0]);
        Assert.Equal("10.0", result[0][1]);
        Assert.Equal("false", result[0][2]);
        Assert.Equal("hello friend", result[0][3]);
    }

    [Fact]
    public async Task Read_And_Write_Csv_With_Types()
    {
        var df = Spark.Range(10)
            .WithColumn("double_col", Lit(10.0))
            .WithColumn("bool_col", Lit(false))
            .WithColumn("string_col", Lit("hello friend"));

        var path = Path.Join(OutputPath, "csv");
        df.Write().Mode("overwrite").Format("csv").Write(path);

        var df2 = Spark.Read.Option("inferSchema", "true").Option("header", "false").Csv(path);
        var result = await df2.CollectAsync();

        Assert.Equal(10, result.Count);
        Assert.Equal(10.0, result[0][1]);
        Assert.Equal(false, result[0][2]);
        Assert.Equal("hello friend", result[0][3]);
    }

    [Fact(Skip = "Needs Avro deployed to server")]
    public async Task Read_And_Write_Avro()
    {
        var df = Spark.Range(10)
            .WithColumn("double_col", Lit(10.0))
            .WithColumn("bool_col", Lit(false))
            .WithColumn("string_col", Lit("hello friend"));

        var path = Path.Join(OutputPath, "avro");
        df.Write().Mode("overwrite").Format("avro").Write(path);

        var df2 = Spark.Read.Format("avro").Load(path);
        var result = await df2.CollectAsync();

        Assert.Equal(10, result.Count);
        Assert.Null(result[0][0]);
        Assert.Equal(10.0, result[0][1]);
        Assert.Equal(false, result[0][2]);
        Assert.Equal("hello friend", result[0][3]);
    }

    [Fact]
    public async Task Read_Table()
    {
        var df = Spark.Range(10)
            .WithColumn("double_col", Lit(10.0))
            .WithColumn("bool_col", Lit(false))
            .WithColumn("string_col", Lit("hello friend"));

        df.CreateOrReplaceTempView("read_table");

        var df2 = Spark.Table("read_table");

        var result = await df2.CollectAsync();

        Assert.Equal(10, result.Count);
        Assert.Equal(10.0, result[0][1]);
        Assert.Equal(false, result[0][2]);
        Assert.Equal("hello friend", result[0][3]);
    }
    
}