using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests;

public class ReadWriteTests : E2ETestBase
{
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
}

public class E2ETestBase
{
    protected readonly SparkSession Spark;
    protected readonly string OutputPath;

    public E2ETestBase()
    {
        var remoteAddress = Environment.GetEnvironmentVariable("SPARK_REMOTE") ?? "http://localhost:15002";
        Spark = SparkSession.Builder.Remote(remoteAddress).GetOrCreate();
        
        var tempFolder = Path.GetTempPath();

        OutputPath = Path.Join(tempFolder, "spark-connect-tests");
    }
}