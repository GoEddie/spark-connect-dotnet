using Spark.Connect.Dotnet.Pipelines.Attributes;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Tests.Pipelines;

[DeclarativePipeline(DefaultDatabase = "dltfun")]
public class ExampleDeclarativePipeline()
{
    [SqlConfFor(Name = "MyPipelineDefinedWithAttributes")]
    public Dictionary<string, string> MyPipelineDefinedWithAttributesSqlConf()
    {
        return new Dictionary<string, string>()
        {
            {"spark.sql.ansi.enabled", "true"}
        };
    }
    
    [PipelineTable(Format = "json")]
    public Dotnet.Sql.DataFrame ADifferentHiveFromat(SparkSession spark)
    {
        var df = spark.Range(100).WithColumn("table", Functions.Lit("I AM IN A DIFFERENT SCHEMA")).WithColumnRenamed("id", "ABC");
        df.PrintSchema();
        return df;
    }

    [PipelineTable(Name = "ADifferentStructSchema", Comment = "a schema defined in code")]
    public Dotnet.Sql.DataFrame MyFirstTable(SparkSession spark)
    {
        var df = spark.Range(100).WithColumn("table", Functions.Lit("one")).WithColumnRenamed("id", "ABC");
        df.PrintSchema();
        return df;
    }
    
    [SchemaFor(Name = "ADifferentStructSchema")]
    public StructType MyFirstTableSchema() => new StructType(
        new[]
        {
            new StructField("ABC", new IntegerType(), false), 
            new StructField("table", new StringType(), false)
        });

    [TableOptionsFor(Name = "ADifferentStructSchema")]
    public Dictionary<string, string> MyFirstTableProperties()
    {
        return new Dictionary<string, string>()
        {
            {"created.by.user", "GOEddie"},
            {"created.when", "1980"}
        };
    }
    
    [SqlConfFor(Name = "ADifferentStructSchema")]
    public Dictionary<string, string> MyFirstTableSqlConf()
    {
        return new Dictionary<string, string>()
        {
            {"spark.sql.ansi.enabled", "false"}
        };
    }
    
    
    [PipelineTable()]
    public Dotnet.Sql.DataFrame MyPartitionedTable(SparkSession spark)
    {
        var df = spark.Range(100).WithColumn("year", Functions.Lit(1980) + Functions.Col("id")).WithColumn("NewCol", Functions.Rand()).Repartition(Column.Col("year"));
        df.PrintSchema();
        return df;
    }
    
    
    [PipelineTable(Comment = "This is a comment on a table")]
    public Dotnet.Sql.DataFrame AnotherTable(SparkSession spark)
    {
        var df = spark.Range(100).WithColumn("AnotherTable", Functions.Lit("one")).WithColumnRenamed("id", "ABC");
        df.PrintSchema();
        return df;
    }
    
    [PipelineTable()]
    public Dotnet.Sql.DataFrame MySecondTable(SparkSession spark)
    {
        return spark.Range(100).WithColumn("table", Functions.Lit("two"));
    }
    
    [PipelineTable(Name = "ThirdTable")]
    public Dotnet.Sql.DataFrame MyThirdTable(SparkSession spark)
    {
        var dfOne = spark.Read.Table("ADifferentStructSchema");
        var dfTwo = spark.Read.Table("MySecondTable");
        
        return dfOne.Union(dfTwo).Filter(Functions.Col("ABC") > 10).GroupBy(Functions.Col("table")).Agg(Functions.Count(Functions.Col("ABC")));
    }
    
    [PipelineMaterializedView(Name = "MatViewOne")]
    public Dotnet.Sql.DataFrame MyMatViewOne(SparkSession spark)
    {
        
        var df = spark.Range(10000);
        df = df.WithColumn("ABC", Functions.Lit("ABC"));
        df = df.Select(Functions.Col("ABC")).Distinct();
        df.Show();
        return df.Limit(1);
    }
    
    [PipelineMaterializedView(Name = "TempViewOne")]
    public Dotnet.Sql.DataFrame T1(SparkSession spark)
    {
        return spark.Read.Table("ADifferentStructSchema");
    }
}