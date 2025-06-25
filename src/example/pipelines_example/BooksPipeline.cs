using Spark.Connect.Dotnet.Pipelines.Attributes;
using Spark.Connect.Dotnet.Sql;
using F=Spark.Connect.Dotnet.Sql.Functions;

namespace pipelines_example;

[DeclarativePipeline(DefaultDatabase = "Bronze")]
public class BooksPipeline
{
    private readonly string _filePath;
    
    public BooksPipeline()
    {
        _filePath = Path.GetFullPath(Path.Join("sampledata", "books_raw.json"));
    }
    
    //BRONZE
    [PipelineTable(Format = "parquet")]
    public DataFrame BooksRaw(SparkSession spark)
    {
        return spark
            .Read
            .Option("multiLine", "true")
            .Json(_filePath)
            .Select(
                    F.Explode(F.Col("books"))
                )
            .Select(
                    F.Expr("col.*")
                );
    }
    
    [PipelineTable(Format = "parquet")]
    public DataFrame AuthorsRaw(SparkSession spark)
    {
        return spark
            .Read
            .Option("multiLine", "true")
            .Json(_filePath)
            .Select(
                F.Explode(F.Col("authors"))
            )
            .Select(
                F.Expr("col.*")
            );
    }
    
    
    [PipelineTable(Format = "parquet")]
    public DataFrame PublishersRaw(SparkSession spark)
    {
        return spark
            .Read
            .Option("multiLine", "true")
            .Json(_filePath)
            .Select(
                F.Explode(F.Col("publishers"))
            )
            .Select(
                F.Expr("col.*")
            );
    }
    
}

[DeclarativePipeline(DefaultDatabase = "Silver")]
public class BooksSilverPipeline
{
    [TableOptionsFor(Names = new[] { "BookDetails" })]
    public IDictionary<string, string> BooksSilverPipelineOptions()
    {
        return new Dictionary<string, string>()
        {
            {"spark.sql.hive.convertMetastoreParquet.mergeSchema", "true"}
        };
    }
    
     [PipelineMaterializedView(Format = "parquet")]
     public DataFrame BookDetails(SparkSession spark)
     {
         var books = spark.Read.Table("Bronze.BooksRaw").Alias("books").WithColumnRenamed("id", "book_id");
         var authors = spark.Read.Table("Bronze.AuthorsRaw").Alias("authors").WithColumnRenamed("id", "author_id");
         var publishers = spark.Read.Table("Bronze.PublishersRaw").Alias("pubs").WithColumnRenamed("id", "pub_id");
    
         var bookDetails = books
                                         .Join(authors, books["author_id"] == authors["author_id"])
                                         .Join(publishers, books["pub_id"] == publishers["pub_id"])
                                         .Select(F.Expr("books.*")
                                                 ,authors["name"].Alias("author_name")
                                                 ,authors["nationality"]
                                                 ,publishers["name"].Alias("publisher_name")
                                                 ,publishers["location"].Alias("pub_location")
                                                 );
         
         return bookDetails;
     }
    
}

[DeclarativePipeline(DefaultDatabase = "Gold")]
public class BooksGoldPipeline
{
    [TableOptionsFor(Names = ["BookAveragePricePerLocation", "BookAveragePricePerAuthor", "BooksPerAuthorPerYear"])]
    public Dictionary<string, string> GoldProperties()
    {
        return new Dictionary<string, string>()
        {
            {"created.by.user", "GOEddie"},
            {"created.when", "1980"}
        };
    }
    
    [SqlConfFor(Names = ["BookAveragePricePerLocation", "BookAveragePricePerAuthor", "BooksPerAuthorPerYear"])]
    public Dictionary<string, string> GoldConf()
    {
        return new Dictionary<string, string>()
        {
            {"spark.sql.ansi.enabled", "false"}
        };
    }

    [PipelineMaterializedView(Format = "parquet")]
    public DataFrame BookAveragePricePerLocation(SparkSession spark)
    {
        var books = spark.Read.Table("Silver.BookDetails");
        var averages = books.GroupBy(F.Col("pub_location")).Agg(F.Avg(F.Col("price")).Alias("avg_price"));
        return averages;
    }
    
    [PipelineMaterializedView(Format = "parquet")]
    public DataFrame BookAveragePricePerAuthor(SparkSession spark)
    {
        var books = spark.Read.Table("Silver.BookDetails");
        var averages = books.GroupBy(F.Col("author_id")).Agg(F.Avg(F.Col("price")).Alias("avg_price"));
        return averages;
    }
    
    [PipelineMaterializedView(Format = "parquet")]
    public DataFrame BooksPerAuthorPerYear(SparkSession spark)
    {
        var books = spark.Read.Table("Silver.BookDetails");
        var averages = books.GroupBy(F.Col("author_id"), F.Col("publication_year")).Agg(F.Count(F.Col("author_id")).Alias("count"));
        return averages;
    }
}