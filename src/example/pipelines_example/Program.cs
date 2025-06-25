
using pipelines_example;
using Spark.Connect.Dotnet.Pipelines;
using Spark.Connect.Dotnet.Sql;


var spark = SparkSession
    .Builder
    .Remote("sc://127.0.0.1:15002")
    .GetOrCreate();




spark.Conf.Set("spark.connect.dotnet.grpclogging", "pipeline");
spark.Sql("CREATE DATABASE IF NOT EXISTS Bronze");
spark.Sql("CREATE DATABASE IF NOT EXISTS Silver");
spark.Sql("CREATE DATABASE IF NOT EXISTS Gold");


var runner = new PipelineRunner();
runner.Run([typeof(BooksPipeline), typeof(BooksSilverPipeline), typeof(BooksGoldPipeline)], spark);

var bookDetails = spark.Read.Table("Silver.BookDetails");
bookDetails.PrintSchema();
bookDetails.Show();


var bookAveragePricePerLocation = spark.Read.Table("Gold.BookAveragePricePerLocation");
bookAveragePricePerLocation.PrintSchema();
bookAveragePricePerLocation.Show();


var bookAveragePricePerAuthor = spark.Read.Table("Gold.BookAveragePricePerAuthor");
bookAveragePricePerAuthor.PrintSchema();
bookAveragePricePerAuthor.Show();

var booksPerAuthorPerYear = spark.Read.Table("Gold.BooksPerAuthorPerYear");
booksPerAuthorPerYear.PrintSchema();
booksPerAuthorPerYear.Show();