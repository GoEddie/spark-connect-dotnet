using delta_lake_example;
using Spark.Connect.Dotnet.Sql;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

const string deltaPath = "/tmp/delta/lake";
const string csvPath = "/tmp/delta/lake-csv";

spark.Range(100).Write().Mode("overwrite").Format("delta").Write(deltaPath);
spark.Range(100).Write().Mode("overwrite").Format("csv").Write(csvPath);


// new ProtoExample().Run(spark, deltaPath, csvPath);
new APIExample().Run(spark, deltaPath, csvPath);