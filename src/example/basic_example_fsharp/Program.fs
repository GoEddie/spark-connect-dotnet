
open Spark.Connect.Dotnet.Sql

let spark = SparkSession.Builder.Remote("http://localhost:15002").GetOrCreate()
let dataFrame = spark.Range(100)

dataFrame.Show()

dataFrame.WithColumn("source", Functions.Lit("f sharp!")).Show()