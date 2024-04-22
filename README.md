# spark-connect-dotnet

## Overview

Apache Spark 3.4 included a new API to connect and interact with Apache Spark which is called the `Spark Connector`, this was announced and described as a way to bring Apache Spark to all languages, not just Python, Scala, R, and Spark SQL. For full details of Spark Connect visit: https://spark.apache.org/docs/latest/spark-connect-overview.html

### .NET for Apache Spark

There was already an existing project called .NET for Apache Spark which was a way for .NET applications to connect to Apache Spark. This was created and managed by Microsoft and was an open source project which took in outside contributions. The project was dropped by Microsoft and is no longer maintained. However, all is not lost, because of Spark Connect we can use gRPC to connect to Apache Spark from any .NET langauge [VB.NET anyone?](src/example/basic_example_vb/Program.vb).

The main problem with the original .NET for Apache Spark was that every version of Spark required a new custom built Jar to be built and maintained and when you connected to Spark you had to ensure your versions matched or you would get errors and nothing would work. This was a bit of a pain but with the new gRPC interface we get none of these issues.

### What happens if this project is dropped?

It doesn't matter, take the code and write your gRPC calls, you don't need the nuget you just need the proto files. This project includes some helpers to make it easier to make the gRPC calls and to mimic the DataFrame API so you can write your spark code like this:


```csharp
var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

var dataFrame = spark.Range(1000);
dataFrame.Show();

var dataFrame2 = dataFrame
    .WithColumn("Hello",
        Lit("Hello From Spark"));

dataFrame2.Write().Json(jsonPath);

var dataFrame3 = spark.Read().Json(jsonPath);
dataFrame3.Show();
```

but there is nothing stopping you from using the proto files and making the calls directly, something like:


```csharp
var showStringPlan = new Plan
{
    Root = new Relation
    {
        ShowString = new ShowString
        {
            Truncate = truncate, Input = input, NumRows = numberOfRows, Vertical = vertical
        }
    }
};

var executeRequest = new ExecutePlanRequest
{
    Plan = showStringPlan, SessionId = sessionId
};

client.ExecutePlan(executeRequest, new Metadata());
```

You will need to build a plan for each of the calls you want to make but the details are all in this repo and the referenced Golang implementation.

### Inspiration

The documentation for Spark Connect is limited at best but there is an example implementaion in [GO](https://github.com/apache/spark-connect-go) and that contains examples for many of the gRPC calls to Spark so has been extremely useful. So thanks go to the contributors over in Golang. 

### Getting Started

[Getting Started Guide](docs/getting-started.md)


### Writing .NET code for Apache Spark

[Dev Guide](docs/dev-guide.md)


### Deployment scenarios to be tested

1. Local on Windows
1. Databricks
1. Synapse Analytics


### Major Features to be implemented

1. All `spark.sql.functions.*` Functions
1. All Data Reader/Writer Functions
1. UDF's
1. Streaming
1. Debugging jobs (this was one of the original benefits of spark connect)

### Optional Features to be implemented

1. DataLake
1. TPC Benchmark

### Questions

1. Are Databricks/Apache Spark communities invested in the future of gRPC / Spark Connect or could it be dropped in future?
1. Can the gRPC calls be used in production?
1. What is the notebook experience expected to be in Databricks - will Databricks ever deploy other kernels or will it only ever be python,scala,sql,r?
