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

If you use this you can create your own `DataFrame` API wrappers for any functions you need.

There is also no one way or another, if for instance this library gives you everything you need apart from one function call then you can mix using this and making your own gRPC calls. This library intentionally makes everything that you would need to make the gRPC calls public so mixing in your own calls with this library is easy and is probably the best way to use this library, there will always be a new version of Spark and keeping up is going to be hard so this library is a good starting point but you should be prepared to make your own calls.

Because the gRPC objects are available you can do this:


```csharp
var spark = SparkSession.Builder.Profile("M1").DatabricksWaitForClusterMaxTime(2).GetOrCreate();
var dataFrame = spark.Sql("SELECT id, id as two, id * 188 as three FROM Range(10)");
var dataFrame2 = spark.Sql("SELECT id, id as two, id * 188 as three FROM Range(20)");

var dataFrame3 = dataFrame.Union(dataFrame2);
dataFrame3.Show(1000);

//Mix gRPC calls with DataFrame API calls - the gRPC  client is available on the SparkSession:
var plan = new Plan()
{
    Root = new Relation()
    {
        Sql = new SQL()
        {
            Query = "SELECT * FROM Range(100)"
        }
    }
};

var (relation, _, _) = await GrpcInternal.Exec(spark.GrpcClient, spark.Host, spark.SessionId, plan, spark.Headers, spark.UserContext, spark.ClientType);
var dataFrameFromRelation = new DataFrame(spark, relation);
dataFrameFromRelation.Show();
```
Output:

```shell
+---+---+-----+
| id|two|three|
+---+---+-----+
|  0|  0|    0|
|  1|  1|  188|
|  3|  3|  564|
|  2|  2|  376|
|  4|  4|  752|
|  6|  6| 1128|
|  5|  5|  940|
|  9|  9| 1692|
|  8|  8| 1504|
|  7|  7| 1316|
| 10| 10| 1880|
| 13| 13| 2444|
| 12| 12| 2256|
| 14| 14| 2632|
| 11| 11| 2068|
| 18| 18| 3384|
| 16| 16| 3008|
| 17| 17| 3196|
| 15| 15| 2820|
| 19| 19| 3572|
+---+---+-----+

+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
|  7|
|  8|
|  9|
+---+
only showing top 10 rows

```


### Inspiration

The documentation for Spark Connect is limited at best but there is an example implementation in [GO](https://github.com/apache/spark-connect-go) and that contains examples for many of the gRPC calls to Spark so has been extremely useful. So thanks go to the contributors over in Golang. 

### Getting Started

[Getting Started Guide](docs/getting-started.md)


### Writing .NET code for Apache Spark

[Dev Guide](docs/dev-guide.md)

### Deployment scenarios

1. [Databricks](https://the.agilesql.club/2024/01/using-spark-connect-from-.net-to-run-spark-jobs-on-databricks/)
1. Synapse Analytics - to be tested

### Major Features to be implemented

1. User Defined Functions

#### UDF Support

Spark 3.5 has added a lot of functions and I think the need for UDF support is getting less and less. With Spark Connect there is no way to create a .NET UDF and use it. There is the possibility that we can use lambda functions and pass those to Spark *but* they would be limited to any captured variables known at runtime and any functions on `Column`, it wouldn't be possible to call any .NET function at all so I am not sure how useful this would be. 

Another possibility is that we can allow people to write UDFs in Python and pass .NET the path and name of the function, we can serialize it and send it over to Spark (Spark Connect supports this) but it would mean writing in Python.

Probably the easiest thing is to allow both options (remember you don't need this library to make the gRPC calls, you can do it yourself) but it would be easier if we provide a nice way to do it.

### Current Function Status

To see how many functions are/are not implemented see [Function Status](docs/function-status.md)

### Optional Features to be implemented

1. TPC Benchmark

### Questions

1. Are Databricks/Apache Spark communities invested in the future of gRPC / Spark Connect or could it be dropped in future?
1. Can the gRPC calls be used in production?
1. What is the notebook experience expected to be in Databricks - will Databricks ever deploy other kernels or will it only ever be python,scala,sql,r?
