# spark-connect-dotnet

## Overview

Apache Spark 3.4 included a new API to connect and interact with Apache Spark which is called the `Spark Connector`, this was announced and described as a way to bring Apache Spark to all languages, not just Python, Scala, R, and Spark SQL. For full details of Spark Connect visit: https://spark.apache.org/docs/latest/spark-connect-overview.html

### .NET for Apache Spark

There was already an existing project called .NET for Apache Spark which was a way for .NET applications to connect to Apache Spark. This was created and managed by Microsoft and was an open source project which took in outside contributions. The project was dropped by Microsoft and is no longer maintained. However, all is not lost, because of Spark Connect we can use Grpc to connect to Apache Spark from any .NET langauge [VB.NET anyone?](src/example/basic_example_vb/Program.vb).

The main problem with the original .NET for Apache Spark was that every version of Spark required a new custom built Jar to be built and maintained and when you connected to Spark you had to ensure your versions matched or you would get errors and nothing would work. This was a bit of a pain but with the new Grpc interface we get none of these issues.

### Inspiration

The documentation for Spark Connect is limited at best but there is an example implementaion in [GO](https://github.com/apache/spark-connect-go) and that contains examples for many of the Grpc calls to Spark so has been extremely useful. So thanks go to the contributors over in Golang. 

### Getting Started

[Getting Started Guide](docs/getting-started.md)


### Writing .NET code for Apache Spark

[Dev Guide](docs/dev-guide.md)


### Major Features to be implemented

1. All `spark.sql.functions.*` Functions
1. All Data Reader/Writer Functions
1. UDF's

