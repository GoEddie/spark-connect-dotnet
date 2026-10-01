# Getting Started

This example uses .NET 8, Java 17, and a local Apache Spark 4.0.0 installation.

## 1. Start Spark Connect

From the directory containing the unpacked `spark-4.0.0-bin-hadoop3` distribution, run:

```sh
./sbin/start-connect-server.sh --master 'local[2]' --packages org.apache.spark:spark-connect_2.13:4.0.0
```

The server listens on port 15002. The initial startup may download Maven dependencies.

## 2. Create a .NET application

```sh
dotnet new console --framework net8.0 --name SparkExample
cd SparkExample
dotnet add package GOEddie.Spark.Dotnet --prerelease
```

The package versions use the `-build.N` suffix, so NuGet treats them as prereleases.
The main package includes the matching gRPC client package as a dependency.

Replace `Program.cs` with:

```csharp
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

var spark = SparkSession.Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

var rows = spark.Range(10)
    .WithColumn("message", Lit("Hello from .NET"));

rows.Show();
```

## 3. Run the application

```sh
dotnet run
```

You should see ten rows with an `id` and a `message` column.
When finished, stop the server from the Spark installation directory:

```sh
./sbin/stop-connect-server.sh
```

See [connection options](connection-options.md) for other connection settings and
[supported Spark versions](supported-spark-versions.md) for compatibility details.
