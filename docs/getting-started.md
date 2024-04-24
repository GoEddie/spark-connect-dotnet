# Getting Started

## Overview

1. Start the Spark Connect Server.
1. Specify the Spark Connect Server in your app.
1. Run Spark code like a winner.


### 1. Start Spark Connect Server

./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0

### 2. Specify the Spark Connect Server in your app

```csharp
var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();
```

### 3. Run Spark code like a winner

dotnet run