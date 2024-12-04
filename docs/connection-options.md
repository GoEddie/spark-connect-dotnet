# Connection Options

When connecting to a Databricks cluster, it might be that the cluster is in the starting state so I have added some code to sit and poll the spark server until the cluster is available.

There are two options that help configure this and I added the options to the `SparkSessionBuilder`.

## DatabricksWaitForClusterMaxTime

This controls how long we will sit and wait for the cluster to start:

```csharp
var spark = 
        SparkSession
        .Builder
        .DatabricksWaitForClusterMaxTime(5)
        .GetOrCreate();
```

## DatabricksWaitForClusterOnSessionCreate

The second option disables the behaviour completely:

```csharp
var spark = 
        SparkSession
        .Builder
        .DatabricksWaitForClusterOnSessionCreate(false)
        .GetOrCreate();
```
