# Options

As far as possible the goal is to align with the PySpark API and its behaviour. There are times when we want to do something different, for example: https://github.com/GoEddie/spark-connect-dotnet/issues/12 where the bevaviour is the same as the PySpark API but we want to provide some more help by validating the name of the column passed to DataFrame["columnName"] so I have implemented it but put it behind an option. Any options will be documented here.


## Validate Column name on DataFrame indexer

If you use the Dataframe indexer, by default the name is not checked against the list of names. If you would like us to validate the name of the column (it will cause an AnalyzePlan request) then you can either enable this option so that it effects all DataFrames or you can enable it on a per DataFrame basis.

Enable globally:

```csharp
spark.Conf.Set("spark.connect.dotnet.validatethiscallcolumnname", "true");
```

or you can set it when building the session:

```csharp
SparkSession
    .Builder
    .Remote(RemotePath)
    .Config("spark.connect.dotnet.validatethiscallcolumnname", "true")
    .GetOrCreate();
```

if you have set it and want to disable it globally then:

```csharp
spark.Conf.Set("spark.connect.dotnet.validatethiscallcolumnname", "false");
```

If you do not have it enabled globally but want it enabled on one specific DataFrame then you can do it like this:

```csharp
var df = spark.Range(100);

var col = df["NotID"];    //This will NOT throw an exception, when Select or another action are called it will fail then
df.ValidateThisCallColumnName = true;
var col = df["NotID"];    //This will throw an exception
```


## Don't decode Apache Arrow

If you get issues when running a command and you do not need the actual response to be decoded from Apache Arrow then you can disable it. This is only really used where the decoding has either not been implemented or there is an issue that needs to be fixed:

```csharp
spark.Conf.Set("spark.connect.dotnet.dontdecodearrow", "true");
```

It is envisioned that you should only use this for specific queries and not as a general option.


## Detailed gRPC logging 

To enable detailed gRPC logging you can set the following option:

```csharp
spark.Conf.Set("spark.connect.dotnet.grpclogging", "console");
```

## Show metrics

To output metrics as responses are reveived enable:

```csharp
spark.Conf.Set("spark.connect.dotnet.showmetrics", "true");
```

## Request Wait Timeout

When we send a request to the server, we need to kill the connection and retry after X seconds because if we have an idle connection then Azure Databricks will kill the tcp connection. To handle this we send a request, wait x seconds and if we haven't completed the response we re-connect to the running query and keep re-connecting every x seconds.

To change how long we wait before killing the connection and re-connecting you can set:

```csharp
spark.Conf.Set("spark.connect.dotnet.requestretrytimelimit", "30");
```

The setting is the amount of seconds, it defaults to 45. It isn't really needed for Spark 4 as it has a sort of keep alive where it sends a response even while the server is busy doing something.