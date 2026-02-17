## CreateDataFrame

There are various ways of passing data to `CreateDataFrame` they all convert the data to the Arrow format and upload the data but the preferred wway is to pass an enumerable of typed tuple:


```csharp

var testScoresData = new List<(int, int, DateTime)>()
{
    (1, 97, DateTime.Today),
    (2, 99, DateTime.Today),
    (3, 98, DateTime.Today)
};

var testScoreSchema = new  StructType(
    new StructField("id", new IntegerType(), false),
    new StructField("score", new IntegerType(), false),
    new StructField("date", new DateType(), false)
    );

var testScores = spark.CreateDataFrame(testScoresData.Cast<ITuple>(), testScoreSchema);
testScores.Show();

```

I would like to add a version of `CreateDataFrame` that works in a similar way to Dapper.NET so you would pass a list of a specific type like `CreateDataFrame<TestScore>()` and it would automatically map the properties to the schema but I haven't implemented that yet.