using Apache.Arrow;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;
using f=Spark.Connect.Dotnet.Sql.Functions;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

spark.Conf.Set(SparkDotnetKnownConfigKeys.DecodeArrowType, "ArrowBuffers");

void SingleInt32Column()
{
    Console.WriteLine("Writing Arrow Batches to Spark:");
    var builder = new Int32Array.Builder();
    builder.Append(1);
    builder.AppendNull();
    builder.Append(3);
    builder.AppendNull();
    builder.Append(5);
    
    var array = builder.Build();
    var metaData = new List<KeyValuePair<string, string>>(){{new("test", "metadata")}};
    var schema = new Schema(new List<Field>() { new Field("test", new Int32Type(), true) }, metaData);
    var batch = new RecordBatch(schema, [array], 5);
    
    var df = spark.CreateDataFrame(batch, schema);
    df.Show();
    df.PrintSchema();

    var arrowBatches = df.CollectAsArrowBatch();
    
    Console.WriteLine("Reading Arrow Batches back from Spark:");
    var data = new List<int>();
    foreach (var recordBatch in arrowBatches)
    {
        foreach (var column in recordBatch.Arrays)
        {
            var int32Array = (column as Int32Array);
            
            for (var i = 0; i < column.Length; i++)
            {
                Console.WriteLine($"row: {i} value: {int32Array.GetValue(i)}");
            }
        }
    }
}


void MultipleColumns()
{
    Console.WriteLine("Writing Arrow Batches to Spark:");
    var int32Builder = new Int32Array.Builder();
    int32Builder.Append(1);
    int32Builder.AppendNull();
    int32Builder.Append(3);
    int32Builder.AppendNull();
    int32Builder.Append(5);
    
    var int32BuiltArray = int32Builder.Build();
    
    var stringBuilder = new StringArray.Builder();
    stringBuilder.Append("Hello");
    stringBuilder.AppendNull();
    stringBuilder.Append("Spark");
    stringBuilder.Append("From Arrow");
    stringBuilder.AppendNull();
    
    var stringBuiltArray = stringBuilder.Build();
    
    var metaData = new List<KeyValuePair<string, string>>();
    var schema = new Schema(new List<Field>()
    {
        new Field("test", new Int32Type(), true),
        new Field("message", new StringType(), true),
    }, metaData);
    
    
    var batch = new RecordBatch(schema, [int32BuiltArray, stringBuiltArray], 5);
    
    var df = spark.CreateDataFrame(batch, schema);
    df.Show();
    df.PrintSchema();

    var arrowBatches = df.CollectAsArrowBatch();
    
    Console.WriteLine("Reading Arrow Batches back from Spark:");
    var data = new List<int>();
    foreach (var recordBatch in arrowBatches)
    {
        foreach (var column in recordBatch.Arrays)
        {
            switch (column)
            {
                case Int32Array int32Array: 
                    Console.WriteLine($"Int32 Column:");
                    for (var i = 0; i < column.Length; i++)
                    {
                        Console.WriteLine($"row: {i} value: {int32Array.GetValue(i)}");
                    }
                    break;
                case StringArray strArray:
                    Console.WriteLine($"String Column:");
                    for (var i = 0; i < column.Length; i++)
                    {
                        Console.WriteLine($"row: {i} value: {strArray.Values[i]}");
                    }
                    break;
            }
        }
    }
}


void ListOfInt32Column()
{
    Console.WriteLine("Writing Arrow Batches to Spark:");
    
    var childType = new Int32Type();
    var childField = new Field("item", childType, true);
    
    // As of April 2025, you can pass a type or a field, if you pass a type then 
    // the child type will be nullable, if you want to control whether the child
    // types are null or not pass in a `Field`
    var listBuilder = new ListArray.Builder(childField);
    
    // listBuilder is for the list and listValuesBuilder for the child items in each list
    var listValuesBuilder = listBuilder.ValueBuilder as Int32Array.Builder;
    
    //Create a new row
    listBuilder.Append();
    //Add the child values
    listValuesBuilder.Append(1);
    listValuesBuilder.Append(2);
    listValuesBuilder.Append(3);
    
    //Create a new row
    listBuilder.Append();
    //Add the child values
    listValuesBuilder.Append(99);
    listValuesBuilder.Append(999);
    listValuesBuilder.AppendNull();
    listValuesBuilder.Append(999);

    //Create a new null row
    listBuilder.AppendNull();

    //Create a new row
    listBuilder.Append();
    //Add the child values - each row doesn't have to have the same length list
    listValuesBuilder.Append(1);
    listValuesBuilder.Append(2);
    listValuesBuilder.Append(3);
    listValuesBuilder.Append(4);
    listValuesBuilder.Append(5);
    listValuesBuilder.Append(6);
    listValuesBuilder.Append(7);
    listValuesBuilder.Append(8);

    //Build the list - we don't need to build the values builder, that is handled by Arrow
    var builtData = new IArrowArray[] { listBuilder.Build() };
    
    var metaData = new List<KeyValuePair<string, string>>(){{new("test", "metadata")}};
    var schema = new Schema(new List<Field>() { new Field("test", new ListType(childField), true) }, metaData);
    var batch = new RecordBatch(schema, builtData, 4);
    
    var df = spark.CreateDataFrame(batch, schema);
    df.Show();
    df.PrintSchema();

    var arrowBatches = df.CollectAsArrowBatch();
    
    Console.WriteLine("Reading Arrow Batches back from Spark:");
    var data = new List<int>();
    foreach (var recordBatch in arrowBatches)
    {
        foreach (var column in recordBatch.Arrays)
        {
            var int32ListArray = (column as ListArray);

            for (var i = 0; i < int32ListArray.Length; i++)
            {
                if (int32ListArray.IsNull(i))
                {
                    Console.WriteLine($"item: {i} is null");
                }
                else
                {
                    // we have a stream of int's - to break them into rows use the buffers offsets
                    Console.WriteLine($"item: {i} is not null");
                }
            }
        }
    }
}

void StructTypes(){
    
    //create two columns
    var colABuilder = new Int32Array.Builder();
    var colBBuilder = new StringArray.Builder();
    
    colABuilder.Append(1);
    colABuilder.Append(2);
    colABuilder.AppendNull();
    colABuilder.Append(3);
    
    colBBuilder.Append("A");
    colBBuilder.Append("B");
    colBBuilder.AppendNull();
    colBBuilder.Append("C");
    
    var colA = colABuilder.Build();
    var colB = colBBuilder.Build();
    
    //create a StructArray

    var structType = new StructType(new List<Field>()
    {
        new Field("colA", new Int32Type(), true), new Field("colB", new StringType(), true)
    });
    
    var structArray = new StructArray(structType, 4, [colA, colB], ArrowBuffer.Empty, 0);
    
    var rowData = new IArrowArray[] { structArray };
    var metaData = new List<KeyValuePair<string, string>>(){{new("test", "metadata")}};
    var schema = new Schema(new List<Field>() { new Field("test", structType, true) }, metaData);
    var batch = new RecordBatch(schema, rowData, 4);
    
    var df = spark.CreateDataFrame(batch, schema);
    df.Show();
    df.PrintSchema();
}


void NestedStructTypes(){
    
    //create two columns
    var colABuilder = new Int32Array.Builder();
    var colBBuilder = new StringArray.Builder();
    
    colABuilder.Append(1);
    colABuilder.Append(2);
    colABuilder.AppendNull();
    colABuilder.Append(3);
    
    colBBuilder.Append("A");
    colBBuilder.Append("B");
    colBBuilder.AppendNull();
    colBBuilder.Append("C");
    
    var colA = colABuilder.Build();
    var colB = colBBuilder.Build();
    
    //create a StructArray

    var structType = new StructType(new List<Field>()
    {
        new Field("colA", new Int32Type(), true), new Field("colB", new StringType(), true)
    });
    
    var structArray = new StructArray(structType, 4, [colA, colB], ArrowBuffer.Empty, 0);

    //Add the structArray as the data for a new "outer" Struct
    var outerStructType = new StructType(new List<Field>()
    {
        new Field("outer", structType, true)
    });
    
    var outerStructArray = new StructArray(outerStructType, 4, [structArray], ArrowBuffer.Empty, 0);
    
    //lets add an id column
    var int32Builder = new Int32Array.Builder();
    int32Builder.Append(100);
    int32Builder.Append(200);
    int32Builder.Append(300);
    int32Builder.Append(400);
    
    var rowData = new IArrowArray[] { int32Builder.Build(), outerStructArray };
    
    var metaData = new List<KeyValuePair<string, string>>();
    
    var schema = new Schema(new List<Field>()
    {
        new Field("this is the id column", new Int32Type(), false), 
        new Field("outer_st", outerStructType, true)
    }, metaData);
    
    var batch = new RecordBatch(schema, rowData, 4);
    
    var df = spark.CreateDataFrame(batch, schema);
    df.Show();
    df.PrintSchema();
    
    df.Select(f.Col("outer_st.outer.colA") * f.Col("this is the id column")).Show();
}


void MapTypes()
{

    var mapType = new MapType(new StringType(), new Int32Type(), true, true);
    var mapArrayBuilder = new MapArray.Builder(mapType);
    
    var keyBuilder = mapArrayBuilder.KeyBuilder as StringArray.Builder;
    var valueBuilder = mapArrayBuilder.ValueBuilder as Int32Array.Builder;
    
    mapArrayBuilder.Append();
    keyBuilder.Append("A");
    valueBuilder.Append(1);

    mapArrayBuilder.AppendNull();
    
    mapArrayBuilder.Append();
    keyBuilder.Append("B");
    valueBuilder.Append(2);
    
    mapArrayBuilder.Append();
    keyBuilder.Append("C");
    valueBuilder.AppendNull();
    
    var mapArray = mapArrayBuilder.Build();
    
    var rowData = new IArrowArray[] { mapArray };
    var metaData = new List<KeyValuePair<string, string>>(){{new("test", "metadata")}};
    var schema = new Schema(new List<Field>() { new Field("test", mapType, true) }, metaData);
    var batch = new RecordBatch(schema, rowData, 4);
    
    var df = spark.CreateDataFrame(batch, schema);
    df.Show();
    df.PrintSchema();
}


SingleInt32Column();
MultipleColumns();
ListOfInt32Column();
StructTypes();
NestedStructTypes();
MapTypes();
