using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Types;
using Spark.Connect;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using DateType = Spark.Connect.Dotnet.Sql.Types.DateType;
using DoubleType = Apache.Arrow.Types.DoubleType;
using StringType = Apache.Arrow.Types.StringType;
using StructType = Apache.Arrow.Types.StructType;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

var nullableDoubleList = new Field("item", new DoubleType(), true);



ListType CreateListWithNullableChild(IArrowType childType)
{
    var newListType = new ListType(childType);
    var backingField = newListType.ValueField.GetType().GetField("<IsNullable>k__BackingField", BindingFlags.NonPublic | BindingFlags.Instance);
    if (backingField == null)
    {
        throw new NullReferenceException("Cannot create nullable list child because we can't find `<IsNullable>k__BackingField` - has the Arrow library changed to support creating nullable list child types yet?");
    }
    
    backingField.SetValue(newListType.ValueField, false);
    return newListType;
}

// var listType = new ListType(new DoubleType());
// var listIntType = new ListType(new Int32Type());

var listType = CreateListWithNullableChild(new DoubleType());
var listIntType = CreateListWithNullableChild(new Int32Type());

var structFields = new []
{
    new Field("type", Int8Type.Default, nullable: false),
    new Field("size", Int32Type.Default, nullable: true),
    new Field("indices", listIntType, nullable: true),
    new Field("values", listType, nullable: true)
};

var structType = new StructType(structFields);

var schema = new Schema(new []
{
    new Field("id", Int32Type.Default, nullable: true),
    new Field("features", structType, nullable: true)
}, new List<KeyValuePair<string, string>>());

var rawData = new List<List<object>>()
{
    new List<object>()
    {
        1, new DenseVector(new() { 0.0, 1.1, 0.1 })
    }
    , new List<object>()
    {
        2, new DenseVector(new() { 2.0, 1.0, -1.0 })
    }
    , new List<object>()
    {
        3, new DenseVector(new() { 2.0, 1.3, 1.0 })
    }
    , new List<object>()
    {
        4, new DenseVector(new() { 0.0, 1.2, -0.5 })
    }
};

var valuesBuilder = new ListArray.Builder(new DoubleType());
var valuesValueBuilder  = valuesBuilder.ValueBuilder as DoubleArray.Builder;

var indiciesBuilder = new ListArray.Builder(new Int32Type());

var lengthBuilder = new Int32Array.Builder();
var typeBuilder = new Int8Array.Builder();

var idBuilder = new Int32Array.Builder();

foreach (var row in rawData)
{
    idBuilder.Append((int)row[0]);
    typeBuilder.Append(1);
    // lengthBuilder.Append(3);
    lengthBuilder.AppendNull();
    indiciesBuilder.AppendNull();
    // indiciesBuilder.Append();
    
    
    
    valuesBuilder.Append();
    var values = (row[1] as DenseVector)!.Values;
    foreach (var value in values!)
    {
        valuesValueBuilder!.Append(value);
    }
}

var featuresData = new IArrowArray[] { typeBuilder.Build(), lengthBuilder.Build(), indiciesBuilder.Build(), valuesBuilder.Build() };
var featuresArray = new StructArray(
    new StructType(structFields),
    4,
    featuresData,
    ArrowBuffer.Empty,0); 

var data = new List<IArrowArray>() { idBuilder.Build(), featuresArray };

var batch = new RecordBatch(schema, data, 4);
spark.Conf.Set(SparkDotnetKnownConfigKeys.DecodeArrowType, "ArrowBuffers");

var df = spark.CreateDataFrame(batch, schema, "{\"fields\":[{\"metadata\":{},\"name\":\"label\",\"nullable\":true,\"type\":\"double\"},{\"metadata\":{},\"name\":\"features\",\"nullable\":true,\"type\":{\"class\":\"org.apache.spark.ml.linalg.VectorUDT\",\"pyClass\":\"pyspark.ml.linalg.VectorUDT\",\"sqlType\":{\"fields\":[{\"metadata\":{},\"name\":\"type\",\"nullable\":false,\"type\":\"byte\"},{\"metadata\":{},\"name\":\"size\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"indices\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"integer\",\"type\":\"array\"}},{\"metadata\":{},\"name\":\"values\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"double\",\"type\":\"array\"}}],\"type\":\"struct\"},\"type\":\"udt\"}}],\"type\":\"struct\"}");

df.Show(1000, 10000);
df.PrintSchema();

// var f1 = new StructField("f1", new Int32Type(), nullable: true);
// var f2 = new StructField("f2", new StructType(), nullable: true);

//
// // Create a record batch with our array
// var batch = new RecordBatch(schema, new[] { listArray }, listArray.Length);
// //
// var df = spark.CreateDataFrame(batch, schema);
// df.Show();
// Console.WriteLine(df.Schema);

//
//
// var schema = new Schema(new[] {
//     new Field("double_lists", listType, nullable: true)
// }, new List<KeyValuePair<string, string>>());
//
// var listBuilder = new ListArray.Builder(new DoubleType());
// var valueBuilder = listBuilder.ValueBuilder as DoubleArray.Builder;
//
// listBuilder.Append();
//
// valueBuilder.Append(1.1);
// valueBuilder.Append(2.2);
// valueBuilder.Append(3.3);
//
// // Second row: list of 2 doubles
// listBuilder.Append();
// valueBuilder.Append(4.4);
// valueBuilder.Append(5.5);
//
// // Third row: list of 4 doubles
// listBuilder.Append();
// valueBuilder.Append(6.6);
// valueBuilder.Append(7.7);
// valueBuilder.Append(8.8);
// valueBuilder.Append(9.9);
//
// // Fourth row: empty list
// listBuilder.Append();
//
// // Build the list array
// var listArray = listBuilder.Build();
//
// // Create a record batch with our array
// var batch = new RecordBatch(schema, new[] { listArray }, listArray.Length);
// //
// var df = spark.CreateDataFrame(batch, schema);
// df.Show();
// Console.WriteLine(df.Schema);
//







//
// var schema = new StructType(new List<StructField>()
// {
//     new StructField("label", new DoubleType(), false), new StructField("features", new DenseVectorUDT(), false),
// });
//
// var values = new DenseVector(new List<double>() { 0.0, 1.1, 0.1 }).DataForDataframe;



//
// const int dataNumRows = 10;
// const int arrayLength = 4;
// const int arrayOffset = 3;
//
// var fields = new List<Field>
// {
//     new Field.Builder().Name("ints").DataType(new Int32Type()).Nullable(true).Build(),
//     new Field.Builder().Name("doubles").DataType(new DoubleType()).Nullable(true).Build(),
// };
//
//
//
// var listBuilder = new ListArray.Builder(new Field("values", DoubleType.Default, false));
//
//
// var arrays = new List<IArrowArray>
// {
//     new StringArray.Builder().AppendRange(Enumerable.Range(0, dataNumRows).Select(p => "DenseVector")).Build(),
//     new DoubleArray.Builder().AppendRange(Enumerable.Range(0, dataNumRows).Select(i => i * 0.1)).Build(),
//     
// };
//
// var nullBitmap = new ArrowBuffer.BitmapBuilder().AppendRange(true, dataNumRows).Build();
// var array = new StructArray(new StructType(fields), arrayLength, arrays, nullBitmap, nullCount: 0, offset: arrayOffset);
//
// var schemaFields = new List<Field>()
// {
//     new Field("ddd", new StructType(fields), false)
// };
// var schema = new Schema(schemaFields, new List<KeyValuePair<string, string>>());
// var columns = new List<IArrowArray>()
// {
//     array
// };
// var b = new RecordBatch.Builder();
// var rs = b.Append("abc", false, array).Build();
// // var records = new RecordBatch(schema, columns, dataNumRows);
// // Console.WriteLine(rs);
//
// var df = spark.CreateDataFrame(rs, rs.Schema);
//
// df.Show();
// Console.WriteLine(df.Schema);
// var df = spark.CreateDataFrame(new List<List<object>>()
// {
//     new List<object>()
//     {
//         1.0, new DenseVector(new (){0.0, 1.1, 0.1}).DataForDataframe
//     },
//     new List<object>()
//     {
//         0.0, new DenseVector(new (){2.0, 1.0, -1.0}).DataForDataframe
//     },
//     new List<object>()
//     {
//         0.0, new DenseVector(new (){2.0, 1.3, 1.0}).DataForDataframe
//     },
//     new List<object>()
//     {
//         1.0, new DenseVector(new (){0.0, 1.2, -0.5}).DataForDataframe
//     },
// }, schema);
//
//
// df.Show();
    //
    //
    // var plan = new Plan()
    // {
    //     Command = new Command()
    //     {
    //         MlCommand = new MlCommand()
    //         {
    //             Fit = new MlCommand.Types.Fit()
    //             {
    //                 Dataset = spark.Range(100).Relation,
    //                 Estimator = new MlOperator()
    //                 {
    //                     Name = "Tokenizer", Type = MlOperator.Types.OperatorType.Transformer, Uid = Guid.NewGuid().ToString()
    //                 }
    //             }
    //         }
    //     }
    // };
    //
    // var request = new RequestExecutor(spark, plan, ArrowHandling.ArrowBuffers);
    // request.Exec();
    // Console.Write(request);
    // spark.GrpcClient.ExecutePlan()