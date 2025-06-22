using System.Runtime.CompilerServices;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Org.Graphframes.Connect.Proto;
using Spark.Connect;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

var spark = SparkSession
    .Builder
    .Remote("http://localhost:15002")
    .GetOrCreate();

var data = new List<(string, string)>()
{
    ("A", "Alice"),
    ("B", "Bob"),
    ("C", "Charlie"),
    ("D", "David"),
    ("E", "Eve")
};

var schema = new  StructType(new[]
{
    new StructField("id", new StringType(), false),
    new StructField("name", new StringType(), false),
});

var vetices = spark.CreateDataFrame(data.Cast<ITuple>(), schema);
vetices.Show();


var edgeData = new List<(string, string,int)>()
{
    ("A", "B", 1),
    ("A", "C", 4),
    ("B", "C", 2),
    ("B", "D", 5),
    ("C", "D", 1),
    ("C", "E", 3),
    ("D", "E", 2)
};

var edgeSchema = new  StructType(new[]
{
    new StructField("src", new StringType(), false),
    new StructField("dst", new StringType(), false),
    new StructField("weight", new IntegerType(), false),
});

var edges = spark.CreateDataFrame(edgeData.Cast<ITuple>(), edgeSchema);

ByteString DataFrameToByteString(DataFrame frame)
{
    var toDF = new ToDF()
    {
        Input = frame.Relation,
        ColumnNames = { frame.Columns }
    };

    var plan = new Plan()
    {
        Root = new Relation()
        {
            ToDf = toDF
        }
    };
    
    return plan.ToByteString();
}

var api = new GraphFramesAPI()
{
    Vertices    = DataFrameToByteString(vetices),
    Edges     = DataFrameToByteString(edges),
    ShortestPaths = new ShortestPaths()
    {
        Landmarks = { new []{new StringOrLongID()
        {
            StringId = "E"
        }} }
    }
};

var sparkRelation = new Relation()
{
    Extension = Any.Pack(api),
};

var plan = new Plan()
{
    Root = sparkRelation
};

var requestExecutor = new RequestExecutor(spark, plan);
requestExecutor.Exec();

var result = requestExecutor.GetRelation();
new DataFrame(spark, result).Show();

Console.WriteLine("done");