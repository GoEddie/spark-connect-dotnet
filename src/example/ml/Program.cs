﻿using System.Reflection;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Spark.Connect;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.ML;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.ML.Param;
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

var data = new List<(double f, DenseVector Vector)>()
{
    (1.0, new DenseVector([0.0, 1.1, 0.1])), 
    (0.0, new DenseVector([2.0, 1.0, -1.0])), 
    (0.0, new DenseVector([2.0, 1.3, 1.0])), 
    (1.0, new DenseVector([0.0, 1.2, -0.5]))
};

var schema = new  Spark.Connect.Dotnet.Sql.Types.StructType(new[]
{
    new StructField("label", new DoubleType(), false),
    new StructField("features", new VectorUDT(), false)
});

var training = spark.CreateDataFrame(data.Cast<ITuple>(), schema);
training.Show();

var lr = new LogisticRegression();
var paramMap = lr.ParamMap;
paramMap.Add("maxIter", 10);
paramMap.Add("regParam", 0.01);
paramMap.Add("aggregationDepth", 299);
paramMap.Add("rawPredictionCol", "my output col");

var transformer = lr.Fit(training, paramMap);

var prediction = transformer.Transform(training);
var result = prediction.Select("features", "label", "my output col", "prediction");
result.PrintSchema();

result.Show(4, 1000);

transformer.Save("/tmp/transformer-save");

var transformerFromDisk = LogisticRegressionModel.Load("/tmp/transformer-save", spark);
Console.WriteLine("Model used Parameters:");
Console.WriteLine(transformerFromDisk.ExplainParams());
var df2 = transformerFromDisk.Transform(training);
df2.Show(4, 1000);

var dataIDF = new List<(double f, DenseVector Vector)>()
{
    (1, new DenseVector([1.0, 2.0])), 
    (2, new DenseVector([0.0, 1.0])), 
    (3, new DenseVector([3.0, 0.2]))
};

var schemaIDF = new  Spark.Connect.Dotnet.Sql.Types.StructType(new[]
{
    new StructField("label", new DoubleType(), false),
    new StructField("tf", new VectorUDT(), false)
});

var dfIDF = spark.CreateDataFrame(dataIDF.Cast<ITuple>(), schemaIDF);

var idf = new IDF();
idf.SetInputCol("tf");
idf.SetMinDocFreq(2);
idf.ParamMap.Add("outputCol", "tf-idf");

dfIDF = dfIDF.WithColumnRenamed("tf", "tfi");

var idfModel = idf.Fit(dfIDF, idf.ParamMap.Update(new Dictionary<string, dynamic>(){{"inputCol", "tfi"}}));

Console.WriteLine($"IDF Model Params: {idfModel.GetInputCol()}, {idfModel.GetOutputCol()}, {idfModel.GetMinDocFreq()}");

var dfIDFOutput = idfModel.Transform(dfIDF);
dfIDFOutput.PrintSchema();
dfIDFOutput.Show(3, 10000);
idfModel.Save("/tmp/transformers/idf-model");

var loadedIdf = IDFModel.Load("/tmp/transformers/idf-model", spark);

idfModel.Transform(dfIDF).Show(3, 10000);

var dfFromLoaded = loadedIdf.Transform(dfIDF);

dfFromLoaded.Show(3, 10000);

var tokenizer = new Tokenizer(spark);

tokenizer.SetInputCol("text");
tokenizer.SetOutputCol("words-go-here");

var dfWords = spark.CreateDataFrame((new List<(int id, string text)>()
{
    (1, "This is a test"), (2, "This is another test")
}).Cast<ITuple>(), new Spark.Connect.Dotnet.Sql.Types.StructType((List<StructField>) [new StructField("id", new Int32Type(), false), new StructField("text", new StringType(), false)]));

var tokens = tokenizer.Transform(dfWords, tokenizer.ParamMap.Update(new Dictionary<string, dynamic>(){{"outputCol", "override-output-col"}}));
tokens.Show(3, 10000);

tokenizer.Save("/tmp/transformers/tokenizer");
var loadedTokenizer = Tokenizer.Load("/tmp/transformers/tokenizer", spark);