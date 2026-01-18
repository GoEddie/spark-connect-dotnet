using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Extracts a vocabulary from document collections and generates a CountVectorizerModel.
/// Converts text documents to vectors of token counts.
/// </summary>
public class CountVectorizer : Estimator<CountVectorizerModel>
{
    public CountVectorizer() : this(DefaultParams.Clone())
    {
    }

    public CountVectorizer(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("countvec"), "org.apache.spark.ml.feature.CountVectorizer", DefaultParams.Clone())
    {
    }

    public CountVectorizer(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("binary", false),
        new("inputCol", ""),
        new("maxDF", 1.0),
        new("minDF", 1.0),
        new("minTF", 1.0),
        new("outputCol", ""),
        new("vocabSize", 262144)
    ]);

    /// <summary>
    /// Sets whether to output binary counts (1 if term appears, 0 otherwise).
    /// </summary>
    public void SetBinary(bool binary) => ParamMap.Add("binary", binary);
    public bool GetBinary() => ParamMap.Get("binary").Value;

    /// <summary>
    /// Sets the input column name.
    /// </summary>
    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public string GetInputCol() => ParamMap.Get("inputCol").Value;

    /// <summary>
    /// Sets the maximum document frequency. Terms appearing in more than maxDF documents are filtered out.
    /// If in range [0, 1), treated as a fraction of total documents.
    /// </summary>
    public void SetMaxDF(double maxDF) => ParamMap.Add("maxDF", maxDF);
    public double GetMaxDF() => ParamMap.Get("maxDF").Value;

    /// <summary>
    /// Sets the minimum document frequency. Terms appearing in fewer than minDF documents are filtered out.
    /// If in range [0, 1), treated as a fraction of total documents.
    /// </summary>
    public void SetMinDF(double minDF) => ParamMap.Add("minDF", minDF);
    public double GetMinDF() => ParamMap.Get("minDF").Value;

    /// <summary>
    /// Sets the minimum term frequency. Filter to ignore rare words in a document.
    /// </summary>
    public void SetMinTF(double minTF) => ParamMap.Add("minTF", minTF);
    public double GetMinTF() => ParamMap.Get("minTF").Value;

    /// <summary>
    /// Sets the output column name.
    /// </summary>
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public string GetOutputCol() => ParamMap.Get("outputCol").Value;

    /// <summary>
    /// Sets the maximum size of the vocabulary.
    /// </summary>
    public void SetVocabSize(int vocabSize) => ParamMap.Add("vocabSize", vocabSize);
    public int GetVocabSize() => ParamMap.Get("vocabSize").Value;
}

/// <summary>
/// Model fitted by CountVectorizer.
/// </summary>
public class CountVectorizerModel : Model
{
    private const string ClassName = "org.apache.spark.ml.feature.CountVectorizerModel";

    public CountVectorizerModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the vocabulary learned from the training data.
    /// </summary>
    public List<string> Vocabulary => Fetch("vocabulary");

    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public void SetBinary(bool binary) => ParamMap.Add("binary", binary);
    public void SetMinTF(double minTF) => ParamMap.Add("minTF", minTF);

    public static CountVectorizerModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, CountVectorizer.DefaultParams.Clone());
        return new CountVectorizerModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
