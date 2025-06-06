using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Word2Vec trains a model of Map(String, Vector), i.e. transforms a word into a vector.
/// </summary>
public class Word2Vec : Estimator<Word2VecModel>
{
    internal const string ClassName = "org.apache.spark.ml.feature.Word2Vec";

    public static readonly ParamMap DefaultParams = new([
        new("vectorSize", 100),
        new("inputCol", ""),
        new("outputCol", ""),
        new("minCount", 5),
        new("numPartitions", 1),
        new("stepSize", 0.025f),
        new("maxIter", 1),
        new("seed", 0L),
        new("windowSize", 5),
        new("maxSentenceLength", 1000)
    ]);

    /// <summary>
    /// Creates a Word2Vec estimator with default parameters.
    /// </summary>
    public Word2Vec() : this(DefaultParams.Clone())
    {
    }

    /// <summary>
    /// Creates a Word2Vec estimator with the provided parameter map.
    /// </summary>
    /// <param name="paramMap">Parameters to use when fitting.</param>
    public Word2Vec(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("word2vec-static"), ClassName, paramMap)
    {
    }

    /// <summary>
    /// Creates a Word2Vec estimator with the provided parameter dictionary.
    /// </summary>
    public Word2Vec(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    /// <summary>
    /// Set the name of the input column containing words.
    /// </summary>
    public void SetInputCol(string value) => ParamMap.Add("inputCol", value);

    /// <summary>
    /// Set the name of the output column to store resulting vectors.
    /// </summary>
    public void SetOutputCol(string value) => ParamMap.Add("outputCol", value);

    /// <summary>
    /// Set the dimension of the vector representation.
    /// </summary>
    public void SetVectorSize(int value) => ParamMap.Add("vectorSize", value);

    /// <summary>
    /// Get the input column name.
    /// </summary>
    public string GetInputCol() => ParamMap.Get("inputCol").Value;

    /// <summary>
    /// Get the output column name.
    /// </summary>
    public string GetOutputCol() => ParamMap.Get("outputCol").Value;

    /// <summary>
    /// Get the vector size.
    /// </summary>
    public int GetVectorSize() => ParamMap.Get("vectorSize").Value;
}
