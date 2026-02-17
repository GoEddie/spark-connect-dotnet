using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Word2Vec Transforms a word into a vector.
/// </summary>
public class Word2Vec : Estimator<Word2VecModel>
{
    internal const string ClassName = "org.apache.spark.ml.feature.Word2Vec";

    public static readonly ParamMap DefaultParams = new([
        new("vectorSize", 100),
        new("minCount", 5),
        new("numPartitions", 1),
        new("stepSize", 0.025),
        new("maxIter", 1),
        new("seed", null),
        new("inputCol", null),
        new("outputCol", null),
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
    /// Set the minimum number of times a token must appear to be included in the vocabulary.
    /// </summary>
    public void SetMinCount(int value) => ParamMap.Add("minCount", value);

    /// <summary>
    /// Set the number of partitions for training.
    /// </summary>
    public void SetNumPartitions(int value) => ParamMap.Add("numPartitions", value);

    /// <summary>
    /// Set the step size for each iteration of optimization.
    /// </summary>
    public void SetStepSize(double value) => ParamMap.Add("stepSize", value);

    /// <summary>
    /// Set the maximum number of iterations to run.
    /// </summary>
    public void SetMaxIter(int value) => ParamMap.Add("maxIter", value);

    /// <summary>
    /// Set the random seed.
    /// </summary>
    public void SetSeed(long? value) => ParamMap.Add("seed", value);

    /// <summary>
    /// Set the window size (context words from [-window, window]).
    /// </summary>
    public void SetWindowSize(int value) => ParamMap.Add("windowSize", value);

    /// <summary>
    /// Set the maximum length (in words) of each sentence.
    /// </summary>
    public void SetMaxSentenceLength(int value) => ParamMap.Add("maxSentenceLength", value);

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

    /// <summary>
    /// Get the minimum count for tokens.
    /// </summary>
    public int GetMinCount() => ParamMap.Get("minCount").Value;

    /// <summary>
    /// Get the number of partitions used in training.
    /// </summary>
    public int GetNumPartitions() => ParamMap.Get("numPartitions").Value;

    /// <summary>
    /// Get the step size.
    /// </summary>
    public double GetStepSize() => ParamMap.Get("stepSize").Value;

    /// <summary>
    /// Get the maximum number of iterations.
    /// </summary>
    public int GetMaxIter() => ParamMap.Get("maxIter").Value;

    /// <summary>
    /// Get the seed value.
    /// </summary>
    public long? GetSeed() => ParamMap.Get("seed").Value;

    /// <summary>
    /// Get the window size.
    /// </summary>
    public int GetWindowSize() => ParamMap.Get("windowSize").Value;

    /// <summary>
    /// Get the maximum sentence length.
    /// </summary>
    public int GetMaxSentenceLength() => ParamMap.Get("maxSentenceLength").Value;
}
