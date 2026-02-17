using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Model fitted by <see cref="Word2Vec"/>.
/// </summary>
public class Word2VecModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap)
    : Model(uid, ClassName, objRef, sparkSession, paramMap)
{
    internal const string ClassName = "org.apache.spark.ml.feature.Word2VecModel";

    /// <summary>
    /// Load a previously saved <see cref="Word2VecModel"/>.
    /// </summary>
    public static Word2VecModel Load(string path, SparkSession sparkSession)
    {
        var mlResult = Transformer.Load(path, sparkSession, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, Word2Vec.DefaultParams.Clone());
        return new Word2VecModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, sparkSession, paramMap);
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
    /// Set the vector size parameter.
    /// </summary>
    public void SetVectorSize(int value) => ParamMap.Add("vectorSize", value);

    /// <summary>
    /// Set the minimum number of times a token must appear.
    /// </summary>
    public void SetMinCount(int value) => ParamMap.Add("minCount", value);

    /// <summary>
    /// Set the number of partitions for training.
    /// </summary>
    public void SetNumPartitions(int value) => ParamMap.Add("numPartitions", value);

    /// <summary>
    /// Set the optimization step size.
    /// </summary>
    public void SetStepSize(double value) => ParamMap.Add("stepSize", value);

    /// <summary>
    /// Set the maximum number of iterations.
    /// </summary>
    public void SetMaxIter(int value) => ParamMap.Add("maxIter", value);

    /// <summary>
    /// Set the random seed.
    /// </summary>
    public void SetSeed(long? value) => ParamMap.Add("seed", value);

    /// <summary>
    /// Set the window size for context words.
    /// </summary>
    public void SetWindowSize(int value) => ParamMap.Add("windowSize", value);

    /// <summary>
    /// Set the maximum sentence length.
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
    /// Get the number of partitions.
    /// </summary>
    public int GetNumPartitions() => ParamMap.Get("numPartitions").Value;

    /// <summary>
    /// Get the step size value.
    /// </summary>
    public double GetStepSize() => ParamMap.Get("stepSize").Value;

    /// <summary>
    /// Get the maximum iterations.
    /// </summary>
    public int GetMaxIter() => ParamMap.Get("maxIter").Value;

    /// <summary>
    /// Get the seed used.
    /// </summary>
    public long? GetSeed() => ParamMap.Get("seed").Value;

    /// <summary>
    /// Get the window size used for context words.
    /// </summary>
    public int GetWindowSize() => ParamMap.Get("windowSize").Value;

    /// <summary>
    /// Get the maximum sentence length.
    /// </summary>
    public int GetMaxSentenceLength() => ParamMap.Get("maxSentenceLength").Value;
}
