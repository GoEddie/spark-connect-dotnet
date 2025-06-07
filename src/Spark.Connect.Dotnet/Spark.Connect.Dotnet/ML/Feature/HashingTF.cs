using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// HashingTF converts a sequence of terms to their term frequency vectors
/// using the hashing trick.
/// </summary>
/// <param name="sparkSession">The <see cref="SparkSession"/> this transformer belongs to.</param>
/// <param name="paramMap">Optional parameters map to initialize the transformer.</param>
public class HashingTF(SparkSession sparkSession, ParamMap paramMap) : Transformer(sparkSession, ClassName, paramMap)
{
    private const string ClassName = "org.apache.spark.ml.feature.HashingTF";

    private static readonly ParamMap DefaultParams = new([
        new("numFeatures", 1 << 18),
        new("binary", false),
        new("inputCol", string.Empty),
        new("outputCol", string.Empty)
    ]);

    /// <summary>
    /// Creates a <see cref="HashingTF"/> with default parameters.
    /// </summary>
    /// <param name="sparkSession">Spark session instance.</param>
    public HashingTF(SparkSession sparkSession) : this(sparkSession, DefaultParams.Clone())
    {
    }

    /// <summary>
    /// Creates a <see cref="HashingTF"/> with parameters supplied in a dictionary.
    /// </summary>
    /// <param name="sparkSession">Spark session instance.</param>
    /// <param name="parameters">Dictionary of parameters to set.</param>
    public HashingTF(SparkSession sparkSession, IDictionary<string, dynamic> parameters)
        : this(sparkSession, DefaultParams.Clone().Update(parameters))
    {
    }

    /// <summary>
    /// Sets the input column containing sequences of terms.
    /// </summary>
    public void SetInputCol(string value) => ParamMap.Add("inputCol", value);

    /// <summary>
    /// Sets the output column to store term frequency vectors.
    /// </summary>
    public void SetOutputCol(string value) => ParamMap.Add("outputCol", value);

    /// <summary>
    /// Sets the number of features (hash buckets).
    /// </summary>
    public void SetNumFeatures(int value) => ParamMap.Add("numFeatures", value);

    /// <summary>
    /// If true, all non-zero counts are set to 1.0.
    /// </summary>
    public void SetBinary(bool value) => ParamMap.Add("binary", value);

    /// <summary>
    /// Gets the input column name.
    /// </summary>
    public string GetInputCol() => ParamMap.Get("inputCol").Value;

    /// <summary>
    /// Gets the output column name.
    /// </summary>
    public string GetOutputCol() => ParamMap.Get("outputCol").Value;

    /// <summary>
    /// Gets the number of features (hash buckets).
    /// </summary>
    public int GetNumFeatures() => ParamMap.Get("numFeatures").Value;

    /// <summary>
    /// Gets whether features are binary.
    /// </summary>
    public bool GetBinary() => ParamMap.Get("binary").Value;

    /// <summary>
    /// Load a previously saved <see cref="HashingTF"/> instance.
    /// </summary>
    /// <param name="path">Path on the Spark Connect server to load from.</param>
    /// <param name="spark">The <see cref="SparkSession"/> to use.</param>
    public static HashingTF Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        return new HashingTF(spark, paramMap);
    }
}
