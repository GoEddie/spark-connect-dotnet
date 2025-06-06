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
