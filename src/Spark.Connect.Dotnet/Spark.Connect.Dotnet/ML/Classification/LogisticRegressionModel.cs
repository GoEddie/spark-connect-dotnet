using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Classification;

/// <summary>
/// Model fitted by `LogisticRegression`.
/// </summary>
public class LogisticRegressionModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap)
    : Model(uid, ClassName, objRef, sparkSession, paramMap)
{
    private const string ClassName = "org.apache.spark.ml.classification.LogisticRegressionModel";

    /// <summary>
    /// Load a `LogisticRegressionModel` that was previously saved to disk on the Spark Connect server
    /// </summary>
    /// <param name="path">Where to read the `LogisticRegressionModel` from</param>
    /// <param name="sparkSession">A `SparkSession` to read the model through</param>
    /// <returns>`LogisticRegressionModel`</returns>
    public static LogisticRegressionModel Load(string path, SparkSession sparkSession)
    {
        var mlResult = Transformer.Load(path, sparkSession, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, LogisticRegression.DefaultParams.Clone());
        
        var loadedModel = new LogisticRegressionModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, sparkSession, paramMap);
        
        return loadedModel;
    }
}