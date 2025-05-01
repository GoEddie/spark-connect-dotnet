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

/// <summary>
/// Logistic regression. This class supports multinomial logistic (softmax) and binomial logistic regression.
/// </summary>
public class LogisticRegression() : Estimator<LogisticRegressionModel>( IdentifiableHelper.RandomUID("logreg-static"), "org.apache.spark.ml.classification.LogisticRegression", DefaultParams)
{
    public static readonly ParamMap DefaultParams = new(
    [
            new("aggregationDepth", 2), 
            new("elasticNetParam", 0.0), 
            new("family", "auto"), 
            new("featuresCol", "features"), 
            new("fitIntercept", true), 
            new("labelCol", "label"), 
            new("maxBlockSizeInMB", 0.0), 
            new("maxIter", 100), 
            new("predictionCol", "prediction"), 
            new("probabilityCol", "probability"), 
            new("rawPredictionCol", "rawPrediction"), 
            new("regParam", 0.0), 
            new("standardization", true), 
            new("threshold", 0.5), 
            new("tol", 1.0E-6)
    ]);
}