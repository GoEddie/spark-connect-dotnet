using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Classification;


/// <summary>
/// Model fitted by `LogisticRegression`.
/// </summary>
public class NaiveBayesModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap)
    : Model(uid, ClassName, objRef, sparkSession, paramMap)
{
    private const string ClassName = "org.apache.spark.ml.classification.NaiveBayesModel";

    /// <summary>
    /// Load a `NaiveBayesModel` that was previously saved to disk on the Spark Connect server
    /// </summary>
    /// <param name="path">Where to read the `NaiveBayesModel` from</param>
    /// <param name="sparkSession">A `SparkSession` to read the model through</param>
    /// <returns>`NaiveBayesModel`</returns>
    public static NaiveBayesModel Load(string path, SparkSession sparkSession)
    {
        var mlResult = Transformer.Load(path, sparkSession, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, NaiveBayes.DefaultParams.Clone());
        
        var loadedModel = new NaiveBayesModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, sparkSession, paramMap);
        
        return loadedModel;
    }
}

/// <summary>
/// Logistic regression. This class supports multinomial logistic (softmax) and binomial logistic regression.
/// </summary>
public class NaiveBayes() : Estimator<NaiveBayesModel>( IdentifiableHelper.RandomUID("nb-static"), "org.apache.spark.ml.classification.NaiveBayes", DefaultParams)
{
    public static readonly ParamMap DefaultParams = new(
    [
            new("featuresCol", "features"), 
            new("labelCol", "label"), 
            new("predictionCol", "prediction"), 
            new("probabilityCol", "probability"), 
            new("rawPredictionCol", "rawPrediction"), 
            new("smoothing", 1.0F), 
            new("modelType", "multinomial"), 
            new("thresholds", new float[]{}),
            new("weightCol", ""),
    ]);
}