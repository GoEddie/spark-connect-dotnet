using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.ML.Classification;

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