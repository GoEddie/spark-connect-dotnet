using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Evaluation;

/// <summary>
/// Evaluator for multiclass classification, which expects two input columns: prediction and label.
/// </summary>
public class MulticlassClassificationEvaluator : Evaluator
{
    private const string ClassName = "org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator";

    private static readonly ParamMap DefaultParams = new(
    [
        new("beta", 1.0),
        new("eps", 1.0E-15),
        new("labelCol", "label"),
        new("metricLabel", 0.0),
        new("metricName", "f1"),
        new("predictionCol", "prediction"),
        new("probabilityCol", "probability"),
        new("weightCol", "")
    ]);

    public MulticlassClassificationEvaluator(SparkSession sparkSession) :
        this(sparkSession, DefaultParams.Clone())
    {
    }

    public MulticlassClassificationEvaluator(SparkSession sparkSession, ParamMap paramMap) :
        base(sparkSession, IdentifiableHelper.RandomUID("mceval"), ClassName, paramMap)
    {
    }

    public MulticlassClassificationEvaluator(SparkSession sparkSession, IDictionary<string, dynamic> paramMap) :
        this(sparkSession, DefaultParams.Clone().Update(paramMap))
    {
    }

    /// <summary>
    /// Sets the beta value used in weightedPrecision and weightedRecall.
    /// </summary>
    public void SetBeta(double beta) => ParamMap.Add("beta", beta);
    public double GetBeta() => ParamMap.Get("beta").Value;

    /// <summary>
    /// Sets the label column name.
    /// </summary>
    public void SetLabelCol(string labelCol) => ParamMap.Add("labelCol", labelCol);
    public string GetLabelCol() => ParamMap.Get("labelCol").Value;

    /// <summary>
    /// Sets the value of the positive label when computing per-label metrics.
    /// </summary>
    public void SetMetricLabel(double metricLabel) => ParamMap.Add("metricLabel", metricLabel);
    public double GetMetricLabel() => ParamMap.Get("metricLabel").Value;

    /// <summary>
    /// Sets the metric to use for evaluation. Supported: "f1", "accuracy", "weightedPrecision",
    /// "weightedRecall", "weightedTruePositiveRate", "weightedFalsePositiveRate",
    /// "weightedFMeasure", "truePositiveRateByLabel", "falsePositiveRateByLabel",
    /// "precisionByLabel", "recallByLabel", "fMeasureByLabel", "logLoss", "hammingLoss".
    /// </summary>
    public void SetMetricName(string metricName) => ParamMap.Add("metricName", metricName);
    public string GetMetricName() => ParamMap.Get("metricName").Value;

    /// <summary>
    /// Sets the prediction column name.
    /// </summary>
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public string GetPredictionCol() => ParamMap.Get("predictionCol").Value;

    /// <summary>
    /// Sets the probability column name.
    /// </summary>
    public void SetProbabilityCol(string probabilityCol) => ParamMap.Add("probabilityCol", probabilityCol);
    public string GetProbabilityCol() => ParamMap.Get("probabilityCol").Value;

    /// <summary>
    /// Sets the weight column name.
    /// </summary>
    public void SetWeightCol(string weightCol) => ParamMap.Add("weightCol", weightCol);
    public string GetWeightCol() => ParamMap.Get("weightCol").Value;
}
