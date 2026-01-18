using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Evaluation;

/// <summary>
/// Evaluator for binary classification, which expects two input columns: rawPrediction and label.
/// </summary>
public class BinaryClassificationEvaluator : Evaluator
{
    private const string ClassName = "org.apache.spark.ml.evaluation.BinaryClassificationEvaluator";

    private static readonly ParamMap DefaultParams = new(
    [
        new("labelCol", "label"),
        new("metricName", "areaUnderROC"),
        new("numBins", 1000),
        new("rawPredictionCol", "rawPrediction"),
        new("weightCol", "")
    ]);

    public BinaryClassificationEvaluator(SparkSession sparkSession) :
        this(sparkSession, DefaultParams.Clone())
    {
    }

    public BinaryClassificationEvaluator(SparkSession sparkSession, ParamMap paramMap) :
        base(sparkSession, IdentifiableHelper.RandomUID("bineval"), ClassName, paramMap)
    {
    }

    public BinaryClassificationEvaluator(SparkSession sparkSession, IDictionary<string, dynamic> paramMap) :
        this(sparkSession, DefaultParams.Clone().Update(paramMap))
    {
    }

    /// <summary>
    /// Sets the label column name.
    /// </summary>
    public void SetLabelCol(string labelCol) => ParamMap.Add("labelCol", labelCol);
    public string GetLabelCol() => ParamMap.Get("labelCol").Value;

    /// <summary>
    /// Sets the metric to use for evaluation. Supported: "areaUnderROC", "areaUnderPR".
    /// </summary>
    public void SetMetricName(string metricName) => ParamMap.Add("metricName", metricName);
    public string GetMetricName() => ParamMap.Get("metricName").Value;

    /// <summary>
    /// Sets the number of bins used when computing metrics.
    /// </summary>
    public void SetNumBins(int numBins) => ParamMap.Add("numBins", numBins);
    public int GetNumBins() => ParamMap.Get("numBins").Value;

    /// <summary>
    /// Sets the raw prediction column name.
    /// </summary>
    public void SetRawPredictionCol(string rawPredictionCol) => ParamMap.Add("rawPredictionCol", rawPredictionCol);
    public string GetRawPredictionCol() => ParamMap.Get("rawPredictionCol").Value;

    /// <summary>
    /// Sets the weight column name.
    /// </summary>
    public void SetWeightCol(string weightCol) => ParamMap.Add("weightCol", weightCol);
    public string GetWeightCol() => ParamMap.Get("weightCol").Value;
}
