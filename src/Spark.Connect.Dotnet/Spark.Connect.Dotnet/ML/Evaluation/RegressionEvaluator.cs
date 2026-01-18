using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Evaluation;

/// <summary>
/// Evaluator for regression, which expects two input columns: prediction and label.
/// </summary>
public class RegressionEvaluator : Evaluator
{
    private const string ClassName = "org.apache.spark.ml.evaluation.RegressionEvaluator";

    private static readonly ParamMap DefaultParams = new(
    [
        new("labelCol", "label"),
        new("metricName", "rmse"),
        new("predictionCol", "prediction"),
        new("throughOrigin", false),
        new("weightCol", "")
    ]);

    public RegressionEvaluator(SparkSession sparkSession) :
        this(sparkSession, DefaultParams.Clone())
    {
    }

    public RegressionEvaluator(SparkSession sparkSession, ParamMap paramMap) :
        base(sparkSession, IdentifiableHelper.RandomUID("regeval"), ClassName, paramMap)
    {
    }

    public RegressionEvaluator(SparkSession sparkSession, IDictionary<string, dynamic> paramMap) :
        this(sparkSession, DefaultParams.Clone().Update(paramMap))
    {
    }

    /// <summary>
    /// Sets the label column name.
    /// </summary>
    public void SetLabelCol(string labelCol) => ParamMap.Add("labelCol", labelCol);
    public string GetLabelCol() => ParamMap.Get("labelCol").Value;

    /// <summary>
    /// Sets the metric to use for evaluation. Supported: "rmse" (root mean squared error),
    /// "mse" (mean squared error), "r2" (R squared coefficient), "mae" (mean absolute error),
    /// "var" (explained variance).
    /// </summary>
    public void SetMetricName(string metricName) => ParamMap.Add("metricName", metricName);
    public string GetMetricName() => ParamMap.Get("metricName").Value;

    /// <summary>
    /// Sets the prediction column name.
    /// </summary>
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public string GetPredictionCol() => ParamMap.Get("predictionCol").Value;

    /// <summary>
    /// Sets whether the regression is through the origin.
    /// </summary>
    public void SetThroughOrigin(bool throughOrigin) => ParamMap.Add("throughOrigin", throughOrigin);
    public bool GetThroughOrigin() => ParamMap.Get("throughOrigin").Value;

    /// <summary>
    /// Sets the weight column name.
    /// </summary>
    public void SetWeightCol(string weightCol) => ParamMap.Add("weightCol", weightCol);
    public string GetWeightCol() => ParamMap.Get("weightCol").Value;
}
