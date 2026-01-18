using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Regression;

/// <summary>
/// Model fitted by LinearRegression.
/// </summary>
public class LinearRegressionModel : Model
{
    private const string ClassName = "org.apache.spark.ml.regression.LinearRegressionModel";

    private static readonly ParamMap DefaultParams = new(
    [
        new("featuresCol", "features"),
        new("labelCol", "label"),
        new("predictionCol", "prediction")
    ]);

    public LinearRegressionModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the model coefficients.
    /// </summary>
    public List<double> Coefficients => Fetch("coefficients");

    /// <summary>
    /// Gets the model intercept.
    /// </summary>
    public double Intercept => Fetch("intercept");

    /// <summary>
    /// Gets the number of features the model was trained on.
    /// </summary>
    public int NumFeatures => Fetch("numFeatures");

    /// <summary>
    /// Gets the scale of the residuals (root mean squared error).
    /// This is only available when using the "huber" solver.
    /// </summary>
    public double Scale => Fetch("scale");

    /// <summary>
    /// Sets the name of the column containing feature vectors.
    /// </summary>
    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public string GetFeaturesCol() => ParamMap.Get("featuresCol").Value;

    /// <summary>
    /// Sets the column name for predicted labels.
    /// </summary>
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public string GetPredictionCol() => ParamMap.Get("predictionCol").Value;

    /// <summary>
    /// Load a LinearRegressionModel from the specified path.
    /// </summary>
    public static LinearRegressionModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        return new LinearRegressionModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
