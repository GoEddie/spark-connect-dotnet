using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.ML.Regression;

/// <summary>
/// Linear regression. The learning objective is to minimize the specified loss function, with regularization.
/// This supports two kinds of regularization: L2 (ridge regression) and L1 (Lasso).
/// </summary>
public class LinearRegression : Estimator<LinearRegressionModel>
{
    public LinearRegression() : this(DefaultParams.Clone())
    {
    }

    public LinearRegression(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("linreg"), "org.apache.spark.ml.regression.LinearRegression", DefaultParams.Clone())
    {
    }

    public LinearRegression(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("aggregationDepth", 2),
        new("elasticNetParam", 0.0),
        new("epsilon", 1.35),
        new("featuresCol", "features"),
        new("fitIntercept", true),
        new("labelCol", "label"),
        new("loss", "squaredError"),
        new("maxBlockSizeInMB", 0.0),
        new("maxIter", 100),
        new("predictionCol", "prediction"),
        new("regParam", 0.0),
        new("solver", "auto"),
        new("standardization", true),
        new("tol", 1.0E-6),
        new("weightCol", "")
    ]);

    /// <summary>
    /// Sets the aggregation depth for the treeAggregate.
    /// </summary>
    public void SetAggregationDepth(int aggregationDepth) => ParamMap.Add("aggregationDepth", aggregationDepth);
    public int GetAggregationDepth() => ParamMap.Get("aggregationDepth").Value;

    /// <summary>
    /// Sets the ElasticNet mixing parameter. For alpha = 0, the penalty is an L2 penalty.
    /// For alpha = 1, it is an L1 penalty.
    /// </summary>
    public void SetElasticNetParam(double elasticNetParam) => ParamMap.Add("elasticNetParam", elasticNetParam);
    public double GetElasticNetParam() => ParamMap.Get("elasticNetParam").Value;

    /// <summary>
    /// Sets the shape parameter to control the amount of robustness.
    /// Must be > 1.0. Only valid for Huber loss.
    /// </summary>
    public void SetEpsilon(double epsilon) => ParamMap.Add("epsilon", epsilon);
    public double GetEpsilon() => ParamMap.Get("epsilon").Value;

    /// <summary>
    /// Sets the name of the column containing feature vectors.
    /// </summary>
    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public string GetFeaturesCol() => ParamMap.Get("featuresCol").Value;

    /// <summary>
    /// Sets whether to fit an intercept term.
    /// </summary>
    public void SetFitIntercept(bool fitIntercept) => ParamMap.Add("fitIntercept", fitIntercept);
    public bool GetFitIntercept() => ParamMap.Get("fitIntercept").Value;

    /// <summary>
    /// Sets the name of the label column.
    /// </summary>
    public void SetLabelCol(string labelCol) => ParamMap.Add("labelCol", labelCol);
    public string GetLabelCol() => ParamMap.Get("labelCol").Value;

    /// <summary>
    /// Sets the loss function to be optimized. Supported: squaredError, huber.
    /// </summary>
    public void SetLoss(string loss) => ParamMap.Add("loss", loss);
    public string GetLoss() => ParamMap.Get("loss").Value;

    /// <summary>
    /// Sets the maximum block size in MB for stacking input data.
    /// </summary>
    public void SetMaxBlockSizeInMB(double maxBlockSizeInMB) => ParamMap.Add("maxBlockSizeInMB", maxBlockSizeInMB);
    public double GetMaxBlockSizeInMB() => ParamMap.Get("maxBlockSizeInMB").Value;

    /// <summary>
    /// Sets the maximum number of iterations.
    /// </summary>
    public void SetMaxIter(int maxIter) => ParamMap.Add("maxIter", maxIter);
    public int GetMaxIter() => ParamMap.Get("maxIter").Value;

    /// <summary>
    /// Sets the column name for predicted labels.
    /// </summary>
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public string GetPredictionCol() => ParamMap.Get("predictionCol").Value;

    /// <summary>
    /// Sets the regularization parameter.
    /// </summary>
    public void SetRegParam(double regParam) => ParamMap.Add("regParam", regParam);
    public double GetRegParam() => ParamMap.Get("regParam").Value;

    /// <summary>
    /// Sets the solver algorithm. Supported options: auto, normal, l-bfgs.
    /// </summary>
    public void SetSolver(string solver) => ParamMap.Add("solver", solver);
    public string GetSolver() => ParamMap.Get("solver").Value;

    /// <summary>
    /// Sets whether to standardize the training features before fitting the model.
    /// </summary>
    public void SetStandardization(bool standardization) => ParamMap.Add("standardization", standardization);
    public bool GetStandardization() => ParamMap.Get("standardization").Value;

    /// <summary>
    /// Sets the convergence tolerance.
    /// </summary>
    public void SetTol(double tol) => ParamMap.Add("tol", tol);
    public double GetTol() => ParamMap.Get("tol").Value;

    /// <summary>
    /// Sets the weight column name.
    /// </summary>
    public void SetWeightCol(string weightCol) => ParamMap.Add("weightCol", weightCol);
    public string GetWeightCol() => ParamMap.Get("weightCol").Value;
}
