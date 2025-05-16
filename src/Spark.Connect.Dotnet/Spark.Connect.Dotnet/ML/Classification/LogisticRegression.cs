using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.ML.Classification;

/// <summary>
/// Logistic regression. This class supports multinomial logistic (softmax) and binomial logistic regression.
/// </summary>
public class LogisticRegression : Estimator<LogisticRegressionModel>
{
    /// <summary>
    /// Represents a logistic regression model for binary classification.
    /// </summary>
    /// <remarks>
    /// This class encapsulates an instance of a logistic regression model. It is
    /// derived from the `Estimator` class and allows fitting the model to a dataset.
    /// </remarks>
    public LogisticRegression() : this(DefaultParams.Clone())
    {
        
    }

    /// <summary>
    /// Logistic regression, implements multinomial logistic (softmax) and binomial logistic regression.
    /// Provides methods to configure training parameters and generate a logistic regression model.
    /// </summary>
    public LogisticRegression(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("logreg-static"), "org.apache.spark.ml.classification.LogisticRegression", DefaultParams.Clone())
    {
        
    }

    /// <summary>
    /// Logistic regression. This class supports multinomial logistic (softmax) and binomial logistic regression.
    /// </summary>
    public LogisticRegression(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
        
    }
    
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

    /// <summary>
    /// Sets the aggregation depth for the treeAggregate (greater values are more memory-intensive).
    /// </summary>
    /// <param name="aggregationDepth">The suggested depth of the tree used for aggregation. Must be an integer greater than or equal to 2.</param>
    public void SetAggregationDepth(int aggregationDepth) => ParamMap.Add("aggregationDepth", aggregationDepth);

    /// <summary>
    /// Gets the value of the "aggregationDepth" parameter for the logistic regression model.
    /// </summary>
    /// <returns>The value of the "aggregationDepth" parameter.</returns>
    public int GetAggregationDepth() => ParamMap.Get("aggregationDepth").Value;

    /// <summary>
    /// Sets the ElasticNet mixing parameter for the `LogisticRegression` model.
    /// </summary>
    /// <param name="elasticNetParam">
    /// The ElasticNet mixing parameter, which combines L1 and L2 regularization.
    /// Valid values are in the range [0, 1]. A value of 0 corresponds to L2 regularization,
    /// while a value of 1 corresponds to L1 regularization.
    /// </param>
    public void SetElasticNetParam(double elasticNetParam) => ParamMap.Add("elasticNetParam", elasticNetParam);

    /// <summary>
    /// Retrieves the value of the ElasticNet parameter used in the logistic regression model.
    /// </summary>
    /// <returns>The ElasticNet regularization parameter as a double.</returns>
    public double GetElasticNetParam() => ParamMap.Get("elasticNetParam").Value;

    /// <summary>
    /// Sets the family parameter for the logistic regression model.
    /// </summary>
    /// <param name="family">The name of the family for the logistic regression model. Supported options: "auto", "binomial", "multinomial".</param>
    public void SetFamily(string family) => ParamMap.Add("family", family);

    /// <summary>
    /// Retrieves the family parameter, which specifies the type of model family to be used
    /// (e.g., "auto", "binomial", "multinomial").
    /// </summary>
    /// <returns>The model family as a string.</returns>
    public string GetFamily() => ParamMap.Get("family").Value;

    /// <summary>
    /// Sets the name of the column containing feature vectors
    /// </summary>
    /// <param name="featuresCol">The name of the column containing feature vectors</param>
    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);

    /// <summary>
    /// Retrieves the name of the column containing feature vectors for model training and predictions.
    /// </summary>
    /// <returns>A string representing the name of the features column.</returns>
    public string GetFeaturesCol() => ParamMap.Get("featuresCol").Value;

    /// <summary>
    /// Specifies whether the model should include an intercept (a constant term) in the computation of the logistic regression.
    /// </summary>
    /// <param name="fitIntercept">A boolean value indicating whether to include an intercept in the model. Set to true to include an intercept, or false to omit it.</param>
    public void SetFitIntercept(bool fitIntercept) => ParamMap.Add("fitIntercept", fitIntercept);

    /// <summary>
    /// Retrieves the value of the `fitIntercept` parameter, which indicates whether the model should include an intercept term.
    /// </summary>
    /// <returns>A boolean value representing whether an intercept is included in the model.</returns>
    public bool GetFitIntercept() => ParamMap.Get("fitIntercept").Value;

    /// <summary>
    /// Sets the name of the label column. The label column specifies the column that contains
    /// the label data in the dataset.
    /// </summary>
    /// <param name="labelCol">The name of the label column</param>
    public void SetLabelCol(string labelCol) => ParamMap.Add("labelCol", labelCol);

    /// <summary>
    /// Retrieves the column name used for the label in the dataset.
    /// </summary>
    /// <returns>The name of the label column as a string.</returns>
    public string GetLabelCol() => ParamMap.Get("labelCol").Value;

    /// <summary>
    /// Sets the maximum block size in MB for storing intermediate computation results.
    /// </summary>
    /// <param name="maxBlockSizeInMB">The maximum block size in MB to be used</param>
    public void SetMaxBlockSizeInMB(double maxBlockSizeInMB) => ParamMap.Add("maxBlockSizeInMB", maxBlockSizeInMB);

    /// <summary>
    /// Returns the maximum block size in MB for matrix factorization computations set in the logistic regression model.
    /// </summary>
    /// <returns>The maximum block size in MB as a double value.</returns>
    public double GetMaxBlockSizeInMB() => ParamMap.Get("maxBlockSizeInMB").Value;

    /// <summary>
    /// Sets the maximum number of iterations for the logistic regression algorithm.
    /// </summary>
    /// <param name="maxIter">The maximum number of iterations to run.</param>
    public void SetMaxIter(int maxIter) => ParamMap.Add("maxIter", maxIter);

    /// <summary>
    /// Retrieves the maximum number of iterations for the logistic regression algorithm.
    /// </summary>
    /// <returns>The maximum number of iterations as an integer.</returns>
    public int GetMaxIter() => ParamMap.Get("maxIter").Value;

    /// <summary>
    /// Sets the column name for predicted labels.
    /// </summary>
    /// <param name="predictionCol">The name of the column where predictions are stored.</param>
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);

    /// <summary>
    /// Gets the name of the column where predictions are stored.
    /// </summary>
    /// <returns>The name of the prediction column as a string.</returns>
    public string GetPredictionCol() => ParamMap.Get("predictionCol").Value;

    /// <summary>
    /// Specifies the column name for the predicted class probabilities in the output.
    /// </summary>
    /// <param name="probabilityCol">The name of the column where probabilities will be stored</param>
    public void SetProbabilityCol(string probabilityCol) => ParamMap.Add("probabilityCol", probabilityCol);

    /// <summary>
    /// Retrieves the name of the column where the predicted class probabilities are stored.
    /// </summary>
    /// <returns>The name of the column storing predicted class probabilities</returns>
    public string GetProbabilityCol() => ParamMap.Get("probabilityCol").Value;

    /// <summary>
    /// Set the name of the column that holds the raw prediction (confidence scores) for each class.
    /// </summary>
    /// <param name="rawPredictionCol">The name of the column to store the raw prediction scores</param>
    public void SetRawPredictionCol(string rawPredictionCol) => ParamMap.Add("rawPredictionCol", rawPredictionCol);

    /// <summary>
    /// Retrieve the name of the column that contains raw prediction vector values for Logistic Regression.
    /// </summary>
    /// <returns>The name of the raw prediction column as a string.</returns>
    public string GetRawPredictionCol() => ParamMap.Get("rawPredictionCol").Value;

    /// <summary>
    /// Sets the regularization parameter for the logistic regression model.
    /// </summary>
    /// <param name="regParam">The value of the regularization parameter to be set.</param>
    public void SetRegParam(double regParam) => ParamMap.Add("regParam", regParam);

    /// <summary>
    /// Retrieves the value of the regularization parameter (regParam) used in the logistic regression model.
    /// </summary>
    /// <returns>The regularization parameter as a double value.</returns>
    public double GetRegParam() => ParamMap.Get("regParam").Value;

    /// <summary>
    /// Set the standardization flag for the LogisticRegression algorithm.
    /// </summary>
    /// <param name="standardization">
    /// A boolean value indicating whether the features should be standardized
    /// before model training. If true, the features will be normalized to
    /// have zero mean and unit variance.
    /// </param>
    public void SetStandardization(bool standardization) => ParamMap.Add("standardization", standardization);

    /// <summary>
    /// Gets the value of the `standardization` parameter. Indicates whether the features should be standardized before training.
    /// </summary>
    /// <returns>A boolean indicating if standardization is enabled.</returns>
    public bool GetStandardization() => ParamMap.Get("standardization").Value;

    /// <summary>
    /// Sets the threshold for binary classification prediction.
    /// </summary>
    /// <param name="threshold">The threshold for predicting positive class. Values must be between 0.0 and 1.0.</param>
    public void SetThreshold(double threshold) => ParamMap.Add("threshold", threshold);

    /// <summary>
    /// Gets the threshold used for classification.
    /// This value determines the classification boundary for predictions.
    /// </summary>
    /// <returns>The threshold value</returns>
    public double GetThreshold() => ParamMap.Get("threshold").Value;

    /// <summary>
    /// Set the convergence tolerance of the iterative algorithm
    /// </summary>
    /// <param name="tol">The tolerance value to be used for stopping criterion of the iterative algorithm</param>
    public void SetTol(double tol) => ParamMap.Add("tol", tol);

    /// <summary>
    /// Gets the convergence tolerance for iterative algorithms.
    /// </summary>
    /// <returns>The convergence tolerance as a `double`.</returns>
    public double GetTol() => ParamMap.Get("tol").Value;
}