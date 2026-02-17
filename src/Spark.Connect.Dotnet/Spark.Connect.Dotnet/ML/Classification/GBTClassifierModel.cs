using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Classification;

public class DecisionTreeRegressor : Transformer
{
    public static readonly ParamMap DefaultParams = new([]);
    public DecisionTreeRegressor(string uid, string className, ObjectRef objRef, SparkSession sparkSession, ParamMap defaultParams) : base(uid, className, objRef, sparkSession, defaultParams)
    {
    }

    protected DecisionTreeRegressor(SparkSession sparkSession, string className, ParamMap paramMap) : base(sparkSession, className, paramMap)
    {
    }
}
public class DecisionTreeRegressionModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap)
    : Model(uid, ClassName, objRef, sparkSession, paramMap)
{
    private const string ClassName = "org.apache.spark.ml.classification.DecisionTreeRegressionModel";
}

/// <summary>
/// Gradient-Boosted Trees (GBTs) learning algorithm for classification. It supports binary labels, as well as both continuous and categorical features.
///
/// 
/// </summary>
public class GBTClassifierModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap)
    : Model(uid, ClassName, objRef, sparkSession, paramMap)
{
    private const string ClassName = "org.apache.spark.ml.classification.GBTClassifierModel";

    /// <summary>
    /// Load a `GBTClassifierModel` that was previously saved to disk on the Spark Connect server
    /// </summary>
    /// <param name="path">Where to read the `GBTClassifierModel` from</param>
    /// <param name="sparkSession">A `SparkSession` to read the model through</param>
    /// <returns>`GBTClassifierModel`</returns>
    public static GBTClassifierModel Load(string path, SparkSession sparkSession)
    {
        var mlResult = Transformer.Load(path, sparkSession, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, GBTClassifier.DefaultParams.Clone());

        var loadedModel = new GBTClassifierModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, sparkSession, paramMap);

        return loadedModel;
    }
    
    
    /// <summary>
    /// Sets the name of the column containing feature vectors for the model
    /// </summary>
    /// <param name="featuresCol">The name of the features column</param>
    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);

    /// <summary>
    /// Retrieves the name of the column used to store feature vectors.
    /// </summary>
    /// <returns>A string representing the name of the features column.</returns>
    public string GetFeaturesCol() => ParamMap.Get("featuresCol").Value;

    /// <summary>
    /// Sets the name of the column containing the label for the `GBTClassifier`.
    /// </summary>
    /// <param name="labelCol">The name of the label column</param>
    public void SetLabelCol(string labelCol) => ParamMap.Add("labelCol", labelCol);

    /// <summary>
    /// Retrieves the name of the column used as the label in the dataset.
    /// </summary>
    /// <returns>A string representing the label column name.</returns>
    public string GetLabelCol() => ParamMap.Get("labelCol").Value;

    /// <summary>
    /// Set the column name for predicted labels in the resulting DataFrame
    /// </summary>
    /// <param name="predictionCol">The name of the column to store the predictions</param>
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);

    /// <summary>
    /// Gets the name of the column used for model prediction output.
    /// </summary>
    /// <returns>The string name of the prediction column.</returns>
    public string GetPredictionCol() => ParamMap.Get("predictionCol").Value;

    /// <summary>
    /// Set the maximum depth of the tree for the `GBTClassifier`.
    /// </summary>
    /// <param name="maxDepth">The maximum depth of the tree (non-negative integer). Deeper trees are more expressive but may lead to overfitting.</param>
    public void SetMaxDepth(int maxDepth) => ParamMap.Add("maxDepth", maxDepth);

    /// <summary>
    /// Gets the maximum depth of the tree allowed during model training.
    /// </summary>
    /// <returns>The maximum depth of the tree as an integer.</returns>
    public int GetMaxDepth() => ParamMap.Get("maxDepth").Value;

    /// <summary>
    /// Sets the maximum number of bins to be used for splitting features in a decision tree.
    /// </summary>
    /// <param name="maxBins">The maximum number of bins, must be at least 2</param>
    public void SetMaxBins(int maxBins) => ParamMap.Add("maxBins", maxBins);

    /// <summary>
    /// Gets the maximum number of bins used for discretizing continuous features and for choosing how to split features at internal tree nodes.
    /// </summary>
    /// <returns>The maximum number of bins</returns>
    public int GetMaxBins() => ParamMap.Get("maxBins").Value;

    /// <summary>
    /// Sets the minimum number of instances each leaf node must have.
    /// </summary>
    /// <param name="minInstancesPerNode">The minimum number of instances per leaf node. Defaults to 1.</param>
    public void SetMinInstancesPerNode(int minInstancesPerNode) => ParamMap.Add("minInstancesPerNode", minInstancesPerNode);

    /// <summary>
    /// Retrieves the minimum number of instances required per node for the gradient-boosted tree.
    /// </summary>
    /// <returns>The minimum number of instances per node.</returns>
    public int GetMinInstancesPerNode() => ParamMap.Get("minInstancesPerNode").Value;

    /// <summary>
    /// Sets the minimum information gain for a split to be considered at a tree node.
    /// </summary>
    /// <param name="minInfoGain">The minimum information gain required for further splitting.</param>
    public void SetMinInfoGain(double minInfoGain) => ParamMap.Add("minInfoGain", minInfoGain);

    /// <summary>
    /// Retrieves the minimum information gain required for a split to be considered valid in a decision tree.
    /// </summary>
    /// <returns>The minimum information gain as a double.</returns>
    public double GetMinInfoGain() => ParamMap.Get("minInfoGain").Value;

    /// <summary>
    /// Sets the maximum amount of memory in megabytes to be used for histogram aggregation in training.
    /// </summary>
    /// <param name="maxMemoryInMB">The maximum memory limit in megabytes</param>
    public void SetMaxMemoryInMB(int maxMemoryInMB) => ParamMap.Add("maxMemoryInMB", maxMemoryInMB);

    /// <summary>
    /// Retrieves the maximum memory in MB allocated for the Gradient-Boosted Trees (GBT) classifier
    /// </summary>
    /// <returns>An integer representing the maximum memory in MB</returns>
    public int GetMaxMemoryInMB() => ParamMap.Get("maxMemoryInMB").Value;

    /// <summary>
    /// Sets whether to cache node IDs during training for this `GBTClassifier`.
    /// </summary>
    /// <param name="cacheNodeIds">
    /// A boolean value indicating whether to cache node IDs. This can improve performance for large datasets or models with many iterations.
    /// </param>
    public void SetCacheNodeIds(bool cacheNodeIds) => ParamMap.Add("cacheNodeIds", cacheNodeIds);

    /// <summary>
    /// Retrieves the value of the `cacheNodeIds` parameter, indicating whether to cache node IDs during transformations.
    /// </summary>
    /// <returns>A boolean value representing the state of the `cacheNodeIds` parameter.</returns>
    public bool GetCacheNodeIds() => ParamMap.Get("cacheNodeIds").Value;

    /// <summary>
    /// Sets the checkpoint interval for the `GBTClassifier`.
    /// </summary>
    /// <param name="checkpointInterval">Specifies how often to checkpoint the model (i.e., save intermediate results). Setting a value of -1 disables checkpointing.</param>
    public void SetCheckpointInterval(int checkpointInterval) => ParamMap.Add("checkpointInterval", checkpointInterval);

    /// <summary>
    /// Gets the checkpoint interval for the model. The checkpoint interval specifies how often
    /// to save checkpoints during the training process.
    /// </summary>
    /// <returns>The checkpoint interval as an integer.</returns>
    public int GetCheckpointInterval() => ParamMap.Get("checkpointInterval").Value;

    /// <summary>
    /// Sets the loss type for the `GBTClassifier`.
    /// </summary>
    /// <param name="lossType">The loss function to use. Supported options: "logistic" (default), "squared", etc.</param>
    public void SetLossType(string lossType) => ParamMap.Add("lossType", lossType);

    /// <summary>
    /// Retrieves the loss type parameter value used for optimization in the `GBTClassifier`.
    /// </summary>
    /// <returns>The loss type as a string</returns>
    public string GetLossType() => ParamMap.Get("lossType").Value;

    /// <summary>
    /// Sets the maximum number of iterations for the GBTClassifier.
    /// </summary>
    /// <param name="maxIter">The maximum number of iterations to run the algorithm</param>
    public void SetMaxIter(int maxIter) => ParamMap.Add("maxIter", maxIter);

    /// <summary>
    /// Returns the maximum number of iterations for training the GBTClassifier.
    /// </summary>
    /// <returns>The maximum number of iterations as an integer.</returns>
    public int GetMaxIter() => ParamMap.Get("maxIter").Value;

    /// <summary>
    /// Sets the step size parameter, which controls the learning rate for the gradient boosting algorithm.
    /// </summary>
    /// <param name="stepSize">The step size for each iteration. Must be greater than 0 and typically less than or equal to 1 for proper convergence.</param>
    public void SetStepSize(double stepSize) => ParamMap.Add("stepSize", stepSize);

    /// <summary>
    /// Gets the step size parameter for gradient descent iterations in the GBTClassifier.
    /// </summary>
    /// <returns>The step size value as a double.</returns>
    public double GetStepSize() => ParamMap.Get("stepSize").Value;

    /// <summary>
    /// Sets the seed for random number generation in model training to ensure reproducibility.
    /// </summary>
    /// <param name="seed">The seed value to use for random number generation.</param>
    public void SetSeed(int seed) => ParamMap.Add("seed", seed);

    /// <summary>
    /// Retrieves the seed value used for random number generation in the `GBTClassifier`.
    /// </summary>
    /// <returns>Seed value as an integer</returns>
    public int GetSeed() => ParamMap.Get("seed").Value;

    /// <summary>
    /// Sets the subsampling rate for training data used by the `GBTClassifier`.
    /// </summary>
    /// <param name="subsamplingRate">
    /// The fraction of the training data used for learning. Must be in the range (0, 1].
    /// </param>
    public void SetSubsamplingRate(double subsamplingRate) => ParamMap.Add("subsamplingRate", subsamplingRate);

    /// <summary>
    /// Gets the subsampling rate used for training. This value represents the fraction of the training data used for learning,
    /// ranging from 0.0 to 1.0.
    /// </summary>
    /// <returns>The subsampling rate as a double.</returns>
    public double GetSubsamplingRate() => ParamMap.Get("subsamplingRate").Value;

    /// <summary>
    /// Set the impurity criterion for the `GBTClassifier`.
    /// </summary>
    /// <param name="impurity">The impurity criterion to set, for example: "variance".</param>
    public void SetImpurity(string impurity) => ParamMap.Add("impurity", impurity);

    /// <summary>
    /// Gets the impurity criterion used for information gain calculation.
    /// </summary>
    /// <returns>The impurity used, represented as a string.</returns>
    public string GetImpurity() => ParamMap.Get("impurity").Value;

    /// <summary>
    /// Sets the strategy for selecting a subset of features at each iteration.
    /// </summary>
    /// <param name="featureSubsetStrategy">The feature subset strategy to use. Examples include "auto", "all", "sqrt", "log2", etc.</param>
    public void SetFeatureSubsetStrategy(string featureSubsetStrategy) => ParamMap.Add("featureSubsetStrategy", featureSubsetStrategy);

    /// <summary>
    /// Gets the feature subset strategy value used in the GBT model.
    /// </summary>
    /// <returns>A string representing the feature subset strategy.</returns>
    public string GetFeatureSubsetStrategy() => ParamMap.Get("featureSubsetStrategy").Value;

    /// <summary>
    /// Sets the validation tolerance for the `GBTClassifier`.
    /// </summary>
    /// <param name="validationTol">Relative tolerance to stop validation, which helps control overfitting</param>
    public void SetValidationTol(double validationTol) => ParamMap.Add("validationTol", validationTol);

    /// <summary>
    /// Retrieves the validation tolerance used for early stopping.
    /// </summary>
    /// <returns>The validation tolerance as a double value.</returns>
    public double GetValidationTol() => ParamMap.Get("validationTol").Value;

    /// <summary>
    /// Sets the validation indicator column. This column is used to specify rows
    /// that should be included or excluded from validation during model training.
    /// </summary>
    /// <param name="validationIndicatorCol">The name of the validation indicator column</param>
    public void SetValidationIndicatorCol(string validationIndicatorCol) => ParamMap.Add("validationIndicatorCol", validationIndicatorCol);

    /// <summary>
    /// Retrieves the value of the "validationIndicatorCol" parameter.
    /// </summary>
    /// <returns>The value of the "validationIndicatorCol" parameter as a string.</returns>
    public string GetValidationIndicatorCol() => ParamMap.Get("validationIndicatorCol").Value;

    /// <summary>
    /// Sets the name of the column to store the leaf index values of each tree in the model.
    /// </summary>
    /// <param name="leafCol">The name of the column to hold tree leaf indices.</param>
    public void SetLeafCol(string leafCol) => ParamMap.Add("leafCol", leafCol);

    /// <summary>
    /// Gets the name of the column that contains the predicted leaf index information.
    /// </summary>
    /// <returns>The name of the leaf index prediction column.</returns>
    public string GetLeafCol() => ParamMap.Get("leafCol").Value;

    /// <summary>
    /// Sets the minimum fraction of the weighted total of nodes to be present at each node for splitting.
    /// </summary>
    /// <param name="minWeightFractionPerNode">The minimum fraction of the weighted total of nodes required for splitting, default is 0.0</param>
    public void SetMinWeightFractionPerNode(double minWeightFractionPerNode) => ParamMap.Add("minWeightFractionPerNode", minWeightFractionPerNode);

    /// <summary>
    /// Retrieves the minimum weight fraction per node used in the GBT model
    /// to restrict the fraction of the total weight of instances in each leaf node.
    /// </summary>
    /// <returns>The minimum weight fraction per node as a double.</returns>
    public double GetMinWeightFractionPerNode() => ParamMap.Get("minWeightFractionPerNode").Value;

    /// <summary>
    /// Sets the column name for instance weights in the model training process
    /// </summary>
    /// <param name="weightCol">The name of the column containing the weights</param>
    public void SetWeightCol(string weightCol) => ParamMap.Add("weightCol", weightCol);

    /// <summary>
    /// Retrieves the name of the column that represents weights for individual rows.
    /// </summary>
    /// <returns>The name of the weight column.</returns>
    public string GetWeightCol() => ParamMap.Get("weightCol").Value;

    /// <summary>
    /// Return the weights for each tree
    /// </summary>
    /// <returns>Tree weights</returns>
    public double?[] TreeWeights()
    {
        return Fetch("treeWeights");
    }

    public List<DecisionTreeRegressionModel> Trees()
    {
        var newObjectRefs = FetchObjectRefs("trees");
        var newDecisionTreeModels = newObjectRefs.Split(",")
                                                                .Select(objRef =>
                                                                    new DecisionTreeRegressionModel(IdentifiableHelper.RandomUID("dtrm-static"), 
                                                                        new ObjectRef() { Id = objRef, }, 
                                                                        SparkSession, 
                                                                        DecisionTreeRegressor.DefaultParams.Clone())).ToList();

        return newDecisionTreeModels;
    }
    
    
    public float Predict(Vector value)
    {
     throw new NotImplementedException("Cannot pass a vector to predict"); //TODO: Get predict working
        // return Fetch("predict", value);
    }
}