using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Classification;

/// <summary>
/// Random Forest learning algorithm for classification.
/// It supports both binary and multiclass labels, as well as both continuous and categorical features.
/// </summary>
public class RandomForestClassifier : Estimator<RandomForestClassifierModel>
{
    public RandomForestClassifier() : this(DefaultParams.Clone())
    {
    }

    public RandomForestClassifier(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("rfc"), "org.apache.spark.ml.classification.RandomForestClassifier", DefaultParams.Clone())
    {
    }

    public RandomForestClassifier(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("bootstrap", true),
        new("cacheNodeIds", false),
        new("checkpointInterval", 10),
        new("featureSubsetStrategy", "auto"),
        new("featuresCol", "features"),
        new("impurity", "gini"),
        new("labelCol", "label"),
        new("leafCol", ""),
        new("maxBins", 32),
        new("maxDepth", 5),
        new("maxMemoryInMB", 256),
        new("minInfoGain", 0.0),
        new("minInstancesPerNode", 1),
        new("minWeightFractionPerNode", 0.0),
        new("numTrees", 20),
        new("predictionCol", "prediction"),
        new("probabilityCol", "probability"),
        new("rawPredictionCol", "rawPrediction"),
        new("seed", 0L),
        new("subsamplingRate", 1.0),
        new("thresholds", null),
        new("weightCol", "")
    ]);

    public void SetBootstrap(bool bootstrap) => ParamMap.Add("bootstrap", bootstrap);
    public bool GetBootstrap() => ParamMap.Get("bootstrap").Value;

    public void SetCacheNodeIds(bool cacheNodeIds) => ParamMap.Add("cacheNodeIds", cacheNodeIds);
    public bool GetCacheNodeIds() => ParamMap.Get("cacheNodeIds").Value;

    public void SetCheckpointInterval(int checkpointInterval) => ParamMap.Add("checkpointInterval", checkpointInterval);
    public int GetCheckpointInterval() => ParamMap.Get("checkpointInterval").Value;

    /// <summary>
    /// Sets the strategy for selecting feature subset at each tree node.
    /// Supported: "auto", "all", "onethird", "sqrt", "log2", (0.0-1.0], [1-n].
    /// </summary>
    public void SetFeatureSubsetStrategy(string featureSubsetStrategy) => ParamMap.Add("featureSubsetStrategy", featureSubsetStrategy);
    public string GetFeatureSubsetStrategy() => ParamMap.Get("featureSubsetStrategy").Value;

    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public string GetFeaturesCol() => ParamMap.Get("featuresCol").Value;

    /// <summary>
    /// Sets the impurity measure. Supported: "gini" (default), "entropy".
    /// </summary>
    public void SetImpurity(string impurity) => ParamMap.Add("impurity", impurity);
    public string GetImpurity() => ParamMap.Get("impurity").Value;

    public void SetLabelCol(string labelCol) => ParamMap.Add("labelCol", labelCol);
    public string GetLabelCol() => ParamMap.Get("labelCol").Value;

    public void SetLeafCol(string leafCol) => ParamMap.Add("leafCol", leafCol);
    public string GetLeafCol() => ParamMap.Get("leafCol").Value;

    public void SetMaxBins(int maxBins) => ParamMap.Add("maxBins", maxBins);
    public int GetMaxBins() => ParamMap.Get("maxBins").Value;

    public void SetMaxDepth(int maxDepth) => ParamMap.Add("maxDepth", maxDepth);
    public int GetMaxDepth() => ParamMap.Get("maxDepth").Value;

    public void SetMaxMemoryInMB(int maxMemoryInMB) => ParamMap.Add("maxMemoryInMB", maxMemoryInMB);
    public int GetMaxMemoryInMB() => ParamMap.Get("maxMemoryInMB").Value;

    public void SetMinInfoGain(double minInfoGain) => ParamMap.Add("minInfoGain", minInfoGain);
    public double GetMinInfoGain() => ParamMap.Get("minInfoGain").Value;

    public void SetMinInstancesPerNode(int minInstancesPerNode) => ParamMap.Add("minInstancesPerNode", minInstancesPerNode);
    public int GetMinInstancesPerNode() => ParamMap.Get("minInstancesPerNode").Value;

    public void SetMinWeightFractionPerNode(double minWeightFractionPerNode) => ParamMap.Add("minWeightFractionPerNode", minWeightFractionPerNode);
    public double GetMinWeightFractionPerNode() => ParamMap.Get("minWeightFractionPerNode").Value;

    /// <summary>
    /// Sets the number of trees to train (at least 1).
    /// </summary>
    public void SetNumTrees(int numTrees) => ParamMap.Add("numTrees", numTrees);
    public int GetNumTrees() => ParamMap.Get("numTrees").Value;

    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public string GetPredictionCol() => ParamMap.Get("predictionCol").Value;

    public void SetProbabilityCol(string probabilityCol) => ParamMap.Add("probabilityCol", probabilityCol);
    public string GetProbabilityCol() => ParamMap.Get("probabilityCol").Value;

    public void SetRawPredictionCol(string rawPredictionCol) => ParamMap.Add("rawPredictionCol", rawPredictionCol);
    public string GetRawPredictionCol() => ParamMap.Get("rawPredictionCol").Value;

    public void SetSeed(long seed) => ParamMap.Add("seed", seed);
    public long GetSeed() => ParamMap.Get("seed").Value;

    /// <summary>
    /// Sets the fraction of the training data used for learning each decision tree.
    /// </summary>
    public void SetSubsamplingRate(double subsamplingRate) => ParamMap.Add("subsamplingRate", subsamplingRate);
    public double GetSubsamplingRate() => ParamMap.Get("subsamplingRate").Value;

    public void SetThresholds(double[] thresholds) => ParamMap.Add("thresholds", thresholds);
    public double[]? GetThresholds() => ParamMap.Get("thresholds").Value;

    public void SetWeightCol(string weightCol) => ParamMap.Add("weightCol", weightCol);
    public string GetWeightCol() => ParamMap.Get("weightCol").Value;
}

/// <summary>
/// Random Forest model for classification.
/// </summary>
public class RandomForestClassifierModel : Model
{
    private const string ClassName = "org.apache.spark.ml.classification.RandomForestClassifierModel";

    public RandomForestClassifierModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    public int NumTrees => Fetch("getNumTrees");
    public int TotalNumNodes => Fetch("totalNumNodes");
    public int NumFeatures => Fetch("numFeatures");
    public int NumClasses => Fetch("numClasses");
    public List<double> FeatureImportances => Fetch("featureImportances");

    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public void SetProbabilityCol(string probabilityCol) => ParamMap.Add("probabilityCol", probabilityCol);
    public void SetRawPredictionCol(string rawPredictionCol) => ParamMap.Add("rawPredictionCol", rawPredictionCol);

    public static RandomForestClassifierModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, RandomForestClassifier.DefaultParams.Clone());
        return new RandomForestClassifierModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
