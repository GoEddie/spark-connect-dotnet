using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Classification;

/// <summary>
/// Decision tree learning algorithm for classification.
/// </summary>
public class DecisionTreeClassifier : Estimator<DecisionTreeClassifierModel>
{
    public DecisionTreeClassifier() : this(DefaultParams.Clone())
    {
    }

    public DecisionTreeClassifier(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("dtc"), "org.apache.spark.ml.classification.DecisionTreeClassifier", DefaultParams.Clone())
    {
    }

    public DecisionTreeClassifier(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("cacheNodeIds", false),
        new("checkpointInterval", 10),
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
        new("predictionCol", "prediction"),
        new("probabilityCol", "probability"),
        new("rawPredictionCol", "rawPrediction"),
        new("seed", 0L),
        new("thresholds", null),
        new("weightCol", "")
    ]);

    public void SetCacheNodeIds(bool cacheNodeIds) => ParamMap.Add("cacheNodeIds", cacheNodeIds);
    public bool GetCacheNodeIds() => ParamMap.Get("cacheNodeIds").Value;

    public void SetCheckpointInterval(int checkpointInterval) => ParamMap.Add("checkpointInterval", checkpointInterval);
    public int GetCheckpointInterval() => ParamMap.Get("checkpointInterval").Value;

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

    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public string GetPredictionCol() => ParamMap.Get("predictionCol").Value;

    public void SetProbabilityCol(string probabilityCol) => ParamMap.Add("probabilityCol", probabilityCol);
    public string GetProbabilityCol() => ParamMap.Get("probabilityCol").Value;

    public void SetRawPredictionCol(string rawPredictionCol) => ParamMap.Add("rawPredictionCol", rawPredictionCol);
    public string GetRawPredictionCol() => ParamMap.Get("rawPredictionCol").Value;

    public void SetSeed(long seed) => ParamMap.Add("seed", seed);
    public long GetSeed() => ParamMap.Get("seed").Value;

    public void SetThresholds(double[] thresholds) => ParamMap.Add("thresholds", thresholds);
    public double[]? GetThresholds() => ParamMap.Get("thresholds").Value;

    public void SetWeightCol(string weightCol) => ParamMap.Add("weightCol", weightCol);
    public string GetWeightCol() => ParamMap.Get("weightCol").Value;
}

/// <summary>
/// Decision tree model for classification.
/// </summary>
public class DecisionTreeClassifierModel : Model
{
    private const string ClassName = "org.apache.spark.ml.classification.DecisionTreeClassifierModel";

    public DecisionTreeClassifierModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    public int Depth => Fetch("depth");
    public int NumNodes => Fetch("numNodes");
    public int NumFeatures => Fetch("numFeatures");
    public int NumClasses => Fetch("numClasses");

    /// <summary>
    /// Gets the importance of each feature in the model.
    /// Values are normalized to sum to 1.
    /// </summary>
    public List<double> FeatureImportances => Fetch("featureImportances");

    /// <summary>
    /// Gets a human-readable description of the decision tree model.
    /// </summary>
    public string ToDebugString => Fetch("toDebugString");

    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public void SetProbabilityCol(string probabilityCol) => ParamMap.Add("probabilityCol", probabilityCol);
    public void SetRawPredictionCol(string rawPredictionCol) => ParamMap.Add("rawPredictionCol", rawPredictionCol);

    public static DecisionTreeClassifierModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DecisionTreeClassifier.DefaultParams.Clone());
        return new DecisionTreeClassifierModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
