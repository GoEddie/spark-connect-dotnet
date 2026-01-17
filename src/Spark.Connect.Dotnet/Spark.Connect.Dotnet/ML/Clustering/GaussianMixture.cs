using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Clustering;

/// <summary>
/// Gaussian Mixture Model (GMM) clustering algorithm.
/// </summary>
public class GaussianMixture : Estimator<GaussianMixtureModel>
{
    public GaussianMixture() : this(DefaultParams.Clone())
    {
    }

    public GaussianMixture(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("gmm"), "org.apache.spark.ml.clustering.GaussianMixture", DefaultParams.Clone())
    {
    }

    public GaussianMixture(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("aggregationDepth", 2),
        new("featuresCol", "features"),
        new("k", 2),
        new("maxIter", 100),
        new("predictionCol", "prediction"),
        new("probabilityCol", "probability"),
        new("seed", 0L),
        new("tol", 0.01),
        new("weightCol", "")
    ]);

    /// <summary>
    /// Sets the aggregation depth for treeAggregate.
    /// </summary>
    public void SetAggregationDepth(int aggregationDepth) => ParamMap.Add("aggregationDepth", aggregationDepth);
    public int GetAggregationDepth() => ParamMap.Get("aggregationDepth").Value;

    /// <summary>
    /// Sets the features column name.
    /// </summary>
    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public string GetFeaturesCol() => ParamMap.Get("featuresCol").Value;

    /// <summary>
    /// Sets the number of independent Gaussians in the mixture model.
    /// </summary>
    public void SetK(int k) => ParamMap.Add("k", k);
    public int GetK() => ParamMap.Get("k").Value;

    /// <summary>
    /// Sets the maximum number of iterations.
    /// </summary>
    public void SetMaxIter(int maxIter) => ParamMap.Add("maxIter", maxIter);
    public int GetMaxIter() => ParamMap.Get("maxIter").Value;

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
    /// Sets the random seed.
    /// </summary>
    public void SetSeed(long seed) => ParamMap.Add("seed", seed);
    public long GetSeed() => ParamMap.Get("seed").Value;

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

/// <summary>
/// Model fitted by GaussianMixture.
/// </summary>
public class GaussianMixtureModel : Model
{
    private const string ClassName = "org.apache.spark.ml.clustering.GaussianMixtureModel";

    public GaussianMixtureModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the number of features the model was trained on.
    /// </summary>
    public int NumFeatures => Fetch("numFeatures");

    /// <summary>
    /// Gets the weights for each Gaussian component.
    /// </summary>
    public List<double> Weights => Fetch("weights");

    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);
    public void SetProbabilityCol(string probabilityCol) => ParamMap.Add("probabilityCol", probabilityCol);

    public static GaussianMixtureModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, GaussianMixture.DefaultParams.Clone());
        return new GaussianMixtureModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
