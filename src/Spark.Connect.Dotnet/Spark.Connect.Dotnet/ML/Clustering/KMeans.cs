using System.Runtime.CompilerServices;
using Spark.Connect.Dotnet.ML.LinAlg;
using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.ML.Clustering;

/// <summary>
/// K-means clustering algorithm.
/// </summary>
public class KMeans : Estimator<KMeansModel>
{
    public KMeans() : this(DefaultParams.Clone())
    {
    }

    public KMeans(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("kmeans"), "org.apache.spark.ml.clustering.KMeans", DefaultParams.Clone())
    {
    }

    public KMeans(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("distanceMeasure", "euclidean"),
        new("featuresCol", "features"),
        new("initMode", "k-means||"),
        new("initSteps", 2),
        new("k", 2),
        new("maxIter", 20),
        new("predictionCol", "prediction"),
        new("seed", 0L),
        new("tol", 1.0E-4),
        new("weightCol", "")
    ]);

    /// <summary>
    /// Sets the distance measure. Supported: euclidean, cosine.
    /// </summary>
    public void SetDistanceMeasure(string distanceMeasure) => ParamMap.Add("distanceMeasure", distanceMeasure);
    public string GetDistanceMeasure() => ParamMap.Get("distanceMeasure").Value;

    /// <summary>
    /// Sets the features column name.
    /// </summary>
    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public string GetFeaturesCol() => ParamMap.Get("featuresCol").Value;

    /// <summary>
    /// Sets the initialization algorithm. Supported: "k-means||" (default), "random".
    /// </summary>
    public void SetInitMode(string initMode) => ParamMap.Add("initMode", initMode);
    public string GetInitMode() => ParamMap.Get("initMode").Value;

    /// <summary>
    /// Sets the number of steps for k-means|| initialization.
    /// </summary>
    public void SetInitSteps(int initSteps) => ParamMap.Add("initSteps", initSteps);
    public int GetInitSteps() => ParamMap.Get("initSteps").Value;

    /// <summary>
    /// Sets the number of clusters to create.
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
/// Model fitted by KMeans.
/// </summary>
public class KMeansModel : Model
{
    private const string ClassName = "org.apache.spark.ml.clustering.KMeansModel";

    public KMeansModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the cluster centers as a list of vectors.
    /// </summary>
    public List<List<double>> ClusterCenters => Fetch("clusterCenters");

    /// <summary>
    /// Gets the number of clusters (K).
    /// </summary>
    public int K => Fetch("k");

    public void SetFeaturesCol(string featuresCol) => ParamMap.Add("featuresCol", featuresCol);
    public void SetPredictionCol(string predictionCol) => ParamMap.Add("predictionCol", predictionCol);

    public static KMeansModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, KMeans.DefaultParams.Clone());
        return new KMeansModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }

    /// <summary>
    /// Predict the cluster index for the given feature vector.
    /// Note: For batch predictions, use Transform() directly for better performance.
    /// </summary>
    /// <param name="value">A feature vector (DenseVector or SparseVector)</param>
    /// <returns>The predicted cluster index as an int</returns>
    public int Predict(Vector value)
    {
        if (value is not IUserDefinedType udt)
        {
            throw new ArgumentException("Vector must be a DenseVector or SparseVector", nameof(value));
        }

        var featuresCol = ParamMap.Get("featuresCol")?.Value as string ?? "features";
        var predictionCol = ParamMap.Get("predictionCol")?.Value as string ?? "prediction";

        var schema = new StructType(new[]
        {
            new StructField(featuresCol, new VectorUDT(), false)
        });

        var data = new List<ValueTuple<IUserDefinedType>> { new ValueTuple<IUserDefinedType>(udt) };
        var df = SparkSession.CreateDataFrame(data.Cast<ITuple>(), schema);

        var result = Transform(df);
        var row = result.First();

        return Convert.ToInt32(row.Get(predictionCol));
    }
}
