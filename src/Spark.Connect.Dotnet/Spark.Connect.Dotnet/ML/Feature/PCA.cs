using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Principal Component Analysis (PCA) is a statistical method to find a rotation such that the
/// first coordinate has the largest variance possible, and each succeeding coordinate,
/// in turn, has the largest variance possible.
/// </summary>
public class PCA : Estimator<PCAModel>
{
    public PCA() : this(DefaultParams.Clone())
    {
    }

    public PCA(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("pca"), "org.apache.spark.ml.feature.PCA", DefaultParams.Clone())
    {
    }

    public PCA(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", ""),
        new("k", 0),
        new("outputCol", "")
    ]);

    /// <summary>
    /// Sets the input column name.
    /// </summary>
    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public string GetInputCol() => ParamMap.Get("inputCol").Value;

    /// <summary>
    /// Sets the number of principal components to compute.
    /// </summary>
    public void SetK(int k) => ParamMap.Add("k", k);
    public int GetK() => ParamMap.Get("k").Value;

    /// <summary>
    /// Sets the output column name.
    /// </summary>
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public string GetOutputCol() => ParamMap.Get("outputCol").Value;
}

/// <summary>
/// Model fitted by PCA. Transforms vectors to a lower dimensional space using PCA.
/// </summary>
public class PCAModel : Model
{
    private const string ClassName = "org.apache.spark.ml.feature.PCAModel";

    public PCAModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the principal components (transformation matrix).
    /// </summary>
    public List<List<double>> Pc => Fetch("pc");

    /// <summary>
    /// Gets the variance explained by each principal component.
    /// </summary>
    public List<double> ExplainedVariance => Fetch("explainedVariance");

    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);

    public static PCAModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, PCA.DefaultParams.Clone());
        return new PCAModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
