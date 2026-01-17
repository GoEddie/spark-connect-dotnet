using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Rescales each feature individually to range [-1, 1] by dividing through the largest maximum absolute value
/// in each feature. It does not shift/center the data, and thus does not destroy any sparsity.
/// </summary>
public class MaxAbsScaler : Estimator<MaxAbsScalerModel>
{
    public MaxAbsScaler() : this(DefaultParams.Clone())
    {
    }

    public MaxAbsScaler(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("maxabsscaler"), "org.apache.spark.ml.feature.MaxAbsScaler", DefaultParams.Clone())
    {
    }

    public MaxAbsScaler(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", ""),
        new("outputCol", "")
    ]);

    /// <summary>
    /// Sets the input column name.
    /// </summary>
    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public string GetInputCol() => ParamMap.Get("inputCol").Value;

    /// <summary>
    /// Sets the output column name.
    /// </summary>
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public string GetOutputCol() => ParamMap.Get("outputCol").Value;
}

/// <summary>
/// Model fitted by MaxAbsScaler.
/// </summary>
public class MaxAbsScalerModel : Model
{
    private const string ClassName = "org.apache.spark.ml.feature.MaxAbsScalerModel";

    public MaxAbsScalerModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the maximum absolute values for each feature.
    /// </summary>
    public List<double> MaxAbs => Fetch("maxAbs");

    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);

    public static MaxAbsScalerModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, MaxAbsScaler.DefaultParams.Clone());
        return new MaxAbsScalerModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
