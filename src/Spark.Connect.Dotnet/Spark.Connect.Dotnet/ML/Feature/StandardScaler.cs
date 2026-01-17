using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Standardizes features by removing the mean and scaling to unit variance using column summary statistics.
/// </summary>
public class StandardScaler : Estimator<StandardScalerModel>
{
    public StandardScaler() : this(DefaultParams.Clone())
    {
    }

    public StandardScaler(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("stdscaler"), "org.apache.spark.ml.feature.StandardScaler", DefaultParams.Clone())
    {
    }

    public StandardScaler(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", ""),
        new("outputCol", ""),
        new("withMean", false),
        new("withStd", true)
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

    /// <summary>
    /// Sets whether to center the data with mean before scaling.
    /// </summary>
    public void SetWithMean(bool withMean) => ParamMap.Add("withMean", withMean);
    public bool GetWithMean() => ParamMap.Get("withMean").Value;

    /// <summary>
    /// Sets whether to scale the data to unit standard deviation.
    /// </summary>
    public void SetWithStd(bool withStd) => ParamMap.Add("withStd", withStd);
    public bool GetWithStd() => ParamMap.Get("withStd").Value;
}

/// <summary>
/// Model fitted by StandardScaler.
/// </summary>
public class StandardScalerModel : Model
{
    private const string ClassName = "org.apache.spark.ml.feature.StandardScalerModel";

    public StandardScalerModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the mean values of the fitted scaler.
    /// </summary>
    public List<double> Mean => Fetch("mean");

    /// <summary>
    /// Gets the standard deviation values of the fitted scaler.
    /// </summary>
    public List<double> Std => Fetch("std");

    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);

    public static StandardScalerModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, StandardScaler.DefaultParams.Clone());
        return new StandardScalerModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
