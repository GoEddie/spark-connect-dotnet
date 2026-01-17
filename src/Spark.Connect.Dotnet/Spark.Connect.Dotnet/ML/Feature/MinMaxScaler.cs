using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Rescales each feature individually to a common range [min, max] linearly using column summary statistics.
/// </summary>
public class MinMaxScaler : Estimator<MinMaxScalerModel>
{
    public MinMaxScaler() : this(DefaultParams.Clone())
    {
    }

    public MinMaxScaler(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("minmaxscaler"), "org.apache.spark.ml.feature.MinMaxScaler", DefaultParams.Clone())
    {
    }

    public MinMaxScaler(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", ""),
        new("outputCol", ""),
        new("min", 0.0),
        new("max", 1.0)
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
    /// Sets the lower bound of the output feature range.
    /// </summary>
    public void SetMin(double min) => ParamMap.Add("min", min);
    public double GetMin() => ParamMap.Get("min").Value;

    /// <summary>
    /// Sets the upper bound of the output feature range.
    /// </summary>
    public void SetMax(double max) => ParamMap.Add("max", max);
    public double GetMax() => ParamMap.Get("max").Value;
}

/// <summary>
/// Model fitted by MinMaxScaler.
/// </summary>
public class MinMaxScalerModel : Model
{
    private const string ClassName = "org.apache.spark.ml.feature.MinMaxScalerModel";

    public MinMaxScalerModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the original minimum values from the training data.
    /// </summary>
    public List<double> OriginalMin => Fetch("originalMin");

    /// <summary>
    /// Gets the original maximum values from the training data.
    /// </summary>
    public List<double> OriginalMax => Fetch("originalMax");

    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);

    public static MinMaxScalerModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, MinMaxScaler.DefaultParams.Clone());
        return new MinMaxScalerModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
