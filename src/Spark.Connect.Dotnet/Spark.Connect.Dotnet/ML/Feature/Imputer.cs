using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Imputation estimator for completing missing values, using the mean, median or mode
/// of the columns in which the missing values are located.
/// </summary>
public class Imputer : Estimator<ImputerModel>
{
    public Imputer() : this(DefaultParams.Clone())
    {
    }

    public Imputer(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("imputer"), "org.apache.spark.ml.feature.Imputer", DefaultParams.Clone())
    {
    }

    public Imputer(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", null),
        new("inputCols", null),
        new("missingValue", double.NaN),
        new("outputCol", null),
        new("outputCols", null),
        new("relativeError", 0.001),
        new("strategy", "mean")
    ]);

    /// <summary>
    /// Sets the input column name.
    /// </summary>
    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public string? GetInputCol() => ParamMap.Get("inputCol").Value;

    /// <summary>
    /// Sets the input column names (for multiple columns).
    /// </summary>
    public void SetInputCols(string[] inputCols) => ParamMap.Add("inputCols", inputCols);
    public string[]? GetInputCols() => ParamMap.Get("inputCols").Value;

    /// <summary>
    /// Sets the placeholder for the missing values.
    /// </summary>
    public void SetMissingValue(double missingValue) => ParamMap.Add("missingValue", missingValue);
    public double GetMissingValue() => ParamMap.Get("missingValue").Value;

    /// <summary>
    /// Sets the output column name.
    /// </summary>
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public string? GetOutputCol() => ParamMap.Get("outputCol").Value;

    /// <summary>
    /// Sets the output column names (for multiple columns).
    /// </summary>
    public void SetOutputCols(string[] outputCols) => ParamMap.Add("outputCols", outputCols);
    public string[]? GetOutputCols() => ParamMap.Get("outputCols").Value;

    /// <summary>
    /// Sets the relative error for approximate quantile calculation when using median strategy.
    /// </summary>
    public void SetRelativeError(double relativeError) => ParamMap.Add("relativeError", relativeError);
    public double GetRelativeError() => ParamMap.Get("relativeError").Value;

    /// <summary>
    /// Sets the imputation strategy. Supported: "mean", "median", "mode".
    /// </summary>
    public void SetStrategy(string strategy) => ParamMap.Add("strategy", strategy);
    public string GetStrategy() => ParamMap.Get("strategy").Value;
}

/// <summary>
/// Model fitted by Imputer.
/// </summary>
public class ImputerModel : Model
{
    private const string ClassName = "org.apache.spark.ml.feature.ImputerModel";

    public ImputerModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public void SetInputCols(string[] inputCols) => ParamMap.Add("inputCols", inputCols);
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public void SetOutputCols(string[] outputCols) => ParamMap.Add("outputCols", outputCols);

    public static ImputerModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, Imputer.DefaultParams.Clone());
        return new ImputerModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
