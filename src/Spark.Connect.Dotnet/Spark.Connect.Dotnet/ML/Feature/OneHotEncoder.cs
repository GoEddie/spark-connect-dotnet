using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// A one-hot encoder that maps a column of category indices to a column of binary vectors,
/// with at most a single one-value per row that indicates the input category index.
/// </summary>
public class OneHotEncoder : Estimator<OneHotEncoderModel>
{
    public OneHotEncoder() : this(DefaultParams.Clone())
    {
    }

    public OneHotEncoder(ParamMap paramMap) : base(IdentifiableHelper.RandomUID("onehotencoder"), "org.apache.spark.ml.feature.OneHotEncoder", DefaultParams.Clone())
    {
    }

    public OneHotEncoder(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
    }

    public static readonly ParamMap DefaultParams = new(
    [
        new("dropLast", true),
        new("handleInvalid", "error"),
        new("inputCol", null),
        new("inputCols", null),
        new("outputCol", null),
        new("outputCols", null)
    ]);

    /// <summary>
    /// Sets whether to drop the last category in the encoded vector.
    /// </summary>
    public void SetDropLast(bool dropLast) => ParamMap.Add("dropLast", dropLast);
    public bool GetDropLast() => ParamMap.Get("dropLast").Value;

    /// <summary>
    /// Sets how to handle invalid data (unseen labels). Options: error, keep.
    /// </summary>
    public void SetHandleInvalid(string handleInvalid) => ParamMap.Add("handleInvalid", handleInvalid);
    public string GetHandleInvalid() => ParamMap.Get("handleInvalid").Value;

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
    /// Sets the output column name.
    /// </summary>
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public string? GetOutputCol() => ParamMap.Get("outputCol").Value;

    /// <summary>
    /// Sets the output column names (for multiple columns).
    /// </summary>
    public void SetOutputCols(string[] outputCols) => ParamMap.Add("outputCols", outputCols);
    public string[]? GetOutputCols() => ParamMap.Get("outputCols").Value;
}

/// <summary>
/// Model fitted by OneHotEncoder.
/// </summary>
public class OneHotEncoderModel : Model
{
    private const string ClassName = "org.apache.spark.ml.feature.OneHotEncoderModel";

    public OneHotEncoderModel(string uid, ObjectRef objRef, SparkSession spark, ParamMap paramMap) :
        base(uid, ClassName, objRef, spark, paramMap)
    {
    }

    /// <summary>
    /// Gets the category sizes for each input column.
    /// </summary>
    public List<int> CategorySizes => Fetch("categorySizes");

    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public void SetInputCols(string[] inputCols) => ParamMap.Add("inputCols", inputCols);
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public void SetOutputCols(string[] outputCols) => ParamMap.Add("outputCols", outputCols);
    public void SetDropLast(bool dropLast) => ParamMap.Add("dropLast", dropLast);
    public void SetHandleInvalid(string handleInvalid) => ParamMap.Add("handleInvalid", handleInvalid);

    public static OneHotEncoderModel Load(string path, SparkSession spark)
    {
        var mlResult = Transformer.Load(path, spark, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, OneHotEncoder.DefaultParams.Clone());
        return new OneHotEncoderModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, spark, paramMap);
    }
}
