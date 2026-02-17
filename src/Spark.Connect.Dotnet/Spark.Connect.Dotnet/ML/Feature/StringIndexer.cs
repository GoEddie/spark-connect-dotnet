using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

///A label indexer that maps a string column of labels to an ML column of label indices. If the input column is numeric, we cast it to string and index the
/// string values. The indices are in [0, numLabels). By default, this is ordered by label frequencies so the most frequent label gets index 0.
/// The ordering behavior is controlled by setting stringOrderType. Its default value is ‘frequencyDesc’.
public class StringIndexerModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap)
    : Model(uid, ClassName, objRef, sparkSession, paramMap)
{
    private const string ClassName = "org.apache.spark.ml.feature.StringIndexerModel";

    /// <summary>
    /// Load a `StringIndexerModel` that was previously saved to disk on the Spark Connect server
    /// </summary>
    /// <param name="path">Where to read the `GBTClassifierModel` from</param>
    /// <param name="sparkSession">A `SparkSession` to read the model through</param>
    /// <returns>`StringIndexerModel`</returns>
    public static StringIndexerModel Load(string path, SparkSession sparkSession)
    {
        var mlResult = Transformer.Load(path, sparkSession, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, StringIndexer.DefaultParams.Clone());

        var loadedModel = new StringIndexerModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, sparkSession, paramMap);

        return loadedModel;
    }
}

/// <summary>
///A label indexer that maps a string column of labels to an ML column of label indices. If the input column is numeric, we cast it to string and index the
/// string values. The indices are in [0, numLabels). By default, this is ordered by label frequencies so the most frequent label gets index 0.
/// The ordering behavior is controlled by setting stringOrderType. Its default value is ‘frequencyDesc’.
/// </summary>
public class StringIndexer(ParamMap paramMap)
    : Estimator<StringIndexerModel>(IdentifiableHelper.RandomUID("stringindexer-static"), "org.apache.spark.ml.feature.StringIndexer", paramMap)
{
    public static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", null),
        new("outputCol", null),
        new("inputCols", null),
        new("outputCols", null),
        new("handleInvalid", "error"),
        new("stringOrderType", "frequencyDesc")
    ]);

    /// <summary>
    /// Represents an estimator that maps a column of string labels to a column of indices,
    /// either encoding a single or multiple input columns of strings into corresponding
    /// numerical labels.
    /// </summary>
    /// <remarks>
    /// This class is part of the Spark ML feature transformation API, specifically
    /// designed to work with string type features. It supports options to handle unknown
    /// labels and determine the order of labels (e.g., frequency-based or alphabetical).
    /// </remarks>
    /// <param name="paramMap">Parameter map containing supported key-value pairs like input column,
    /// output column, and handling of invalid entries.</param>
    /// <returns>A new instance of <c>StringIndexer</c> configured with the provided parameters.
    /// Supports multiple configuration settings like handling invalid strings or defining the order
    /// of strings during transformation.</returns>
    public StringIndexer() : this(DefaultParams.Clone())
    {
        
    }

    /// <summary>
    /// A label indexer that maps a string column of labels to an ML column of label indices.
    /// If the input column is numeric, it casts it to string and indexes the string values.
    /// The indices are in [0, numLabels). By default, this is ordered by label frequencies so the
    /// most frequent label gets index 0. The ordering behavior is controlled by the stringOrderType parameter.
    /// </summary>
    public StringIndexer(IDictionary<string, dynamic> paramMap) : this(DefaultParams.Clone().Update(paramMap))
    {
        
    }
    
    /// <summary>
    /// Sets the input column for the `StringIndexer`.
    /// </summary>
    /// <param name="val">The name of the input column that contains the labels to be indexed.</param>
    public void SetInputCol(string val) => ParamMap.Add("inputCol", val);

    /// <summary>
    /// Sets the name of the output column for the `StringIndexer`.
    /// </summary>
    /// <param name="val">The name of the output column to set.</param>
    public void SetOutputCol(string val) => ParamMap.Add("outputCol", val);

    /// <summary>
    /// Sets the input columns for the `StringIndexer` to transform multiple input columns into indexed labels.
    /// </summary>
    /// <param name="val">An array of input column names to be indexed</param>
    public void SetInputCols(string[] val) => ParamMap.Add("inputCols", val);

    /// <summary>
    /// Sets the output columns for the `StringIndexer`.
    /// </summary>
    /// <param name="val">An array of strings specifying the names of the output columns</param>
    public void SetOutputCols(string[] val) => ParamMap.Add("outputCols", val);

    /// <summary>
    /// Sets the strategy used to handle invalid data in the input column(s).
    /// </summary>
    /// <param name="val">The strategy for handling invalid data. Supported options are:
    /// "error" (throws an exception), "skip" (filters out rows with invalid data), or "keep"
    /// (assigns an additional index for invalid data).</param>
    public void SetHandleInvalid(string val) => ParamMap.Add("handleInvalid", val);

    /// <summary>
    /// Retrieves the name of the input column for the `StringIndexer`.
    /// </summary>
    /// <returns>The name of the input column as a string.</returns>
    public string GetInputCol() => ParamMap.Get("inputCol").Value;

    /// <summary>
    /// Retrieves the name of the output column set for the StringIndexer.
    /// </summary>
    /// <returns>A string representing the name of the output column.</returns>
    public string GetOutputCol() => ParamMap.Get("outputCol").Value;

    /// <summary>
    /// Retrieves the input columns that have been set for the `StringIndexer`.
    /// </summary>
    /// <returns>An array of strings representing the input column names.</returns>
    public string[] GetInputCols() => ParamMap.Get("inputCols").Value;

    /// <summary>
    /// Retrieves the output columns configured for the `StringIndexer`.
    /// </summary>
    /// <returns>An array of strings representing the output column names.</returns>
    public string[] GetOutputCols() => ParamMap.Get("outputCols").Value;

    /// <summary>
    /// Retrieves the value of the `handleInvalid` parameter, which specifies how to handle invalid data during transformation.
    /// </summary>
    /// <returns>A string representing the `handleInvalid` parameter. Possible values include "error", "skip", or "keep".</returns>
    public string GetHandleInvalid() => ParamMap.Get("handleInvalid").Value;
}