using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// A feature transformer that filters out stop words from input.
/// </summary>
public class StopWordsRemover : Transformer
{
    private const string ClassName = "org.apache.spark.ml.feature.StopWordsRemover";

    private static readonly ParamMap DefaultParams = new(
    [
        new("caseSensitive", false),
        new("inputCol", null),
        new("inputCols", null),
        new("locale", "en_US"),
        new("outputCol", null),
        new("outputCols", null),
        new("stopWords", null)
    ]);

    public StopWordsRemover(SparkSession sparkSession) : this(sparkSession, DefaultParams.Clone())
    {
    }

    public StopWordsRemover(SparkSession sparkSession, ParamMap parameters) : base(sparkSession, ClassName, parameters)
    {
    }

    public StopWordsRemover(SparkSession sparkSession, IDictionary<string, dynamic> parameters) : this(sparkSession, DefaultParams.Clone().Update(parameters))
    {
    }

    /// <summary>
    /// Sets whether to do a case-sensitive comparison over the stop words.
    /// </summary>
    public void SetCaseSensitive(bool caseSensitive) => ParamMap.Add("caseSensitive", caseSensitive);
    public bool GetCaseSensitive() => ParamMap.Get("caseSensitive").Value;

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
    /// Sets the locale of the input for case insensitive matching.
    /// </summary>
    public void SetLocale(string locale) => ParamMap.Add("locale", locale);
    public string GetLocale() => ParamMap.Get("locale").Value;

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
    /// Sets the stop words. If not provided, default stop words for the locale will be used.
    /// </summary>
    public void SetStopWords(string[] stopWords) => ParamMap.Add("stopWords", stopWords);
    public string[]? GetStopWords() => ParamMap.Get("stopWords").Value;

    public static StopWordsRemover Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName, MlOperator.Types.OperatorType.Transformer);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        return new StopWordsRemover(spark, paramMap);
    }
}
