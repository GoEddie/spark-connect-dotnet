using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// A regex based tokenizer that extracts tokens either by using the provided regex pattern
/// (in Java dialect) to split the text (default) or repeatedly matching the regex (if gaps is false).
/// </summary>
public class RegexTokenizer : Transformer
{
    private const string ClassName = "org.apache.spark.ml.feature.RegexTokenizer";

    private static readonly ParamMap DefaultParams = new(
    [
        new("gaps", true),
        new("inputCol", ""),
        new("minTokenLength", 1),
        new("outputCol", ""),
        new("pattern", "\\s+"),
        new("toLowercase", true)
    ]);

    public RegexTokenizer(SparkSession sparkSession) : this(sparkSession, DefaultParams.Clone())
    {
    }

    public RegexTokenizer(SparkSession sparkSession, ParamMap parameters) : base(sparkSession, ClassName, parameters)
    {
    }

    public RegexTokenizer(SparkSession sparkSession, IDictionary<string, dynamic> parameters) : this(sparkSession, DefaultParams.Clone().Update(parameters))
    {
    }

    /// <summary>
    /// Sets whether regex splits on gaps (true) or matches tokens (false).
    /// </summary>
    public void SetGaps(bool gaps) => ParamMap.Add("gaps", gaps);
    public bool GetGaps() => ParamMap.Get("gaps").Value;

    /// <summary>
    /// Sets the input column name.
    /// </summary>
    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    public string GetInputCol() => ParamMap.Get("inputCol").Value;

    /// <summary>
    /// Sets the minimum token length. Tokens shorter than this are filtered out.
    /// </summary>
    public void SetMinTokenLength(int minTokenLength) => ParamMap.Add("minTokenLength", minTokenLength);
    public int GetMinTokenLength() => ParamMap.Get("minTokenLength").Value;

    /// <summary>
    /// Sets the output column name.
    /// </summary>
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    public string GetOutputCol() => ParamMap.Get("outputCol").Value;

    /// <summary>
    /// Sets the regex pattern used for tokenizing.
    /// </summary>
    public void SetPattern(string pattern) => ParamMap.Add("pattern", pattern);
    public string GetPattern() => ParamMap.Get("pattern").Value;

    /// <summary>
    /// Sets whether to convert all characters to lowercase before tokenizing.
    /// </summary>
    public void SetToLowercase(bool toLowercase) => ParamMap.Add("toLowercase", toLowercase);
    public bool GetToLowercase() => ParamMap.Get("toLowercase").Value;

    public static RegexTokenizer Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName, MlOperator.Types.OperatorType.Transformer);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        return new RegexTokenizer(spark, paramMap);
    }
}
