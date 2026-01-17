using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// A transformer that maps a column of indices back to a new column of corresponding string values.
/// The index-string mapping is either from the ML attributes of the input column, or from user-supplied labels.
/// </summary>
public class IndexToString : Transformer
{
    private const string ClassName = "org.apache.spark.ml.feature.IndexToString";

    private static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", ""),
        new("outputCol", ""),
        new("labels", null)
    ]);

    public IndexToString(SparkSession sparkSession) : this(sparkSession, DefaultParams.Clone())
    {
    }

    public IndexToString(SparkSession sparkSession, ParamMap parameters) : base(sparkSession, ClassName, parameters)
    {
    }

    public IndexToString(SparkSession sparkSession, IDictionary<string, dynamic> parameters) : this(sparkSession, DefaultParams.Clone().Update(parameters))
    {
    }

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
    /// Sets the array of labels to be used for the index-to-string transformation.
    /// </summary>
    public void SetLabels(string[] labels) => ParamMap.Add("labels", labels);
    public string[]? GetLabels() => ParamMap.Get("labels").Value;

    public static IndexToString Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName, MlOperator.Types.OperatorType.Transformer);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        return new IndexToString(spark, paramMap);
    }
}
