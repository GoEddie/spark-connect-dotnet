using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// A feature transformer that converts the input array of strings into an array of n-grams.
/// Null values in the input array are ignored.
/// </summary>
public class NGram : Transformer
{
    private const string ClassName = "org.apache.spark.ml.feature.NGram";

    private static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", ""),
        new("n", 2),
        new("outputCol", "")
    ]);

    public NGram(SparkSession sparkSession) : this(sparkSession, DefaultParams.Clone())
    {
    }

    public NGram(SparkSession sparkSession, ParamMap parameters) : base(sparkSession, ClassName, parameters)
    {
    }

    public NGram(SparkSession sparkSession, IDictionary<string, dynamic> parameters) : this(sparkSession, DefaultParams.Clone().Update(parameters))
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
    /// Sets the minimum n-gram length. Must be >= 1.
    /// </summary>
    public void SetN(int n) => ParamMap.Add("n", n);
    public int GetN() => ParamMap.Get("n").Value;

    public static NGram Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName, MlOperator.Types.OperatorType.Transformer);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        return new NGram(spark, paramMap);
    }
}
