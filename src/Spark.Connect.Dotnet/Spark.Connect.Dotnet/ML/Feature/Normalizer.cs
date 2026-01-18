using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Normalizes a vector to have unit norm using the given p-norm.
/// This is a pure transformer (no fitting required).
/// </summary>
public class Normalizer : Transformer
{
    private const string ClassName = "org.apache.spark.ml.feature.Normalizer";

    private static readonly ParamMap DefaultParams = new(
    [
        new("inputCol", ""),
        new("outputCol", ""),
        new("p", 2.0)
    ]);

    public Normalizer(SparkSession sparkSession) : this(sparkSession, DefaultParams.Clone())
    {
    }

    public Normalizer(SparkSession sparkSession, ParamMap parameters) : base(sparkSession, ClassName, parameters)
    {
    }

    public Normalizer(SparkSession sparkSession, IDictionary<string, dynamic> parameters) : this(sparkSession, DefaultParams.Clone().Update(parameters))
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
    /// Sets the p-norm used for normalization. Default is 2.0 (L2 norm).
    /// Common values: 1.0 (L1/Manhattan), 2.0 (L2/Euclidean), Infinity (max norm).
    /// </summary>
    public void SetP(double p) => ParamMap.Add("p", p);
    public double GetP() => ParamMap.Get("p").Value;

    public static Normalizer Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName, MlOperator.Types.OperatorType.Transformer);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        return new Normalizer(spark, paramMap);
    }
}
