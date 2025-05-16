using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Binarize a column of continuous features given a threshold. Since 3.0.0, Binarize can map multiple columns at once by setting the inputCols parameter.
/// Note that when both the inputCol and inputCols parameters are set, an Exception will be thrown.
/// The threshold parameter is used for single column usage, and thresholds is for multiple columns.
/// </summary>
/// <param name="sparkSession">`SparkSession` to create the `Tokenizer` on</param>
/// <param name="parameters">Optional parameters, can be skipped and set manually</param>
public class Binarizer(SparkSession sparkSession, ParamMap parameters) : Transformer(sparkSession, ClassName, parameters)
{
    private const string ClassName = "org.apache.spark.ml.feature.Binarizer";

    private static readonly ParamMap DefaultParams = new(
    [
        new("threshold", 0.0f),
        new("thresholds", null as float[]),
        new("inputCol", ""), 
        new("outputCol", ""), 
        new("inputCols", new List<string>()), 
        new("outputCols", new List<string>()),
    ]);

    /// <summary>
    /// Binarize a column of continuous features given a threshold. Since 3.0.0, Binarize can map multiple columns at once by setting the inputCols parameter.
    /// Note that when both the inputCol and inputCols parameters are set, an Exception will be thrown.
    /// The threshold parameter is used for single column usage, and thresholds is for multiple columns.
    /// </summary>
    /// <param name="sparkSession">`SparkSession` to create the `Tokenizer` on</param>
    public Binarizer(SparkSession sparkSession) : this(sparkSession,  DefaultParams.Clone())
    {
        
    }

    /// <summary>
    /// Binarize a column of continuous features given a threshold. Since 3.0.0, Binarize can map multiple columns at once by setting the inputCols parameter.
    /// Note that when both the inputCol and inputCols parameters are set, an Exception will be thrown.
    /// The threshold parameter is used for single column usage, and thresholds is for multiple columns.
    /// </summary>
    /// <param name="sparkSession">`SparkSession` to create the `Tokenizer` on</param>
    /// <param name="parameters">Set any parameters here</param>
    public Binarizer(SparkSession sparkSession, IDictionary<string, dynamic> parameters) : this(sparkSession,  DefaultParams.Clone().Update(parameters))
    {
        
    }

    /// <summary>
    /// Sets the inputCol parameter
    /// </summary>
    /// <param name="val">Name of the column with the input data</param>
    public void SetInputCol(string val) => ParamMap.Add("inputCol", val);
    
    /// <summary>
    /// Sets the outputCol parameter
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public void SetOutputCol(string val) => ParamMap.Add("outputCol", val);
    
    /// <summary>
    /// Sets the inputCols parameter
    /// </summary>
    /// <param name="val">Name of the column with the input data</param>
    public void SetInputCols(List<string> val) => ParamMap.Add("inputCols", val);
    
    /// <summary>
    /// Sets the outputCols parameter
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public void SetOutputCols(List<string> val) => ParamMap.Add("outputCols", val);

    /// <summary>
    /// Sets the threshold for binarization of a column. Any value in the input column less than or equal to
    /// the threshold will be mapped to 0.0, and any value greater than the threshold will be mapped to 1.0.
    /// This applies for single column usage. For multiple column usage, use the thresholds parameter.
    /// </summary>
    /// <param name="val">Threshold value to apply for binarization</param>
    public void SetThreshold(float val) => ParamMap.Add("threshold", val);
    
    /// <summary>
    /// Retrieves the threshold value used for binarizing a column of continuous features.
    /// </summary>
    /// <returns>The threshold value as a float.</returns>
    public float GetThreshold() => ((float)ParamMap.Get("threshold").Value!);
    
    /// <summary>
    /// Sets the threshold for binarization of a column. Any value in the input column less than or equal to
    /// the threshold will be mapped to 0.0, and any value greater than the threshold will be mapped to 1.0.
    /// This applies for single column usage. For multiple column usage, use the thresholds parameter.
    /// </summary>
    /// <param name="val">Threshold value to apply for binarization</param>
    public void SetThresholds(List<float> val) => ParamMap.Add("threshold", val);


    /// <summary>
    /// Retrieves the threshold value used for binarizing a column of continuous features.
    /// </summary>
    /// <returns>The threshold value as a float.</returns>
    public List<float> GetThresholds() => ((List<float>)ParamMap.Get("threshold").Value!);

    
    /// <summary>
    /// Gets the inputCol parameter, required
    /// </summary>
    public string GetInputCol() => ParamMap.Get("inputCol").Value.ToString();

    /// <summary>
    /// Gets the outputCol parameter, required
    /// </summary>
    public string GetOutputCol() => ParamMap.Get("outputCol").Value.ToString();

    /// <summary>
    /// Gets the inputCol parameter, required
    /// </summary>
    public List<string> GetInputCols() => ParamMap.Get("inputCols").Value.ToString();

    /// <summary>
    /// Gets the outputCol parameter, required
    /// </summary>
    public List<string> GetOutputCols() => ParamMap.Get("outputCols").Value.ToString();
    
    /// <summary>
    /// Load the `Bucketizer` from the Spark Connect server
    /// </summary>
    /// <param name="path">Path to load the `Tokenizer` from on the Spark Connect server</param>
    /// <param name="spark">the `SparkSession` to load the data from</param>
    /// <returns></returns>
    public static Binarizer Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName);
        
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        var tokenizer = new Binarizer(spark, paramMap);
        return tokenizer;
    }
}