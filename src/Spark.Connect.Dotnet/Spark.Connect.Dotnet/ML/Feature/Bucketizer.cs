using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Maps a column of continuous features to a column of feature buckets. Since 3.0.0, Bucketizer can map multiple columns at once by setting the inputCols parameter.
/// Note that when both the inputCol and inputCols parameters are set, an Exception will be thrown. The splits parameter is only used for single column usage,
/// and splitsArray is for multiple columns.
/// </summary>
/// <param name="sparkSession">`SparkSession` to create the `Tokenizer` on</param>
/// <param name="parameters">Optional parameters, can be skipped and set manually</param>
public class Bucketizer(SparkSession sparkSession, ParamMap parameters) : Transformer(sparkSession, ClassName, parameters)
{
    private const string ClassName = "org.apache.spark.ml.feature.Bucketizer";

    private static readonly ParamMap DefaultParams = new(
    [
        new("splits", new List<float>()), 
        new("inputCol", ""), 
        new("outputCol", ""), 
        new("handleInvalid", "error"), 
        new("splitsArray", new List<List<float>>()),
        new("inputCols", new List<string>()), 
        new("outputCols", new List<string>()),
    ]);

    /// <summary>
    /// Maps a column of continuous features to a column of feature buckets. Since 3.0.0, Bucketizer can map multiple columns at once by setting the inputCols parameter.
    /// Note that when both the inputCol and inputCols parameters are set, an Exception will be thrown. The splits parameter is only used for single column usage,
    /// and splitsArray is for multiple columns.
    /// </summary>
    /// <param name="sparkSession">`SparkSession` to create the `Tokenizer` on</param>
    public Bucketizer(SparkSession sparkSession) : this(sparkSession,  DefaultParams.Clone())
    {
        
    }

    /// <summary>
    /// Maps a column of continuous features to discrete buckets (bins) defined by specified splits.
    /// Bucketizer can process a single column or multiple columns by setting the respective input/output parameters.
    /// When both `inputCol` and `inputCols` parameters are set, an Exception will be thrown.
    /// Similarly, the `splits` parameter is used for single column usage, and `splitsArray` is used for multiple columns.
    /// </summary>
    /// <param name="sparkSession">The SparkSession instance used to create the Bucketizer.</param>
    /// <param name="parameters">An optional map of parameters to initialize the Bucketizer.</param>
    public Bucketizer(SparkSession sparkSession, IDictionary<string, dynamic> parameters) : this(sparkSession, DefaultParams.Clone().Update(parameters))
    {
        
    }
    
    /// <summary>
    /// Sets the inputCol parameter
    /// </summary>
    /// <param name="val">Name of the column with the input data</param>
    public void SetInputCol(string val) => this.ParamMap.Add("inputCol", val);
    
    /// <summary>
    /// Sets the outputCol parameter
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public void SetOutputCol(string val) => this.ParamMap.Add("outputCol", val);
    
    /// <summary>
    /// Sets the inputCols parameter
    /// </summary>
    /// <param name="val">Name of the column with the input data</param>
    public void SetInputCols(List<string> val) => this.ParamMap.Add("inputCols", val);
    
    /// <summary>
    /// Sets the outputCols parameter
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public void SetOutputCols(List<string> val) => this.ParamMap.Add("outputCols", val);
    
    /// <summary>
    /// Set the handleInvalid parameter
    /// </summary>
    /// <param name="val"></param>
    public void SetHandleInvalid(string val) => this.ParamMap.Add("handleInvalid", val);
    
    /// <summary>
    /// Gets the handleInvalid parameter
    /// </summary>
    /// <returns></returns>
    public string GetHandleInvalid() => this.ParamMap.Get("handleInvalid").Value.ToString();
    
    /// <summary>
    /// Set the splits parameter
    /// </summary>
    /// <param name="val"></param>
    public void SetSplits(float[] val) => this.ParamMap.Add("splits", val);

    /// <summary>
    /// Retrieves the splits value for the Bucketizer transformation.
    /// The splits define the binning thresholds for single-column feature bucketing.
    /// </summary>
    /// <returns>
    /// A list of float values representing the thresholds for bucketizing the input column.
    /// </returns>
    public List<float> GetSplits() => this.ParamMap.Get("splits").Value.ToString();

    /// <summary>
    /// Sets the array of split points for mapping continuous features into buckets for multiple columns.
    /// </summary>
    /// <param name="val">A two-dimensional list where each inner list represents the split points for a specific column.</param>
    public void SetSplitsArray(List<List<float>> val) => this.ParamMap.Add("splitsArray", val);

    /// <summary>
    /// Retrieves the array of splits used for multiple columns during bucketization.
    /// This parameter allows the Bucketizer to define split points for multiple columns simultaneously.
    /// </summary>
    /// <returns>A list of lists containing float values, where each inner list represents the split points for a specific column.</returns>
    public List<List<float>> GetSplitsArray() => this.ParamMap.Get("splitsArray").Value.ToString();
    
    /// <summary>
    /// Gets the inputCol parameter, required
    /// </summary>
    public string GetInputCol() => this.ParamMap.Get("inputCol").Value.ToString();

    /// <summary>
    /// Gets the outputCol parameter, required
    /// </summary>
    public string GetOutputCol() => this.ParamMap.Get("outputCol").Value.ToString();

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
    public static Bucketizer Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName);
        
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, DefaultParams.Clone());
        var tokenizer = new Bucketizer(spark, paramMap);
        return tokenizer;
    }
}