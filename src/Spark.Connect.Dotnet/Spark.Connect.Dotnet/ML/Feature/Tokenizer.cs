using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// A tokenizer that converts the input string to lowercase and then splits it by white spaces
/// </summary>
/// <param name="sparkSession">`SparkSession` to create the `Tokenizer` on</param>
/// <param name="parameters">Optional parameters, can be skipped and set manually</param>
public class Tokenizer(SparkSession sparkSession, ParamMap parameters) : Transformer(sparkSession, ClassName, parameters)
{
    private const string ClassName = "org.apache.spark.ml.feature.Tokenizer";

    private static readonly ParamMap DefaultParams = new(
    [
        new("minDocFreq", 1), 
        new("inputCol", ""), 
        new("outputCol", "")
    ]);

    /// <summary>
    /// A tokenizer that converts the input string to lowercase and then splits it by white spaces, uses default parameters
    /// </summary>
    /// <param name="sparkSession">`SparkSession` to create the `Tokenizer` on</param>
    public Tokenizer(SparkSession sparkSession) : this(sparkSession,  DefaultParams)
    {
        
    }

    /// <summary>
    /// Sets the inputCol parameter, required
    /// </summary>
    /// <param name="val">Name of the column with the input data</param>
    public void SetInputCol(string val) => this.ParamMap.Add("inputCol", val);
    
    /// <summary>
    /// Sets the outputCol parameter, required
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public void SetOutputCol(string val) => this.ParamMap.Add("outputCol", val);
    
    /// <summary>
    /// Sets the minDocFreq parameter, defaults to 1
    /// </summary>
    /// <param name="val">The minDocFreq</param>
    public void SetMinDocFreq(int val) => this.ParamMap.Add("minDocFreq", val);
    
    /// <summary>
    /// Sets the inputCol parameter, required
    /// </summary>
    /// <param name="val">Name of the column with the input data</param>
    public string GetInputCol() => this.ParamMap.Get("inputCol").Value.ToString();

    /// <summary>
    /// Sets the outputCol parameter, required
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public string GetOutputCol() => this.ParamMap.Get("outputCol").Value.ToString();

    /// <summary>
    /// Sets the minDocFreq parameter, defaults to 1
    /// </summary>
    /// <param name="val">The minDocFreq</param>
    public int GetMinDocFreq() => ((int)this.ParamMap.Get("minDocFreq").Value);

    /// <summary>
    /// Load the `Tokenizer` from the Spark Connect server
    /// </summary>
    /// <param name="path">Path to load the `Tokenizer` from on the Spark Connect server</param>
    /// <param name="spark">the `SparkSession` to load the data from</param>
    /// <returns></returns>
    public static Tokenizer Load(string path, SparkSession spark)
    {
        var mlResult = Load(path, spark, ClassName);
        
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, IDF.DefaultParams.Clone());
        var tokenizer = new Tokenizer(spark, paramMap);
        return tokenizer;
    }
}