using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// A feature transformer that merges multiple columns into a vector column.
/// </summary>
/// <param name="sparkSession"></param>
/// <param name="parameters"></param>
public class VectorAssembler(SparkSession sparkSession, ParamMap parameters) : Transformer(sparkSession, ClassName, parameters)
{
    private const string ClassName = "org.apache.spark.ml.feature.VectorAssembler";

    private static readonly ParamMap DefaultParams = new(
    [
        new("inputCols", Array.Empty<string>()), 
        new("outputCol", String.Empty),
        new("handleInvalid", "error")
    ]);
    
    /// <summary>
    /// A feature transformer that merges multiple columns into a vector column.
    /// </summary>
    /// <param name="sparkSession">`SparkSession` to create the `Tokenizer` on</param>
    public VectorAssembler(SparkSession sparkSession) : this(sparkSession,  DefaultParams)
    {
        
    }

    /// <summary>
    /// Sets the inputCols parameter, required
    /// </summary>
    /// <param name="val">List of the columns to be assembled into a Vector</param>
    public void SetInputCols(IList<string> val) => this.ParamMap.Add("inputCols", val);
    
    /// <summary>
    /// Sets the outputCol parameter, required
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public void SetOutputCol(string val) => this.ParamMap.Add("outputCol", val);

    /// <summary>
    /// Sets the handleInvalid parameter
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public void SetHandleInvalid(string val) => this.ParamMap.Add("handleInvalid", val);
    
    /// <summary>
    /// Gets the inputCols parameter
    /// </summary>
    /// <param name="val">List of the columns to be assembled into a Vector</param>
    public string[] GetInputCols() => this.ParamMap.Get("inputCols").Value as string[];
    
    /// <summary>
    /// Gets the outputCol parameter
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public void GetOutputCol() => this.ParamMap.Get("outputCol").Value.ToString();

    /// <summary>
    ///  Gets the value of handleInvalid or its default value.
    /// </summary>
    /// <param name="val">Name of the column to place the output data</param>
    public string GetHandleInvalid() => this.ParamMap.Get("handleInvalid").Value.ToString();
    
}