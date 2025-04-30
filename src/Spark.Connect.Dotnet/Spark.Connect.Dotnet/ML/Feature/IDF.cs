using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Creates an `IDF` which is used to "Compute the Inverse Document Frequency (IDF) given a collection of documents"
///
/// You can either pass the parameters via the constructor or set them yourself but they do need to be set.
/// </summary>
/// <param name="minDocFreq">Sets the minDocFreq, the default is 0</param>
/// <param name="inputCol">Sets the inputCol, there is no default</param>
/// <param name="outputCol">Sets the outputCol, there is no default</param>
public class IDF(int minDocFreq = 0, string? inputCol = null, string? outputCol = null)
    : Estimator<IDFModel>(IdentifiableHelper.RandomUID("idf-static"),
                "org.apache.spark.ml.feature.IDF",
                DefaultParams.Update(new()
                    {
                        { "minDocFreq", minDocFreq }, { "inputCol", inputCol }, { "outputCol", outputCol }
                    }
                ))
{
    private int _minDocFreq = minDocFreq;
    private string? _inputCol = inputCol;
    private string? _outputCol = outputCol;

    public static readonly ParamMap DefaultParams = new(
    [
            new("minDocFreq", 1), 
            new("inputCol", ""), 
            new("outputCol", "")
    ]);

    /// <summary>
    /// Set the inputCol
    /// </summary>
    /// <param name="inputCol">Name of the input column</param>
    public void SetInputCol(string inputCol) => _inputCol = inputCol;
    
    /// <summary>
    /// Get the inputCol
    /// </summary>
    /// <returns>string</returns>
    public string GetInputCol() => _inputCol;
    
    /// <summary>
    /// Set the outputCol
    /// </summary>
    /// <param name="outputCol">Name of the output column</param>
    public void SetOutputCol(string outputCol) => _outputCol = outputCol;
    
    /// <summary>
    /// Get the outputCol
    /// </summary>
    /// <returns></returns>
    public string GetOutputCol() => _outputCol;
    
    /// <summary>
    /// Sets the minDocFreq
    /// </summary>
    /// <param name="minDocFreq">Min value</param>
    public void SetMinDocFreq(int minDocFreq) => _minDocFreq = minDocFreq;
    
    /// <summary>
    /// Gets the minDocFreq
    /// </summary>
    /// <returns></returns>
    public int GetMinDocFreq() => _minDocFreq;

}