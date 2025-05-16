using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.ML.Feature;

/// <summary>
/// Creates an `IDF` which is used to "Compute the Inverse Document Frequency (IDF) given a collection of documents"
///
/// You can either pass the parameters via the constructor or set them yourself but they do need to be set.
/// </summary>
public class IDF(ParamMap paramMap) : Estimator<IDFModel>(IdentifiableHelper.RandomUID("idf-static"), "org.apache.spark.ml.feature.IDF", paramMap)
{
    
    public static readonly ParamMap DefaultParams = new(
    [
            new("minDocFreq", 1), 
            new("inputCol", ""), 
            new("outputCol", "")
    ]);

    /// <summary>
    /// Represents an IDF (Inverse Document Frequency) estimator used in processing text data to calculate the inverse frequency of terms across a collection of documents.
    /// This assists in transforming data features and is particularly useful in machine learning and natural language processing workflows.
    /// </summary>
    public IDF() : this(DefaultParams.Clone())
    {
        
    }

    /// <summary>
    /// Represents an IDF (Inverse Document Frequency) estimator used to compute the IDF of a collection of documents.
    /// </summary>
    /// <param name="parameters">An optional map of parameters to initialize the IDF.</param>
    public IDF(IDictionary<string, dynamic> parameters) : this(DefaultParams.Clone().Update(parameters))
    {
        
    }
 
    /// <summary>
    /// Set the inputCol
    /// </summary>
    /// <param name="inputCol">Name of the input column</param>
    public void SetInputCol(string inputCol) => ParamMap.Add("inputCol", inputCol);
    
    /// <summary>
    /// Get the inputCol
    /// </summary>
    /// <returns>string</returns>
    public string GetInputCol() => ParamMap.Get("inputCol").Value;
    
    /// <summary>
    /// Set the outputCol
    /// </summary>
    /// <param name="outputCol">Name of the output column</param>
    public void SetOutputCol(string outputCol) => ParamMap.Add("outputCol", outputCol);
    
    /// <summary>
    /// Get the outputCol
    /// </summary>
    /// <returns></returns>
    public string GetOutputCol() => ParamMap.Get("outputCol").Value;
    
    /// <summary>
    /// Sets the minDocFreq
    /// </summary>
    /// <param name="minDocFreq">Min value</param>
    public void SetMinDocFreq(int minDocFreq) => ParamMap.Add("minDocFreq", minDocFreq);
    
    /// <summary>
    /// Gets the minDocFreq
    /// </summary>
    /// <returns></returns>
    public int GetMinDocFreq() => ParamMap.Get("minDocFreq").Value;
}