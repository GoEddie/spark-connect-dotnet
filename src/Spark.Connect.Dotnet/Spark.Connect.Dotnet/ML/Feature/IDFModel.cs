using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Feature;

public class IDFModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap)
    : Model(uid, ClassName, objRef, sparkSession, paramMap)
{
    private const string ClassName = "org.apache.spark.ml.feature.IDFModel";

    /// <summary>
    /// Load the IDF Model from Disk
    /// </summary>
    /// <param name="path">The path the Spark Server should read from (it is not a local path)</param>
    /// <param name="sparkSession">SparkSession so we can ask the Spark Server</param>
    /// <returns>`IDFModel`</returns>
    public static IDFModel Load(string path, SparkSession sparkSession)
    {
        var mlResult = Transformer.Load(path, sparkSession, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, IDF.DefaultParams.Clone());
        
        var objectRef = GetObjectRef(mlResult.OperatorInfo.ObjRef);
        var loadedModel = new IDFModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, sparkSession, paramMap);
        
        return loadedModel;
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