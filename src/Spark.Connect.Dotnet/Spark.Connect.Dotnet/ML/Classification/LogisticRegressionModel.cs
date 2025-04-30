using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Classification;

public class LogisticRegressionModel : Model
{
    private const string ClassName = "org.apache.spark.ml.classification.LogisticRegressionModel";
    
    public LogisticRegressionModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap) : base(uid, ClassName, objRef, sparkSession, paramMap)
    {
        
    }
    
    public static LogisticRegressionModel Load(string path, SparkSession sparkSession)
    {
        var mlResult = Transformer.Load(path, sparkSession, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, LogisticRegression.DefaultParams.Clone());
        
        var objectRef = GetObjectRef(mlResult.OperatorInfo.ObjRef);
        
        var loadedModel = new LogisticRegressionModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, sparkSession, paramMap);
        
        return loadedModel;
        
    }

    
}