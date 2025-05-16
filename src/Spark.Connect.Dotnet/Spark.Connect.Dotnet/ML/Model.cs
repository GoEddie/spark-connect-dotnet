using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML;

/// <summary>
/// Most transformers are models, a model is a type of transformer
/// </summary>
public class Model : Transformer
{
    public Model(string uid, string className, ObjectRef objRef, SparkSession sparkSession, ParamMap defaultParams)
        : base(uid, className, objRef, sparkSession, defaultParams)
    {
        OperatorType = MlOperator.Types.OperatorType.Model;
    }
    
}