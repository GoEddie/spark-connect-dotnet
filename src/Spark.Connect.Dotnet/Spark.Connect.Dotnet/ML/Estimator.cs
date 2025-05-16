using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Feature;
using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML;

/// <summary>
/// Estimators, used to create things like `LogisticRegression`
/// </summary>
/// <param name="uid"></param>
/// <param name="className"></param>
/// <param name="defaultParams"></param>
/// <typeparam name="T"></typeparam>
public abstract class Estimator<T>(string uid, string className, ParamMap defaultParams) : Params(defaultParams), Identifiable 
    where T : Transformer
{
    public string Uid { get; set; } = uid;
    private string ClassName { get; set;} = className;

    public T Fit(DataFrame df, ParamMap? paramMap = null)
    {
        var parameters = new MapField<string, Expression.Types.Literal>();
        if (paramMap is null)
        {
            paramMap = this.ParamMap;    
        }
        
        var plan = new Plan()
        {
            Command = new Command()
            { 
                MlCommand = new MlCommand()
                {
                    Fit = new MlCommand.Types.Fit()
                    {
                        Dataset = df.Relation, 
                        Params = new MlParams()
                        {
                            Params = { paramMap.ToMapField() }
                        },
                        Estimator = new MlOperator()
                        {
                            Uid = Uid, 
                            Name = ClassName, 
                            Type = MlOperator.Types.OperatorType.Estimator
                        }
                    },
                }
            }
        };

        var executor = new RequestExecutor(df.SparkSession, plan);
        executor.Exec();

        var mlResult = executor.GetMlCommandResult();
        
        switch (typeof(T))
        {
           case { } when typeof(T) == typeof(LogisticRegressionModel):
               return (T)(object)new LogisticRegressionModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, df.SparkSession, paramMap);
           case {} when typeof(T) == typeof(IDFModel):
               return (T)(object)new IDFModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, df.SparkSession, paramMap);
           case {} when typeof(T) == typeof(NaiveBayesModel):
               return (T)(object)new NaiveBayesModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, df.SparkSession, paramMap);
           case {} when typeof(T) == typeof(StringIndexerModel):
               return (T)(object)new StringIndexerModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, df.SparkSession, paramMap);
           case {} when typeof(T) == typeof(GBTClassifierModel):
               return (T)(object)new GBTClassifierModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, df.SparkSession, paramMap);
           default:
                throw new NotImplementedException($"Unable to create a `Transformer` or `Model` for {typeof(T).Name}");
        }
        
    }
    
}