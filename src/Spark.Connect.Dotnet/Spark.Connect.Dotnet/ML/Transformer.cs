using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML;

public class Model : Transformer
{
    public Model(string uid, string className, ObjectRef objRef, SparkSession sparkSession, ParamMap defaultParams)
        : base(uid, className, objRef, sparkSession, defaultParams)
    {
        OperatorType = MlOperator.Types.OperatorType.Model;
    }
    
}

public class Transformer(string uid, string className, ObjectRef objRef, SparkSession sparkSession, ParamMap defaultParams) : Params(defaultParams), Identifiable
{
    private readonly SparkSession _sparkSession = sparkSession;
    public string Uid { get; } = uid;
    private string ClassName { get; } = className;
    private ObjectRef ObjRef { get; } = objRef;
    
    protected MlOperator.Types.OperatorType OperatorType { get; set; } = MlOperator.Types.OperatorType.Transformer;
    
    public DataFrame Transform(DataFrame df)
    {
        var plan = new Plan()
        {
            Root = new Relation()
            {
                MlRelation = new MlRelation()
                {
                    Transform = new MlRelation.Types.Transform()
                    {
                        
                        Transformer = new MlOperator()
                        {
                            Uid = Uid, Name = ClassName, Type = MlOperator.Types.OperatorType.Transformer,
                        }, 
                        Input = df.Relation, 
                        ObjRef = ObjRef
                    }
                },
            }
        };

        var executor = new RequestExecutor(df.SparkSession, plan);
        return new DataFrame(df.SparkSession, executor.GetRelation());
    }

    public void Save(string path, bool overwrite = true)
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                MlCommand = new MlCommand()
                {
                    Write = new MlCommand.Types.Write()
                    { 
                        Path = path, ShouldOverwrite = overwrite, Params = new MlParams()
                        {
                            Params = { ParamMap.ToMapField() }
                        }, 
                        ObjRef = ObjRef
                    }
                }
            }
        };

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
    }

    protected static MlCommandResult Load(string path, SparkSession sparkSession, string className)
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                MlCommand = new MlCommand()
                {
                    Read = new MlCommand.Types.Read()
                    {
                        Path = path, 
                        Operator = new MlOperator()
                        {
                            Name = className,
                            Type = MlOperator.Types.OperatorType.Model
                        }
                    }
                }
            }
        };

        var executor = new RequestExecutor(sparkSession, plan);
        executor.Exec();

        return executor.GetMlCommandResult();
    }
    
    protected static string GetObjectRef(ObjectRef? objRef)
    {
        return objRef != null ? objRef.Id : Guid.NewGuid().ToString();
    }
}