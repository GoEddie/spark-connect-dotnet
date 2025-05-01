using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.ML.Classification;
using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML;

public class Transformer : Params, Identifiable
{
    /// <summary>
    /// This allows you to create transformers or models we don't already handle, just implement `Transformer` or `Model`.
    ///
    /// Unless you are trying to use a model/transformer not currently supported by spark connect dotnet, you don't need to use this directly
    /// </summary>
    /// <param name="uid">uid of the object, from the Spark Connect server </param>
    /// <param name="className">scala class name of the transformer you are trying to implement</param>
    /// <param name="objRef">this is from the response to `Fit`</param>
    /// <param name="sparkSession"></param>
    /// <param name="defaultParams">The default parameters that the transformer/model uses</param>
    public Transformer(string uid, string className, ObjectRef objRef, SparkSession sparkSession, ParamMap defaultParams) : 
        this(sparkSession, className, defaultParams)
            {
                Uid = uid;
                ObjRef = objRef;
            } 
    
    
    protected Transformer(SparkSession sparkSession, string className, ParamMap paramMap) : base(paramMap)
    {
        _sparkSession = sparkSession;
        ClassName = className;

        Uid = null;
        ObjRef = null;
    }
    
    private readonly SparkSession _sparkSession;
    public string? Uid { get; }
    private string ClassName { get; }
    private ObjectRef? ObjRef { get; }
    
    /// <summary>
    /// This is used by both transformers and models, specify what we have here
    /// </summary>
    protected MlOperator.Types.OperatorType OperatorType { get; set; } = MlOperator.Types.OperatorType.Transformer;
    
    /// <summary>
    /// Call `Transform` on the `DataFrame` using the transformer, optionally include a different set of parameters
    /// </summary>
    /// <param name="df">The `DataFrame` to transform</param>
    /// <param name="paramMapOverride">Optional parameters, otherwise existing parameters are used. This whole set of parameters is used</param>
    /// <returns>Transformed `DataFrame`</returns>
    public DataFrame Transform(DataFrame df, ParamMap? paramMapOverride = null)
    {
        var parameterMap = paramMapOverride ?? ParamMap;
        
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
                            Name = ClassName, 
                            Type = OperatorType,
                        }, 
                        Input = df.Relation,
                        Params = new MlParams()
                        {
                            Params = { parameterMap.ToMapField() }
                        }
                    }
                },
            }
        };

        if (Uid is not null)
        {
            plan.Root.MlRelation.Transform.Transformer.Uid = Uid;
        }

        if (ObjRef is not null)
        {
            plan.Root.MlRelation.Transform.ObjRef = ObjRef;
        }

        var executor = new RequestExecutor(df.SparkSession, plan);
        return new DataFrame(df.SparkSession, executor.GetRelation());
    }

    /// <summary>
    /// Save the ML instance to the input path (on the Spark Connect server)
    /// </summary>
    /// <param name="path">The path to save the object to</param>
    /// <param name="overwrite">Should we overwrite</param>
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

        if (OperatorType == MlOperator.Types.OperatorType.Transformer)
        {
            plan.Command.MlCommand.Write.Operator = new MlOperator
            {
                Name = ClassName, Type = OperatorType
            };
        }
        
        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
    }

    /// <summary>
    /// Reads an ML instance from the input path (on the Spark Connect server). This is used by the derived symbols, you probably don't need it
    ///   unless you are implementing a transformer that spark connect dotnet doesn't currently support
    /// </summary>
    /// <param name="path">The path to read the object from</param>
    /// <param name="sparkSession">The current `SparkSession`</param>
    /// <param name="className">The scala class name of the object to load</param>
    /// <returns>`MLCommandResult` with the Spark Connect server reference</returns>
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