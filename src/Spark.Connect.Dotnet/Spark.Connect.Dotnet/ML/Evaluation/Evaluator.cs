using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Evaluation;

/// <summary>
/// Base class for evaluators that compute metrics on a dataset.
/// </summary>
public abstract class Evaluator : Params, Identifiable
{
    protected readonly SparkSession SparkSession;
    private readonly string _className;

    public string Uid { get; }

    protected Evaluator(SparkSession sparkSession, string uid, string className, ParamMap paramMap) : base(paramMap)
    {
        SparkSession = sparkSession;
        Uid = uid;
        _className = className;
    }

    /// <summary>
    /// Evaluates the output of the model on the given dataset.
    /// </summary>
    /// <param name="df">The dataset to evaluate.</param>
    /// <returns>The metric value.</returns>
    public double Evaluate(DataFrame df)
    {
        var plan = new Plan()
        {
            Command = new Command()
            {
                MlCommand = new MlCommand()
                {
                    Evaluate = new MlCommand.Types.Evaluate()
                    {
                        Dataset = df.Relation,
                        Params = new MlParams()
                        {
                            Params = { ParamMap.ToMapField() }
                        },
                        Evaluator = new MlOperator()
                        {
                            Uid = Uid,
                            Name = _className,
                            Type = MlOperator.Types.OperatorType.Evaluator
                        }
                    }
                }
            }
        };

        var executor = new RequestExecutor(SparkSession, plan);
        executor.Exec();

        var mlResult = executor.GetMlCommandResult();

        if (mlResult.ResultTypeCase == MlCommandResult.ResultTypeOneofCase.Param)
        {
            return ParamMap.GetValueFromLiteral(mlResult.Param);
        }

        throw new InvalidOperationException($"Expected evaluation result but got {mlResult.ResultTypeCase}");
    }
}
