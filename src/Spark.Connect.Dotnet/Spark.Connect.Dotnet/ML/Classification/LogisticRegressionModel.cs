using Spark.Connect.Dotnet.ML.Param;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Classification;

/// <summary>
/// Model fitted by `LogisticRegression`.
/// </summary>
public class LogisticRegressionModel(string uid, ObjectRef objRef, SparkSession sparkSession, ParamMap paramMap)
    : Model(uid, ClassName, objRef, sparkSession, paramMap)
{
    private const string ClassName = "org.apache.spark.ml.classification.LogisticRegressionModel";

    /// <summary>
    /// Load a `LogisticRegressionModel` that was previously saved to disk on the Spark Connect server
    /// </summary>
    /// <param name="path">Where to read the `LogisticRegressionModel` from</param>
    /// <param name="sparkSession">A `SparkSession` to read the model through</param>
    /// <returns>`LogisticRegressionModel`</returns>
    public static LogisticRegressionModel Load(string path, SparkSession sparkSession)
    {
        var mlResult = Transformer.Load(path, sparkSession, ClassName);
        var paramMap = ParamMap.FromMLOperatorParams(mlResult.OperatorInfo.Params.Params, LogisticRegression.DefaultParams.Clone());

        var loadedModel = new LogisticRegressionModel(mlResult.OperatorInfo.Uid, mlResult.OperatorInfo.ObjRef, sparkSession, paramMap);

        return loadedModel;
    }

    /// <summary>
    /// Gets the model coefficients (for binary classification).
    /// </summary>
    public List<double> Coefficients => Fetch("coefficients");

    /// <summary>
    /// Gets the model intercept (for binary classification).
    /// </summary>
    public double Intercept => Fetch("intercept");

    /// <summary>
    /// Gets the coefficient matrix (for multinomial classification).
    /// Each row corresponds to a class, each column to a feature.
    /// </summary>
    public List<List<double>> CoefficientMatrix => Fetch("coefficientMatrix");

    /// <summary>
    /// Gets the intercept vector (for multinomial classification).
    /// </summary>
    public List<double> InterceptVector => Fetch("interceptVector");

    /// <summary>
    /// Gets the number of classes.
    /// </summary>
    public int NumClasses => Fetch("numClasses");

    /// <summary>
    /// Gets the number of features the model was trained on.
    /// </summary>
    public int NumFeatures => Fetch("numFeatures");
}