namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Wrap DATATYPE_MISMATCH.NON_FOLDABLE_INPUT
/// </summary>
public class DataTypeMismatchNonFoldableInputException : SparkException
{
    //DATATYPE_MISMATCH.NON_FOLDABLE_INPUT
    /// <summary>
    /// Wrap DATATYPE_MISMATCH.NON_FOLDABLE_INPUT
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
    public DataTypeMismatchNonFoldableInputException(string exceptionMessage, Exception exception) : base(
        exceptionMessage, exception)
    {
    }

    /// <summary>
    /// Wrap DATATYPE_MISMATCH.NON_FOLDABLE_INPUT
    /// </summary>
    /// <param name="exception"></param>
    public DataTypeMismatchNonFoldableInputException(Exception exception) : base(exception)
    {
    }
}