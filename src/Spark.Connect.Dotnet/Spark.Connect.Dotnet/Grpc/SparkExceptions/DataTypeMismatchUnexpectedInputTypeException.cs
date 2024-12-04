namespace Spark.Connect.Dotnet.Grpc;

/// <summary>
/// Wrap [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]
/// </summary>
public class DataTypeMismatchUnexpectedInputTypeException : SparkException
{
    /// <summary>
    /// Wrap [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]
    /// </summary>
    /// <param name="exceptionMessage"></param>
    /// <param name="exception"></param>
    public DataTypeMismatchUnexpectedInputTypeException(string exceptionMessage, Exception exception) : base(
        exceptionMessage, exception)
    {
        //Format: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "sha1(id0)" due to data type mismatch: Parameter 1 requires the "BINARY" type, however "id0" has the type "BIGINT"
        OverrideMessage = exceptionMessage.Replace("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]", "");
    }

    /// <summary>
    /// Wrap [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]
    /// </summary>
    /// <param name="exception"></param>
    public DataTypeMismatchUnexpectedInputTypeException(Exception exception) : base(exception)
    {
    }
}