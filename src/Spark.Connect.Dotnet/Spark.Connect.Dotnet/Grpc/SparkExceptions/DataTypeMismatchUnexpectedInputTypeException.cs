namespace Spark.Connect.Dotnet.Grpc;

public class DataTypeMismatchUnexpectedInputTypeException : SparkException
{
    public DataTypeMismatchUnexpectedInputTypeException(string exceptionMessage, Exception exception) : base(
        exceptionMessage, exception)
    {
        //Format: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "sha1(id0)" due to data type mismatch: Parameter 1 requires the "BINARY" type, however "id0" has the type "BIGINT"
        OverrideMessage = exceptionMessage.Replace("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]", "");
    }

    public DataTypeMismatchUnexpectedInputTypeException(Exception exception) : base(exception)
    {
    }
}