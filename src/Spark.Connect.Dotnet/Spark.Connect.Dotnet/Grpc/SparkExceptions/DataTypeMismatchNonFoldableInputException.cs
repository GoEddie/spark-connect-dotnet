namespace Spark.Connect.Dotnet.Grpc;

public class DataTypeMismatchNonFoldableInputException : SparkException{
    //DATATYPE_MISMATCH.NON_FOLDABLE_INPUT
    public DataTypeMismatchNonFoldableInputException(string exceptionMessage, Exception exception) : base(exceptionMessage, exception)
    {
        
    }

    public DataTypeMismatchNonFoldableInputException(Exception exception) : base(exception)
    {
        
    }
}