using Spark.Connect.Dotnet.ML.LinAlg;

namespace Spark.Connect.Dotnet.Sql.Types;

public abstract class UserDefinedType(string typeName) : SparkDataType(typeName), IHasCustomJson
{
    public abstract Spark.Connect.Dotnet.Sql.Types.StructType GetStructType();
}