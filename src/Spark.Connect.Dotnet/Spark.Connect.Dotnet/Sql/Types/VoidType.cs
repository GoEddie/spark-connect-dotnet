using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class VoidType : SparkDataType
{
    public VoidType() : base("Void")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Null = new DataType.Types.NULL()
        };
    }

    public override IArrowType ToArrowType()
    {
        return NullType.Default;
    }

    public override string SimpleString()
    {
        return "void";
    }
}