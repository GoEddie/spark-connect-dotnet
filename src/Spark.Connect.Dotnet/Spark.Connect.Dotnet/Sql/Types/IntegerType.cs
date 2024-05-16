using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class IntegerType : SparkDataType
{
    public IntegerType() : base("Int")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Integer = new DataType.Types.Integer()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Int32Type();
    }

    public override string JsonTypeName()
    {
        return "integer";
    }
}