using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class BooleanType : SparkDataType
{
    public BooleanType() : base("Boolean")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Boolean = new DataType.Types.Boolean()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.BooleanType();
    }
}