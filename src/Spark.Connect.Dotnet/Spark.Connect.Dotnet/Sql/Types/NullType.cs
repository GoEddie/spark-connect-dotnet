using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class NullType : SparkDataType
{
    public NullType() : base("NULL")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType()
        {
            Null = new DataType.Types.NULL()
            {
            }
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.NullType();
    }
}