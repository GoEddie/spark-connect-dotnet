using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class FloatType : SparkDataType
{
    public FloatType() : base("Float")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Float = new DataType.Types.Float()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.FloatType();
    }
}