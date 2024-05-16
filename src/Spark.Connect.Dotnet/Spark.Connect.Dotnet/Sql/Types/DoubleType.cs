using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class DoubleType : SparkDataType
{
    public DoubleType() : base("Double")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Double = new DataType.Types.Double()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.DoubleType();
    }
}