using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class DecimalType : SparkDataType
{
    public DecimalType() : base("Decimal")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType()
        {
            Decimal = new DataType.Types.Decimal()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Decimal128Type(28, new decimal().Scale);
    }
}