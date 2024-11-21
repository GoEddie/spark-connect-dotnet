using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class DecimalType : SparkDataType
{
    public int Precision { get; set; }
    public int Scale { get; set; }

    public DecimalType() : base("Decimal")
    {
        Precision = 18;
        Scale = 2;
    }

    public DecimalType(int precision, int scale) : base("Decimal")
    {
        Precision = precision;
        Scale = scale;
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Decimal = new DataType.Types.Decimal() { Scale = Scale, Precision = Precision }
        };
    }

    public override IArrowType ToArrowType()
    {
        //The maximum precision for decimal128() is 38 significant digits,
        //while for decimal256() it is 76 digits.
        if (Precision > 38)
        {
            return new Decimal256Type(Precision, Scale);
        }
        else
        {
            return new Decimal128Type(Precision, Scale);
        }
    }
}