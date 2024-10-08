using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class VariantType : SparkDataType
{
    public VariantType() : base("Variant")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Variant = new()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.StringType();
    }
}