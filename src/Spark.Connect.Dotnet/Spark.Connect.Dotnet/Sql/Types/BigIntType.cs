using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class BigIntType : SparkDataType
{
    public BigIntType() : base("BigInt")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Long = new DataType.Types.Long()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Int64Type();
    }
}