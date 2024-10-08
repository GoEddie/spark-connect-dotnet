using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class LongType : SparkDataType
{
    public LongType() : base("Long")
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