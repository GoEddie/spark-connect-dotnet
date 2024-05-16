using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class ShortType : SparkDataType
{
    public ShortType() : base("Short")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Short = new DataType.Types.Short()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Int16Type();
    }
}