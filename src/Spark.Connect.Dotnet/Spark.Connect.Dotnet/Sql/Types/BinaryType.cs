using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class BinaryType : SparkDataType
{
    public BinaryType() : base("Binary")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Binary = new DataType.Types.Binary()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.BinaryType();
    }
}