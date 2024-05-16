using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class ByteType : SparkDataType
{
    public ByteType() : base("Byte")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Byte = new DataType.Types.Byte()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Int8Type();
    }
    
}