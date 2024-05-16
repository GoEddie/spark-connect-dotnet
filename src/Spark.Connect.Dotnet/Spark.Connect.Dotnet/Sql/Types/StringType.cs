using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class StringType : SparkDataType
{
    public StringType() : base("String")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            String = new DataType.Types.String()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.StringType();
    }
}