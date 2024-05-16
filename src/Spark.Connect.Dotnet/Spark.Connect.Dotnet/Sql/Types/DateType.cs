using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class DateType : SparkDataType
{
    public DateType() : base($"Date")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Date = new DataType.Types.Date()
            {
            }
        };
    }

    public override IArrowType ToArrowType()
    {
        return Date32Type.Default;
    }

    public override string SimpleString()
    {
        return $"date";
    }
}