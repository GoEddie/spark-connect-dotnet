using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class TimestampType : SparkDataType
{
    public TimestampType() : base("Timestamp")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Timestamp = new DataType.Types.Timestamp()
        };
    }

    public override IArrowType ToArrowType()
    {
        var ts = new Apache.Arrow.Types.TimestampType(TimeUnit.Microsecond, TimeZoneInfo.Utc);
        return ts;
    }

    public override string SimpleString()
    {
        return "timestamp";
    }
}