using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class TimestampNtzType : SparkDataType
{
    public TimestampNtzType() : base("Timestamp")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            TimestampNtz = new DataType.Types.TimestampNTZ()
        };
    }

    public override IArrowType ToArrowType()
    {
        TimeZoneInfo? tz = null;
        var ts = new Apache.Arrow.Types.TimestampType(TimeUnit.Microsecond, tz);
        return ts;
    }

    public override string SimpleString()
    {
        return "timestamp_ntz";
    }
}