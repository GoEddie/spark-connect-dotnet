using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class TimeType : SparkDataType
{
    public TimeType() : base("time")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Time = new DataType.Types.Time()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.Time64Type();
    }

    public override string SimpleString()
    {
        return "time";
    }
}