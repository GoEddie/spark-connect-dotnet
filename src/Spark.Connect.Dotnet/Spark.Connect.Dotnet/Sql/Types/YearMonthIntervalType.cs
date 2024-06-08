using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class YearMonthIntervalType : SparkDataType
{
    public int Year { get; }
    public int Month { get; }

    public YearMonthIntervalType(int year, int month) : base("YearMonthInterval")
    {
        Year = year;
        Month = month;
    }
    
    public YearMonthIntervalType() : base("YearMonthInterval")
    {
        Year = 0;
        Month = 1;
    }

    public override DataType ToDataType()
    {
        return new StructType(new StructField("year", LongType(), false), new StructField("month", LongType(), false)).ToDataType();
    }

    public override IArrowType ToArrowType()
    {
        return new StructType(new StructField("year", LongType(), false), new StructField("month", LongType(), false)).ToArrowType();
    }
}