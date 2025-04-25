using Apache.Arrow;
using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql;

public class ArrowHelpers
{
    public static IArrowArrayBuilder GetArrowBuilderForArrowType(IArrowType type) => type.TypeId switch
    {
        ArrowTypeId.Boolean => new BooleanArray.Builder(),
        ArrowTypeId.Int8 => new Int8Array.Builder(),
        ArrowTypeId.Int16 => new Int16Array.Builder(),
        ArrowTypeId.Int32 => new Int32Array.Builder(),
        ArrowTypeId.Int64 => new Int64Array.Builder(),
        ArrowTypeId.Float => new FloatArray.Builder(),
        ArrowTypeId.Double => new DoubleArray.Builder(),
        ArrowTypeId.Decimal128 => new Decimal128Array.Builder((Decimal128Type)type),
        ArrowTypeId.Decimal256 => new Decimal256Array.Builder((Decimal256Type)type),
        ArrowTypeId.String => new StringArray.Builder(),
        ArrowTypeId.Binary => new BinaryArray.Builder(),
        ArrowTypeId.Date32 => new Date32Array.Builder(),
        ArrowTypeId.Date64 => new Date64Array.Builder(),
        ArrowTypeId.Timestamp => new TimestampArray.Builder(),
        ArrowTypeId.Struct => throw new NotImplementedException("NEED struct builder"),
        ArrowTypeId.List => new ListArray.Builder(((ListType)type).ValueField.DataType),
        ArrowTypeId.Map => throw new NotImplementedException("NEED map builder"),
        ArrowTypeId.Interval => new DayTimeIntervalArray.Builder(),
        
        _ => throw new ArgumentOutOfRangeException($"Cannot convert Arrow Type '{type}'")
    };
}