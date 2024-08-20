using Apache.Arrow;
using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql.Types;

public class MapType : SparkDataType
{
    private readonly bool _isNullableValue;
    private readonly SparkDataType _keyType;
    private readonly SparkDataType _valueType;

    public MapType(SparkDataType keyType, SparkDataType valueType, bool isNullableValue) : base("Map")
    {
        _keyType = keyType;
        _valueType = valueType;
        _isNullableValue = isNullableValue;
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Map = new DataType.Types.Map
            {
                KeyType = _keyType.ToDataType(), ValueType = _valueType.ToDataType()
            }
        };
    }

    public override IArrowType ToArrowType()
    {
        var keyField = new Field("key", _keyType.ToArrowType(), false);
        var valueField = new Field("value", _valueType.ToArrowType(), _isNullableValue);
        return new Apache.Arrow.Types.MapType(keyField, valueField);
    }
}