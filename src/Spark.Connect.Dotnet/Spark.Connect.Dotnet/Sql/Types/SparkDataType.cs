using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql.Types;

public abstract class SparkDataType
{
    private readonly string _typeName;

    public SparkDataType(string typeName)
    {
        _typeName = typeName;
    }

    public string TypeName => _typeName.ToLower();
    public abstract DataType ToDataType();
    public abstract IArrowType ToArrowType();

    public virtual string SimpleString()
    {
        return TypeName;
    }

    public virtual string JsonValue()
    {
        return TypeName;
    }

    public static ByteType ByteType()
    {
        return new ByteType();
    }

    public static ShortType ShortType()
    {
        return new ShortType();
    }

    public static StringType StringType()
    {
        return new StringType();
    }

    public static BigIntType LongType()
    {
        return new BigIntType();
    }

    public static FloatType FloatType()
    {
        return new FloatType();
    }
    
    public static BigIntType BigIntType()
    {
        return new BigIntType();
    }

    public static IntegerType IntegerType()
    {
        return new IntegerType();
    }

    public static BinaryType BinaryType()
    {
        return new BinaryType();
    }

    public static BooleanType BooleanType()
    {
        return new BooleanType();
    }

    public static DoubleType DoubleType()
    {
        return new DoubleType();
    }

    public static IntegerType IntType()
    {
        return new IntegerType();
    }

    public static VoidType VoidType()
    {
        return new VoidType();
    }
    
    public static DateType DateType()
    {
        return new DateType();
    }
    
    
    public static TimestampType TimestampType()
    {
        return new TimestampType();
    }

    public static StructType StructType(params StructField[] fields)
    {
        return new StructType(fields);
    }

    public static ArrayType ArrayType(SparkDataType elementType, bool nullableValues)
    {
        return new ArrayType(elementType, nullableValues);
    }

    public static MapType MapType(SparkDataType keyType, SparkDataType valueType, bool nullableValue)
    {
        return new MapType(keyType, valueType, nullableValue);
    }

    public static SparkDataType FromString(string type)
    {
        var lower = type.ToLowerInvariant();

        switch (lower)
        {
            case "str":
            case "string":
                return new StringType();

            case "long":
            case "bigint":
            case "int64":
                return new BigIntType();

            case "short":
            case "int16":
                return new ShortType();

            case "int":
            case "int32":
                return new IntegerType();

            case "byte":
                return new ByteType();

            case "binary":
                return new BinaryType();
            
            case "double":
                return new DoubleType();
            
            case "float":
                return new FloatType();

            case "bool":
            case "boolean":
                return new BooleanType();
            
            case "null":
            case "void":
                return new VoidType();
            
            case "timestamp":
                return new TimestampType();
            
            case "timestampntz":
                return new TimestampNtzType();
            
            case "date":
                return new DateType();
        }

        if (lower.StartsWith("array"))
        {
            var elementType = Regex.Match(lower, "<(.*?)>").Groups[0];
            var elementSparkType = FromString(elementType.Value);
            return new ArrayType(elementSparkType, true);
        }

        if (lower.StartsWith("map"))
        {
            try
            {
                var matches = Regex.Match(lower, "<(.*?),(.*?)>");
                var keyType = matches.Groups[0].Value;
                var valueType = matches.Groups[1].Value;

                return new MapType(FromString(keyType), FromString(valueType), true);
            }
            catch (Exception ex)
            {
                throw new SparkException(
                    $"Expected map format like 'map<keyType,valueType>' but couldn't figure out the types from {lower}",
                    ex);
            }
        }


        throw new NotImplementedException($"Missing DataType From String: '{type}'");
    }
    

    public static SparkDataType FromDotNetType(object o) => o switch
    {
        int => IntType(),
        long => LongType(),
        double => DoubleType(),
        float => FloatType(),
        short => ShortType(),
        string => StringType(),
        DateTime => TimestampType(),
        DateOnly => DateType(),
        byte => ByteType(),

        _ => throw new ArgumentOutOfRangeException($"Type {o.GetType().Name} needs a FromDotNetType")
    };

    public static SparkDataType FromSparkConnectType(DataType type) 
    {
        if (type.Array != null)
        {
            bool nullableValues = type.Array.ContainsNull;
            return ArrayType(FromSparkConnectType(type.Array.ElementType), nullableValues);
        }

        if (type.String != null)
        {
            return StringType();
        }

        if (type.Boolean != null)
        {
            return BooleanType();
        }

        if (type.Integer != null)
        {
            return IntegerType();
        }

        if (type.Double != null)
        {
            return DoubleType();
        }

        if (type.Map != null)
        {
            var valueType = FromSparkConnectType(type.Map.ValueType);
            return new MapType(FromSparkConnectType(type.Map.KeyType), valueType, type.Map.ValueContainsNull);
        }

        if (type.Binary != null)
        {
            return new BinaryType();
        }

        if (type.Long != null)
        {
            return new BigIntType();
        }

        if (type.Date != null)
        {
            return new DateType();
        }

        if (type.Timestamp != null)
        {
            return new TimestampType();
        }

        if (type.TimestampNtz != null)
        {
            return new TimestampNtzType();
        }

        if (type.Struct != null)
        {
            return new StructType(type.Struct.Fields);
        }
        
        
        throw new NotImplementedException();
    }
}

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

public class FloatType : SparkDataType
{
    public FloatType() : base("Float")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Float = new DataType.Types.Float()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.FloatType();
    }
}

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

public class BooleanType : SparkDataType
{
    public BooleanType() : base("Boolean")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Boolean = new DataType.Types.Boolean()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.BooleanType();
    }
}

public class DoubleType : SparkDataType
{
    public DoubleType() : base("Double")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Double = new DataType.Types.Double()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.DoubleType();
    }
}

public class ShortType : SparkDataType
{
    public ShortType() : base("Short")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Short = new DataType.Types.Short()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Int16Type();
    }
}

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

public class IntegerType : SparkDataType
{
    public IntegerType() : base("Int")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Integer = new DataType.Types.Integer()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Int32Type();
    }
}

public class BigIntType : SparkDataType
{
    public BigIntType() : base("BigInt")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Long = new DataType.Types.Long()
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Int64Type();
    }
}

public class MapType : SparkDataType
{
    private readonly SparkDataType _keyType;
    private readonly SparkDataType _valueType;
    private readonly bool _isNullableValue;

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
                KeyType = _keyType.ToDataType(),
                ValueType = _valueType.ToDataType()
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

public class ArrayType : SparkDataType
{
    private readonly SparkDataType _elementType;
    private readonly bool _nullableValues;

    public ArrayType(SparkDataType elementType, bool nullableValues) : base($"Array<{elementType.TypeName}>")
    {
        _elementType = elementType;
        _nullableValues = nullableValues;
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Array = new DataType.Types.Array
            {
                ElementType = _elementType.ToDataType()
            }
        };
    }

    public override IArrowType ToArrowType()
    {
        var elementType = _elementType.ToArrowType();
        var childField = new Field("element", elementType, _nullableValues);
        return new ListType(childField);
    }

    public override string SimpleString()
    {
        return $"array<{_elementType.SimpleString()}>";
    }
}


public class VoidType : SparkDataType
{
    public VoidType() : base($"Void")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Null = new DataType.Types.NULL()
        };
    }

    public override IArrowType ToArrowType()
    {
        return NullType.Default;
    }

    public override string SimpleString()
    {
        return $"void";
    }
}

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
        };
    }

    public override IArrowType ToArrowType()
    {
        return Apache.Arrow.Types.Date32Type.Default;
    }

    public override string SimpleString()
    {
        return $"date";
    }
}


public class TimestampType : SparkDataType
{
    public TimestampType() : base($"Timestamp")
    {
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Timestamp = new DataType.Types.Timestamp(),
        };
    }

    public override IArrowType ToArrowType()
    {
        var ts = Apache.Arrow.Types.TimestampType.Default;
        return ts;
    }

    public override string SimpleString()
    {
        return $"timestamp";
    }
}

public class TimestampNtzType : SparkDataType
{
    public TimestampNtzType() : base($"Timestamp")
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
        return new Apache.Arrow.Types.Time64Type(); //Apache.Arrow.Types.TimestampType(timezone: "+00:00");
    }

    public override string SimpleString()
    {
        return $"timestampntz";
    }
}

