using System.Text.RegularExpressions;
using Apache.Arrow;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Grpc;

namespace Spark.Connect.Dotnet.Sql.Types;

public abstract class SparkDataType
{
    public abstract DataType ToDataType();
    public abstract IArrowType ToArrowType();
    
    private readonly string _typeName;

    public SparkDataType(string typeName)
    {
        _typeName = typeName;
    }

    public string TypeName => _typeName.ToLower();
    public virtual string SimpleString() => TypeName;
    public virtual string JsonValue() => TypeName;
    
    public static ByteType ByteType() => new ();
    public static ShortType ShortType() => new ();
    public static StringType StringType() => new ();
    public static BigIntType LongType() => new ();
    
    public static BigIntType BigIntType() => new ();
    public static IntegerType IntegerType() => new();

    public static BinaryType BinaryType() => new();
    public static BooleanType BooleanType() => new();

    public static DoubleType DoubleType() => new();
    
    public static IntegerType IntType() => new();
    public static StructType StructType(params StructField[] fields) => new (fields);

    public static ArrayType ArrayType(SparkDataType elementType) => new(elementType);
    
    public static MapType MapType(SparkDataType keyType, SparkDataType valueType) => new(keyType, valueType);

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
            
            case "bool":
            case "boolean":
                return new BooleanType();
        }

        if (lower.StartsWith("array"))
        {
            var elementType = Regex.Match(lower, "<(.*?)>").Groups[0];
            var elementSparkType = FromString(elementType.Value);
            return new ArrayType(elementSparkType);
        }
        
        if (lower.StartsWith("map"))
        {
            try
            {
                var matches = Regex.Match(lower, "<(.*?),(.*?)>");
                var keyType = matches.Groups[0].Value;
                var valueType = matches.Groups[1].Value;

                return new MapType(FromString(keyType), FromString(valueType));
            }
            catch (Exception ex)
            {
                throw new SparkException(
                    $"Expected map format like 'map<keyType,valueType>' but couldn't figure out the types from {lower}", ex);
            }
        }


        throw new NotImplementedException($"Missing DataType From String: '{type}'");
    }
}

public class ByteType : SparkDataType
{
    public ByteType() : base("Byte")
    {
    }
    
    public override DataType ToDataType() =>
        new()
        {
            Byte = new DataType.Types.Byte()
        };

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.Int8Type();
    }
}

public class BinaryType : SparkDataType
{
    public BinaryType() : base("Binary")
    {
    }
    
    public override DataType ToDataType() =>
        new()
        {
           Binary = new DataType.Types.Binary()
        };

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
    
    public override DataType ToDataType() =>
        new()
        {
            Boolean = new DataType.Types.Boolean()
        };

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
    
    public override DataType ToDataType() =>
        new()
        {
            Double = new DataType.Types.Double()
        };

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
    
    public override DataType ToDataType() =>
        new()
        {
            Short = new DataType.Types.Short()
        };

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.Int16Type();
    }
}

public class StringType : SparkDataType
{
    public StringType() : base("String")
    {
    }
    
    public override DataType ToDataType() =>
        new()
        {
            String = new DataType.Types.String()
            {

            }
        };

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
    
    public override DataType ToDataType() =>
        new()
        {
            Integer = new DataType.Types.Integer()
        };

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.Int32Type();
    }
}

public class BigIntType : SparkDataType
{
    public BigIntType() : base("BigInt")
    {
    }
    
    public override DataType ToDataType() =>
        new()
        {
            Long = new DataType.Types.Long()
        };

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.Int64Type();
    }
}

public class MapType : SparkDataType
{
    private readonly SparkDataType _keyType;
    private readonly SparkDataType _valueType;

    public MapType(SparkDataType keyType, SparkDataType valueType) : base("Map")
    {
        _keyType = keyType;
        _valueType = valueType;
    }
    
    public override DataType ToDataType() =>
        new()
        {
            Map = new DataType.Types.Map()
            {
                KeyType = _keyType.ToDataType(),
                ValueType = _valueType.ToDataType()
            }
        };

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.DictionaryType(_keyType.ToArrowType(), _valueType.ToArrowType(), false);
    }
}

public class ArrayType : SparkDataType
{
    private readonly SparkDataType _elementType;

    public ArrayType(SparkDataType elementType) : base($"Array<{elementType.TypeName}>")
    {
        _elementType = elementType;
    }
    
    public override DataType ToDataType() =>
        new()
        {
            Array = new DataType.Types.Array()
            {
                ElementType = _elementType.ToDataType()
            }
        };

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.ListType(_elementType.ToArrowType());
    }

    public override string SimpleString()
    {
        return $"array<{_elementType.SimpleString()}>";
    }
}