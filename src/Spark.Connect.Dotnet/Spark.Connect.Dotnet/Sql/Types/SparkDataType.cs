using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
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

    public virtual string JsonTypeName()
    {
        return SimpleString();
    }

    public virtual string ToDdl(string name, bool nullable)
    {
        var ddl = new System.Text.StringBuilder();
        ddl.Append(name);
        ddl.Append(" ");
        ddl.Append(this.TypeName);
        if (!nullable)
        {
            ddl.Append(" NOT NULL");
        }
        return ddl.ToString();
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

    public static TimestampNtzType TimestampNtzType()
    {
        return new TimestampNtzType();
    }

    public static YearMonthIntervalType YearMonthIntervalType()
    {
        return new YearMonthIntervalType();
    }

    public static StructType StructType(params StructField[] fields)
    {
        return new StructType(fields);
    }

    public static ArrayType ArrayType(SparkDataType elementType, bool nullableValues = true)
    {
        return new ArrayType(elementType, nullableValues);
    }

    public static MapType MapType(SparkDataType keyType, SparkDataType valueType, bool nullableValue)
    {
        return new MapType(keyType, valueType, nullableValue);
    }

    public static VariantType VariantType()
    {
        return new VariantType();
    }

    public static NullType NullType()
    {
        return new NullType();
    }

    public static DecimalType DecimalType()
    {
        return new DecimalType();
    }

    public static DecimalType DecimalType(int precision, int scale)
    {
        return new DecimalType(precision, scale);
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
            
            case "decimal":
                return new DecimalType();

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
                return new NullType();
            case "void":
                return new VoidType();

            case "timestamp":
                return new TimestampType();

            case "timestampntz":
                return new TimestampNtzType();

            case "date":
                return new DateType();
            
            case "variant":
                return new VariantType();
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
        IDictionary<string, long?> => MapType(StringType(), LongType(), true),
        IDictionary<string, int?> => MapType(StringType(), IntType(), true),
        IDictionary<string, string?> => MapType(StringType(), StringType(), true),
        IDictionary<string, object> dict => MapType(StringType(), FromDotNetType(dict.Values.FirstOrDefault()), true),

        string[] => ArrayType(StringType()),

        _ => throw new ArgumentOutOfRangeException($"Type {o.GetType().Name} needs a FromDotNetType")
    };

    public static SparkDataType FromSparkConnectType(DataType type)
    {
        if (type.Array != null)
        {
            var nullableValues = type.Array.ContainsNull;
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

        if (type.Byte != null)
        {
            return new ByteType();
        }

        if (type.YearMonthInterval != null)
        {
            return new YearMonthIntervalType(type.YearMonthInterval.StartField, type.YearMonthInterval.EndField);
        }

        if (type.Variant != null)
        {
            return new VariantType();
        }

        if (type.Float != null)
        {
            return new FloatType();
        }

        if (type.Null != null)
        {
            return new NullType();
        }

        if (type.Decimal != null)
        {
            return new DecimalType();
        }
        
        throw new NotImplementedException($"Need Type For '{type.KindCase}'");
    }
}