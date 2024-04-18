using System.Text.RegularExpressions;

namespace Spark.Connect.Dotnet.Sql.Types;

public abstract class SparkDataType
{
    public abstract DataType ToDataType();
    
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
    public static BigIntType BigIntType() => new ();
    public static IntType IntType() => new();
    public static StructType StructType(params StructField[] fields) => new (fields);

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
                return new IntType();
            
            case "byte":
                return new ByteType();
        }

        if (lower.StartsWith("array"))
        {
            var elementType = Regex.Match(lower, "<(.*?)>").Groups[0];
            var elementSparkType = FromString(elementType.Value);
            return new ArrayType(elementSparkType);
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
}

public class IntType : SparkDataType
{
    public IntType() : base("Int")
    {
    }
    
    public override DataType ToDataType() =>
        new()
        {
            Integer = new DataType.Types.Integer()
        };
}

public class BigIntType : SparkDataType
{
    public BigIntType() : base("BigInt")
    {
    }
    
    public override DataType ToDataType() =>
        new()
        {
            Integer = new DataType.Types.Integer()
        };
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

    public override string SimpleString()
    {
        return $"array<{_elementType.SimpleString()}>";
    }
}