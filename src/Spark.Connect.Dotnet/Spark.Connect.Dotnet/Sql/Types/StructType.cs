using Apache.Arrow;
using Apache.Arrow.Types;
using Google.Protobuf.Collections;
using static Spark.Connect.Dotnet.Sql.Types.SparkDataType;

namespace Spark.Connect.Dotnet.Sql.Types;

public class StructType : SparkDataType
{
    public StructType(DataType.Types.Struct source) : base("StructType")
    {
        Fields = new List<StructField>();
        foreach (var field in source.Fields)
        {
            Fields.Add(new StructField(field.Name, field.DataType, field.Nullable));
        }
    }

    public StructType(RepeatedField<DataType.Types.StructField> source) : base("StructType")
    {
        Fields = new List<StructField>();
        foreach (var field in source)
        {
            Fields.Add(new StructField(field.Name, field.DataType, field.Nullable));
        }
    }

    public StructType(params StructField[] fields) : base("StructType")
    {
        Fields = fields.ToList();
    }
    
    public StructType(List<StructField> fields) : base("StructType")
    {
        Fields = fields;
    }

    public StructType(Schema readerSchema) : base("StructType")
    {

        Fields = new List<StructField>();
        
        foreach (var field in readerSchema.FieldsList)
        {
            var sparkField = new StructField(field.Name, field.DataType, field.IsNullable);
            Fields.Add(sparkField);
        }
    }

    public List<StructField> Fields { get; }

    public StructType Add(string name, SparkDataType type, bool nullable)
    {
        Fields.Add(new StructField(name, type, nullable));
        return this;
    }

    public List<string> FieldNames()
    {
        return Fields.Select(p => p.Name).ToList();
    }

    public override string SimpleString()
    {
        string GetNameAndType(StructField field)
        {
            return $"{field.Name}:{field.DataType.SimpleString()}";
        }

        return $"StructType<{string.Join(",", Fields.Select(GetNameAndType))}>";
    }

    public override DataType ToDataType()
    {
        var fields = Fields.Select(field => new DataType.Types.StructField
        {
            DataType = field.DataType.ToDataType(),
            Name = field.Name,
            Nullable = field.Nullable
        });

        return new DataType
        {
            Struct = new DataType.Types.Struct
            {
                Fields = { fields }
            }
        };
    }

    public override IArrowType ToArrowType()
    {
        return new Apache.Arrow.Types.StructType(Fields
            .Select(field => new Field(field.Name, field.DataType.ToArrowType(), field.Nullable)).ToList());
    }
}

public class StructField
{
    public StructField()
    {
    }

    public StructField(string name, SparkDataType type, bool nullable)
    {
        Name = name;
        DataType = type;
        Nullable = nullable;
    }

    public StructField(string name, DataType type, bool nullable)
    {
        Name = name;
        Nullable = nullable;
        DataType = FromConnectDataType(type);
    }

    public StructField(string name, IArrowType type, bool nullable)
    {
        Name = name;
        Nullable = nullable;
        DataType = FromArrowType(type);
    }

    public string Name { get; set; }
    
    public SparkDataType DataType { get; set; }
    public bool Nullable { get; set; }

    private SparkDataType FromArrowType(IArrowType type) => type.TypeId switch
    {
        ArrowTypeId.Null => VoidType(),
        ArrowTypeId.Boolean => BooleanType(),
        ArrowTypeId.Int8 => ByteType(),
        ArrowTypeId.Int16 => ShortType(),
        ArrowTypeId.Int32 => IntegerType(),
        ArrowTypeId.Int64 => LongType(),
        ArrowTypeId.Float => FloatType(),
        ArrowTypeId.Double => DoubleType(),
        ArrowTypeId.String => StringType(),
        ArrowTypeId.Binary => BinaryType(),
        ArrowTypeId.Date32 => DateType(),
        ArrowTypeId.Timestamp => TimestampType(),
        ArrowTypeId.Struct => StructType(),
        ArrowTypeId.List => ArrayType(FromArrowType((type as ListType).ValueDataType)),
        
        ArrowTypeId.Map => MapType(FromArrowType((type as Apache.Arrow.Types.MapType).KeyField.DataType), FromArrowType((type as Apache.Arrow.Types.MapType).ValueField.DataType)),
        _ => throw new ArgumentOutOfRangeException($"Cannot convert Arrow Type '{type}'")
    };
    
    private SparkDataType FromConnectDataType(DataType type)
    {
        if (type.String != null)
        {
            return new StringType();
        }

        if (type.Short != null)
        {
            return new ShortType();
        }

        if (type.Integer != null)
        {
            return new IntegerType();
        }

        if (type.Long != null)
        {
            return new BigIntType();
        }

        if (type.Byte != null)
        {
            return new ByteType();
        }

        if (type.Struct != null)
        {
            return new StructType(type.Struct.Fields);
        }

        if (type.Array != null)
        {
            var elementType = FromConnectDataType(type.Array.ElementType);
            return new ArrayType(elementType);
        }

        if (type.Binary != null)
        {
            return new BinaryType();
        }

        if (type.Boolean != null)
        {
            return new BooleanType();
        }

        if (type.Double != null)
        {
            return new DoubleType();
        }

        if (type.Map != null)
        {
            return new MapType(FromConnectDataType(type.Map.KeyType), FromConnectDataType(type.Map.ValueType));
        }
        
        if(type.Date != null)
        {
            return new DateType();
        }

        if (type.Timestamp != null)
        {
            return new TimestampType();
        }

        return new VoidType();
    }
}