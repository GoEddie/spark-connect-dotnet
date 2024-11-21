using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Google.Protobuf.Collections;
using Grpc.Core;
using Spark.Connect.Dotnet.Grpc;
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

    public StructType(DataType? schema) : base("StructType")
    {
        if (schema == null)
        {
            throw new SparkException("Did not receive a schema from the gRPC call");
        }

        Fields = new List<StructField>();

        foreach (var field in schema.Struct.Fields)
        {
            Fields.Add(new StructField(field.Name, field.DataType, field.Nullable));
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

    public override string ToDdl(string name, bool nullable)
    {
        var ddl = new StringBuilder();
        ddl.Append(name);
        ddl.Append(" ");
        ddl.Append(ToDdl());
        if (!nullable)
        {
            ddl.Append(" NOT NULL");
        }
        return ddl.ToString();
    }

    public string ToDdl()
    {
        var ddl = new StringBuilder();
        ddl.Append("STRUCT<");
        ddl.Append(FieldsAsDdl());
        ddl.Append(">");
        return ddl.ToString();
    }

    public string FieldsAsDdl()
    {
        return string.Join(", ", Fields.Select(f => f.Ddl()));
    }

    public string Json()
    {
        return DataTypeJsonSerializer.StructTypeToJson(this);
    }

    public override DataType ToDataType()
    {
        var fields = Fields.Select(field => new DataType.Types.StructField
        {
            DataType = field.DataType.ToDataType(), Name = field.Name, Nullable = field.Nullable
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

    public static StructField StructField(string name, SparkDataType type, bool isNullable = true)
    {
        return new StructField(name, type, isNullable);
    }
}

public class StructField
{
    public StructField()
    {
        Name = string.Empty;
        DataType = new StringType();
        Metadata = new Dictionary<string, object>();
    }

    public StructField(string name, SparkDataType type, bool nullable, IDictionary<string, object>? metadata = null)
    {
        Name = name;
        DataType = type;
        Nullable = nullable;
        Metadata = metadata ?? new Dictionary<string, object>();
    }

    public StructField(string name, DataType type, bool nullable, IDictionary<string, object>? metadata = null)
    {
        Name = name;
        Nullable = nullable;
        DataType = FromConnectDataType(type);
        Metadata = metadata ?? new Dictionary<string, object>();
    }

    public StructField(string name, IArrowType type, bool nullable, IDictionary<string, object>? metadata = null)
    {
        Name = name;
        Nullable = nullable;
        DataType = FromArrowType(type);
        Metadata = metadata ?? new Dictionary<string, object>();
    }

    public string Name { get; set; }
    public SparkDataType DataType { get; set; }
    public bool Nullable { get; set; }

    public string Ddl()
    {
        if (DataType is ArrayType arrayDataType)
        {
            return arrayDataType.ToDdl(Name, Nullable);
        }
        else
        {
            return DataType.ToDdl(Name, Nullable);
        }
    }

    public IDictionary<string, object> Metadata;

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
        ArrowTypeId.Decimal128 => DecimalType(((Decimal128Type)type).Precision, ((Decimal128Type)type).Scale),
        ArrowTypeId.Decimal256 => DecimalType(((Decimal256Type)type).Precision, ((Decimal256Type)type).Scale),
        ArrowTypeId.String => StringType(),
        ArrowTypeId.Binary => BinaryType(),
        ArrowTypeId.Date32 => DateType(),
        ArrowTypeId.Date64 => DateType(),        
        ArrowTypeId.Timestamp => TimestampType(),
        ArrowTypeId.Struct => StructType(),
        ArrowTypeId.List => ArrayType(FromArrowType((type as ListType).ValueDataType), (type as ListType).ValueField.IsNullable),
        ArrowTypeId.Map => MapType(FromArrowType((type as Apache.Arrow.Types.MapType).KeyField.DataType), FromArrowType((type as Apache.Arrow.Types.MapType).ValueField.DataType), (type as Apache.Arrow.Types.MapType).ValueField.IsNullable),
        ArrowTypeId.Interval => IntervalToType(type as IntervalType),
        
        _ => throw new ArgumentOutOfRangeException($"Cannot convert Arrow Type '{type}'")
    };

    private SparkDataType IntervalToType(IntervalType type)
    {
        if (type.Unit == IntervalUnit.YearMonth)
        {
            return new YearMonthIntervalType();
        }

        throw new NotImplementedException($"Unknown Interval Unit: {type.Unit}");
    }

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
            return new ArrayType(elementType, type.Array.ContainsNull);
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
            return new MapType(FromConnectDataType(type.Map.KeyType), FromConnectDataType(type.Map.ValueType),
                type.Map.ValueContainsNull);
        }

        if (type.Date != null)
        {
            return new DateType();
        }

        if (type.Timestamp != null)
        {
            return new TimestampType();
        }

        if (type.Variant != null)
        {
            return new VariantType();
        }

        return new VoidType();
    }
}

public static class DataTypeJsonSerializer
{
    private const string TextStart = @"{""fields"":[";
    private const string TextEnd = @"],""type"":""struct""}";

    private const string MetadataStart = @"{""metadata"":{";
    private const string MetadataEnd = @"},";
    private const string MetadataItemFormat = @"""{0}"":{1}";

    private const string NameStart = @"""name"":""";
    private const string NameEnd = @""",";

    private const string NullableStart = @"""nullable"":";
    private const string NullableEnd = @",";

    private const string ContainsNullStart = @"""containsNull"":";
    private const string ContainsNullEnd = ",";

    private const string ElementTypeStart = @"""elementType"":""";
    private const string ElementTypeEnd = @""",";

    private const string ArrayType = @"""type"":""array""";

    private const string TypeStart = @"""type"":""";
    private const string TypeEnd = @"""";

    private const string TypeStructStart = @"""type"":";
    private const string TypeStructEnd = @"";

    private const string FieldEnd = "}";

    /// <summary>
    ///     Converts a struct type which is a list of fields and some optional metadata into a string
    /// </summary>
    /// <param name="structType"></param>
    /// <returns></returns>
    public static string StructTypeToJson(StructType structType)
    {
        var json = new StringBuilder();
        json.Append(TextStart);
        var first = true;

        foreach (var field in structType.Fields)
        {
            if (!first)
            {
                json.Append(',');
            }

            first = false;

            if (field.DataType is ArrayType array)
            {
                json.Append(ArrayTypeToJson(field, array));
                continue;
            }

            if (field.DataType is MapType)
            {
                continue;
            }

            if (field.DataType is StructType structTypeField)
            {
                json.Append(StructTypeToJson(field, structTypeField));
                continue;
            }

            json.Append(SimpleTypeToJson(field));
        }

        json.Append(TextEnd);

        return json.ToString();
    }

    private static string StructTypeToJson(StructField field, StructType structType)
    {
        var json = new StringBuilder();
        json.Append(MetadataToString(field));
        json.Append(NameToString(field));
        json.Append(NullableToString(field));
        json.Append($"{TypeStructStart}{StructTypeToJson(structType)}{TypeStructEnd}");
        json.Append(FieldEnd);
        return json.ToString();
    }

    private static string SimpleTypeToJson(StructField field)
    {
        var json = new StringBuilder();
        json.Append(MetadataToString(field));
        json.Append(NameToString(field));
        json.Append(NullableToString(field));
        json.Append(SimpleTypeToString(field));
        json.Append(FieldEnd);
        return json.ToString();
    }

    private static string ArrayTypeToJson(StructField field, ArrayType array)
    {
        var json = new StringBuilder();
        json.Append(MetadataToString(field));
        json.Append(NameToString(field));
        json.Append(NullableToString(field));

        json.Append($"{TypeStructStart}{{{ArrayTypeToJson(array)}{TypeStructEnd}}}");
        json.Append(FieldEnd);
        return json.ToString();
    }

    private static string ArrayTypeToJson(ArrayType array)
    {
        if (array.ElementType is ArrayType)
        {
            return string.Empty;
        }

        if (array.ElementType is MapType)
        {
            return string.Empty;
        }

        if (array.ElementType is StructType)
        {
            return string.Empty;
        }

        var json = new StringBuilder();
        json.Append(ContainsNullStart);
        json.Append(array.NullableValues.ToString().ToLowerInvariant());
        json.Append(ContainsNullEnd);
        json.Append(ElementTypeStart);
        json.Append(array.ElementType.JsonTypeName());
        json.Append(ElementTypeEnd);
        json.Append(ArrayType);

        return json.ToString();
    }

    /// <summary>
    ///     is the value bare or wrapped in quotes
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    private static string ValueToFormattedString(object value)
    {
        if (value is string or DateTime)
        {
            return $@"""{value}""";
        }
        
        return ReferenceEquals(null, value) ? "None" : value.ToString();
    }

    private static string MetadataToString(StructField field)
    {
        var metadata = new StringBuilder();
        metadata.Append(MetadataStart);

        if (field.Metadata != null && field.Metadata.Count > 0)
        {
            var items = string.Join(",",
                field.Metadata.Select(p => string.Format(MetadataItemFormat, p.Key, ValueToFormattedString(p.Value))));
            metadata.Append(items);
        }

        metadata.Append(MetadataEnd);
        return metadata.ToString();
    }

    private static string NameToString(StructField field)
    {
        return $@"{NameStart}{field.Name}{NameEnd}";
    }

    private static string SimpleTypeToString(StructField field)
    {
        return $@"{TypeStart}{field.DataType.JsonTypeName()}{TypeEnd}";
    }

    private static string NullableToString(StructField field)
    {
        return $@"{NullableStart}{field.Nullable.ToString().ToLowerInvariant()}{NullableEnd}";
    }
}