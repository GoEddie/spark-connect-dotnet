using Apache.Arrow.Types;
using Google.Protobuf.Collections;

namespace Spark.Connect.Dotnet.Sql.Types;

public class StructType : SparkDataType
{
    public List<StructField> Fields { get; }

    public StructType(Spark.Connect.DataType.Types.Struct source) : base("StructType")
    {
        Fields = new List<StructField>();
        foreach (var field in source.Fields)
        {
            Fields.Add(new StructField(field.Name, field.DataType, field.Nullable));
        }
    }
    
    public StructType(RepeatedField<Spark.Connect.DataType.Types.StructField> source) : base("StructType")
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

    public StructType Add(string name, SparkDataType type, bool nullable)
    {
        Fields.Add(new StructField(name, type, nullable));
        return this;
    }

    public List<string> FieldNames() => Fields.Select(p => p.Name).ToList();

    public override string SimpleString()
    {
        string GetNameAndType(StructField field) => $"{field.Name}:{field.DataType.SimpleString()}";
        return $"StructType<{string.Join(",", Fields.Select(GetNameAndType))}>";
    }
    
    public override DataType ToDataType()
    {
        var fields = Fields.Select(field => new DataType.Types.StructField()
        {
            DataType = field.DataType.ToDataType(), 
            Name = field.Name, 
            Nullable = field.Nullable
        });
        
        return new DataType()
        {
            Struct = new DataType.Types.Struct()
            {
                Fields = { fields }
            }
        };
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
            return new IntType();
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
        
        return null;
    }
    public StructField(string name, DataType type, bool nullable)
    {
        Name = name;
        Nullable = nullable;
        DataType = FromConnectDataType(type);
    }
    
    public string Name { get; set; }
    
    
    public SparkDataType DataType { get; set; }
    public bool Nullable { get; set; }
}