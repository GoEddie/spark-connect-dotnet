using Apache.Arrow;
using Apache.Arrow.Types;
using System.Text;

namespace Spark.Connect.Dotnet.Sql.Types;

public class ArrayType : SparkDataType
{
    public readonly SparkDataType ElementType;
    public readonly bool NullableValues;

    public ArrayType(SparkDataType elementType, bool nullableValues) : base($"Array<{elementType.TypeName}>")
    {
        ElementType = elementType;
        NullableValues = nullableValues;
    }

    public override DataType ToDataType()
    {
        return new DataType
        {
            Array = new DataType.Types.Array
            {
                ElementType = ElementType.ToDataType()
            }
        };
    }

    public override IArrowType ToArrowType()
    {
        var elementType = ElementType.ToArrowType();
        var childField = new Field("element", elementType, NullableValues);
        return new ListType(childField);
    }

    public override string SimpleString()
    {
        return $"array<{ElementType.SimpleString()}>";
    }

    public override string ToDdl(string name, bool nullable)
    {
        var ddl = new StringBuilder();
        ddl.Append(name);
        ddl.Append(" ");
        ddl.Append("ARRAY<");
        if (ElementType is StructType structTypeElement)
        {
            ddl.Append(structTypeElement.ToDdl());
        }
        else
        {
            ddl.Append(ElementType.TypeName);
        }
        ddl.Append(">");
        if (!nullable)
        {
            ddl.Append(" NOT NULL");
        }
        return ddl.ToString();
    }

}