using Apache.Arrow;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using IntegerType = Spark.Connect.Dotnet.Sql.Types.IntegerType;
using StructType = Apache.Arrow.Types.StructType;

namespace Spark.Connect.Dotnet.ML.LinAlg;

public class DenseVector(List<double> values) : IUserDefinedType
{
    public List<double> Values { get; } = values;
    
// we return null so we can send null to spark, we handle it and only ever check that the
// value is null and if so the arrow builder does AppendNull,
// we don't ever try to actually use the null values.
#pragma warning disable CS8601, CS8625 
    public object[] GetDataForDataframe() => [(sbyte)1, null, null as List<int>, Values];
#pragma warning restore CS8601, CS8625
    public SparkDataType GetDataType() => new VectorUDT();
    
}

public class SparseVector(int size, List<int> indices, List<double> values) : IUserDefinedType
{
    public List<double> Values { get; } = values;
    public List<int> Indices { get; } = indices;
    
    public int Size { get; } = size;
    
    public object[] GetDataForDataframe() => [(sbyte)0, Size, Indices, Values];
    public SparkDataType GetDataType() => new VectorUDT();
    
}

public interface IHasCustomJson
{
    public string Json();
}

public abstract class UserDefinedType(string typeName) : SparkDataType(typeName), IHasCustomJson
{
    public abstract Spark.Connect.Dotnet.Sql.Types.StructType GetStructType();
}

public class VectorUDT : UserDefinedType
{
    public VectorUDT() : base("org.apache.spark.ml.linalg.VectorUDT")
    {
        _arrowBuilders = CreateArrowBuilders();
    }

    public override string Json()
    {
        return
            "{\"metadata\":{},\"name\":\"__NAME__\",\"nullable\":true,\"type\":{\"class\":\"org.apache.spark.ml.linalg.VectorUDT\",\"pyClass\":\"pyspark.ml.linalg.VectorUDT\",\"sqlType\":{\"fields\":[{\"metadata\":{},\"name\":\"type\",\"nullable\":false,\"type\":\"byte\"},{\"metadata\":{},\"name\":\"size\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"indices\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"integer\",\"type\":\"array\"}},{\"metadata\":{},\"name\":\"values\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"double\",\"type\":\"array\"}}],\"type\":\"struct\"},\"type\":\"udt\"}}";

    }

    public override Sql.Types.StructType GetStructType()
    {
        return new Sql.Types.StructType()
        {
            Fields =
            {
                new StructField() { Name = "type", DataType = new ByteType() }, 
                new StructField() { Name = "size", DataType = new IntegerType() }, 
                new StructField() { Name = "indices", DataType = new ArrayType(IntegerType(), false) },
                new StructField() { Name = "values", DataType = new ArrayType(DoubleType(), false) }

            }
        };
    }


    public override DataType ToDataType()
    {
        return new DataType()
        {
            Struct = new DataType.Types.Struct()
            {
                Fields =
                {
                    new List<DataType.Types.StructField>()
                    {
                        new() { DataType = new ByteType().ToDataType(), Name = "type", Nullable = false }, 
                        new() { DataType = new IntegerType().ToDataType(), Name = "size", Nullable = true }, 
                        new() { DataType = new ArrayType(SparkDataType.IntegerType(), false).ToDataType(), Name = "indices", Nullable = true},
                        new() { DataType = new ArrayType(SparkDataType.DoubleType(), false).ToDataType(), Name = "values" , Nullable = true}
                    }
                }
            }
        };
    }

    public override IArrowType ToArrowType()
    {   
        return new StructType(new List<Field>()
        {
            new ("type", ByteType().ToArrowType(), false),
            new ("size", IntegerType().ToArrowType(), true),
            new ("indices", ArrayType(SparkDataType.IntegerType(), false).ToArrowType(), true),
            new ("values", ArrayType(SparkDataType.DoubleType(), false).ToArrowType(), true)
        });
    }

    private readonly IEnumerable<IArrowArrayBuilder> _arrowBuilders;

    private  IEnumerable<IArrowArrayBuilder> CreateArrowBuilders()
    {
        var builders = new List<IArrowArrayBuilder>();
        
        var sourceTypes = ToArrowType();
        foreach (var arrowType in (sourceTypes as StructType)!.Fields)
        {
            var builder = ArrowHelpers.GetArrowBuilderForArrowType(arrowType.DataType);
            builders.Add(builder);
        }

        return builders;
    }
    

    public override IEnumerable<IArrowArrayBuilder> GetArrowArrayBuilders()
    {
        return _arrowBuilders;
    }
}

