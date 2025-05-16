using Apache.Arrow;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using BooleanType = Spark.Connect.Dotnet.Sql.Types.BooleanType;
using IntegerType = Spark.Connect.Dotnet.Sql.Types.IntegerType;
using StructType = Apache.Arrow.Types.StructType;

namespace Spark.Connect.Dotnet.ML.LinAlg;

public class MatrixUDT : UserDefinedType
{
    public MatrixUDT() : base("org.apache.spark.ml.linalg.MatrixUDT")
    {
        _arrowBuilders = CreateArrowBuilders();
        throw new NotImplementedException("MatrixUDT is not implemented yet.");
    }

    // public override string Json()
    // {
    //     // return
    //         // "{\"metadata\":{},\"name\":\"__NAME__\",\"nullable\":true,\"type\":{\"class\":\"org.apache.spark.ml.linalg.VectorUDT\",\"pyClass\":\"pyspark.ml.linalg.VectorUDT\",\"sqlType\":{\"fields\":[{\"metadata\":{},\"name\":\"type\",\"nullable\":false,\"type\":\"byte\"},{\"metadata\":{},\"name\":\"size\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"indices\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"integer\",\"type\":\"array\"}},{\"metadata\":{},\"name\":\"values\",\"nullable\":true,\"type\":{\"containsNull\":false,\"elementType\":\"double\",\"type\":\"array\"}}],\"type\":\"struct\"},\"type\":\"udt\"}}";
    // }

    public override Sql.Types.StructType GetStructType()
    {
        return new Sql.Types.StructType()
        {
            Fields =
            {
                new StructField() { Name = "type", DataType = new ByteType() }, 
                new StructField() { Name = "numRows", DataType = new IntegerType() },
                new StructField() { Name = "numCols", DataType = new IntegerType() },
                new StructField() { Name = "colPtrs", DataType = new ArrayType(IntegerType(), false), Nullable = true },
                new StructField() { Name = "rowIndices", DataType = new ArrayType(IntegerType(), false), Nullable = true },
                new StructField() { Name = "values", DataType = new ArrayType(DoubleType(), false), Nullable = true},
                new StructField() { Name = "isTransposed", DataType = new BooleanType() , Nullable = false}
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
                        new() { DataType = new IntegerType().ToDataType(), Name = "numRows", Nullable = false},
                        new() { DataType = new IntegerType().ToDataType(), Name = "numCols", Nullable = false },
                        new() { DataType = new ArrayType(SparkDataType.IntegerType(), false).ToDataType(), Name = "colPtrs", Nullable = true},
                        new() { DataType = new ArrayType(SparkDataType.IntegerType(), false).ToDataType(), Name = "rowIndices", Nullable = true},
                        new() { DataType = new ArrayType(SparkDataType.DoubleType(), false).ToDataType(), Name = "values" , Nullable = true},
                        new() { DataType = new BooleanType().ToDataType(), Name = "isTransposed", Nullable = false },
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
            new ("numRows", IntegerType().ToArrowType(), false),
            new ("numCols", IntegerType().ToArrowType(), false),
            new ("colPtrs", ArrayType(SparkDataType.IntegerType(), false).ToArrowType(), true),
            new ("rowIndices", ArrayType(SparkDataType.IntegerType(), false).ToArrowType(), true),
            new ("values", ArrayType(SparkDataType.DoubleType(), false).ToArrowType(), true),
            new ("isTransposed", BooleanType().ToArrowType(), false),
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

