using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.ML.LinAlg;

public class SparseVector(int size, List<int> indices, List<double> values) : Vector, IUserDefinedType
{
    public List<double> Values { get; } = values;
    public List<int> Indices { get; } = indices;
    
    public int Size { get; } = size;
    
    public object[] GetDataForDataframe() => [(sbyte)0, Size, Indices, Values];
    public SparkDataType GetDataType() => new VectorUDT();
    
}