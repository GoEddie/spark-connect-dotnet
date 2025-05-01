using Spark.Connect.Dotnet.Sql.Types;

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