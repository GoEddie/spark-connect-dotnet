using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Sql;

public class Row
{
    private readonly StructType _schema;
    public readonly List<object> Data;

    public Row(StructType schema, params object[] data)
    {
        _schema = schema;
        Data = data.ToList();
    }

    public Row(params Tuple<string, object>[] data)
    {
        var fields = new List<StructField>();
        Data = new List<object>();

        foreach (var tuple in data)
        {
            fields.Add(new StructField(tuple.Item1, SparkDataType.FromString(tuple.Item2.GetType().Name), true));
            Data.Add(tuple.Item2);
        }

        _schema = new StructType(fields.ToArray());
    }

    public override string ToString()
    {
        var data = "";
        for (var i = 0; i < _schema.Fields.Count; i++)
        {
            data += $"{_schema.Fields[0].Name}={Data[i]}, ";
        }

        return $"Row({data})";
    }

    public StructType Schema => _schema;

    public object this[int i] => Data[i];

} 