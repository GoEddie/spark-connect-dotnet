using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Sql;

public class Row
{
    public readonly List<object> Data;

    public Row(StructType schema, params object[] data)
    {
        Schema = schema;
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

        Schema = new StructType(fields.ToArray());
    }

    public StructType Schema { get; }

    public object this[int i] => Data[i];

    public object[] Values => Data.ToArray();

    public static Row FromMsDataFrameRow(StructType schema, IEnumerable<KeyValuePair<string, object>> objects)
    {
        var data = new List<object>();
        // format is a dictionary of values, iterate and pull out the values.
        foreach (var pair in objects)
        {
            data.Add(pair.Value);
        }

        return new Row(schema, data);
    }

    public override string ToString()
    {
        var data = "";
        for (var i = 0; i < Schema.Fields.Count; i++)
        {
            data += $"{Schema.Fields[0].Name}={Data[i]}, ";
        }

        return $"Row({data})";
    }

    public int Size()
    {
        return Data.Count;
    }

    public object Get(int index)
    {
        if (index >= Size())
        {
            throw new IndexOutOfRangeException($"index ({index}) >= column counts ({Size()})");
        }

        if (index < 0)
        {
            throw new IndexOutOfRangeException($"index ({index}) < 0)");
        }

        return Data[index];
    }

    public object Get(string columnName)
    {
        var index = 0;
        foreach (var field in Schema.Fields)
        {
            if (field.Name == columnName)
            {
                return Data[index];
            }

            index++;
        }

        throw new IndexOutOfRangeException(
            $"Field '{columnName}' was not found in the schema: '{Schema.SimpleString()}'");
    }
}