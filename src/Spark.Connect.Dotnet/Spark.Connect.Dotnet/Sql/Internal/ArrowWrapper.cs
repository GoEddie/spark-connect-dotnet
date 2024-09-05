using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Sql.Types;
using StructType = Spark.Connect.Dotnet.Sql.Types.StructType;
using TimestampType = Apache.Arrow.Types.TimestampType;

namespace Spark.Connect.Dotnet.Sql;

public class ArrowWrapper
{
    /// <summary>
    ///     When we receive an Apache Arrow Batch the batch is a set of pointers to a stream of data, we take those pointers
    ///     and the stream of data
    ///     and convert them into .NET objects. If you do Collect() on large data it will effect the performance. You should
    ///     probably avoid doing large collects
    ///     and instead use this to read and write data via Spark.
    /// </summary>
    /// <param name="batches"></param>
    /// <param name="connectSchema"></param>
    /// <returns></returns>
    public async Task<IList<Row>> ArrowBatchesToRows(List<ExecutePlanResponse.Types.ArrowBatch> batches, DataType connectSchema)
    {
        var arrowTable = await ArrowBatchesToTable(batches, connectSchema);
        return ArrowTableToRows(arrowTable);
    }
    
    public async Task<IList<Row>> ArrowBatchToRows(ExecutePlanResponse.Types.ArrowBatch batch, DataType connectSchema)
    {
        var arrowTable = await ArrowBatchToTable(batch, connectSchema);
        return ArrowTableToRows(arrowTable);
    }

    public IList<Row> ArrowBatchesToRows(List<RecordBatch> batches, Schema arrowSchema)
    {
        var arrowTable = Table.TableFromRecordBatches(arrowSchema, batches);
        return ArrowTableToRows(arrowTable);
    }

    private IList<Row> ArrowTableToRows(Table arrowTable)
    {
        // This is a pre-created list of empty arrays that will need to be filled
        var rows = CreateRowsOfArrays(arrowTable.ColumnCount, arrowTable.RowCount);
        for (var columnIndex = 0; columnIndex < arrowTable.ColumnCount; columnIndex++)
        {
            var arrowArrays = GetChunksForColumnIndex(columnIndex, arrowTable);

            var readCount = 0;
            foreach (var arrowArray in arrowArrays)
            {
                var visitor = new ArrowVisitor(rows, columnIndex, readCount);
                arrowArray.Accept(visitor);
                readCount += arrowArray.Length;
            }
        }

        return rows.Select(p => new Row(new StructType(arrowTable.Schema), p)).ToList();
    }

    private List<IArrowArray> GetChunksForColumnIndex(int index, Table table)
    {
        var column = table.Column(index);
        var arrays = new List<IArrowArray>(column.Data.ArrayCount);
        for (var i = 0; i < column.Data.ArrayCount; i++)
        {
            arrays.Add(column.Data.ArrowArray(i));
        }

        return arrays;
    }

    private List<object[]> CreateRowsOfArrays(int columnCount, long rowCount)
    {
        if (rowCount > int.MaxValue)
        {
            Console.WriteLine(
                $"Maximum row count we will convert to .net objects is Int32.MaxValue, you have {rowCount} rows, truncating to {int.MaxValue}");
            rowCount = int.MaxValue;
        }

        var rows = new List<object[]>();
        for (var i = 0; i < rowCount; i++)
        {
            rows.Add(new object[columnCount]);
        }

        return rows;
    }

    public async Task<Table> ArrowBatchesToTable(List<ExecutePlanResponse.Types.ArrowBatch> arrowBatches, DataType connectSchema)
    {
        var recordBatches = new List<RecordBatch>();
        foreach (var batch in arrowBatches)
        {
            var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(batch.Data.ToByteArray()));
            var recordBatch = await reader.ReadNextRecordBatchAsync();

            recordBatches.Add(recordBatch);
        }

        var arrowSchema = ToArrowSchema(connectSchema);

        var readSchema = ReadArrowSchema(recordBatches, arrowSchema);

        return Table.TableFromRecordBatches(readSchema, recordBatches);
    }
    
    public async Task<Table> ArrowBatchToTable(ExecutePlanResponse.Types.ArrowBatch batch, DataType connectSchema)
    {
        var recordBatches = new List<RecordBatch>();
        var reader = new ArrowStreamReader(new ReadOnlyMemory<byte>(batch.Data.ToByteArray()));
        var recordBatch = await reader.ReadNextRecordBatchAsync();

        recordBatches.Add(recordBatch);
        
        var arrowSchema = ToArrowSchema(connectSchema);
        var readSchema = ReadArrowSchema(recordBatches, arrowSchema);

        return Table.TableFromRecordBatches(readSchema, recordBatches);
    }

    /// <summary>
    ///     This is a bit odd, there are some datatypes like timestamp that include the timezone in the arrow info but not the
    ///     spark schema
    ///     iterate through the spark schema and pull out the actualy datatypes from the arrow data so we are aligned.
    /// </summary>
    /// <param name="recordBatches"></param>
    /// <param name="arrowSchema"></param>
    /// <returns></returns>
    private Schema ReadArrowSchema(List<RecordBatch> recordBatches, Schema arrowSchema)
    {
        var fields = new List<Field>();
        var columnCount = arrowSchema.FieldsList.Count();
        var batch = recordBatches.FirstOrDefault();

        for (var i = 0; i < columnCount; i++)
        {
            var column = batch.Column(i);
            var existingField = arrowSchema.FieldsList[i];
            fields.Add(new Field(existingField.Name, column.Data.DataType, existingField.IsNullable,
                existingField.Metadata));
        }

        return new Schema(fields, arrowSchema.Metadata);
    }

    public Schema ToArrowSchema(DataType? schema)
    {
        var fields = schema.Struct.Fields.Select(p =>
            new Field(p.Name, SparkDataType.FromSparkConnectType(p.DataType).ToArrowType(), p.Nullable));
        var arrowSchema = new Schema(fields, new List<KeyValuePair<string, string>>());
        return arrowSchema;
    }
}

public class ArrowVisitor :
    IArrowArrayVisitor<StringArray>,
    IArrowArrayVisitor<BooleanArray>,
    IArrowArrayVisitor<Int8Array>,
    IArrowArrayVisitor<Int16Array>,
    IArrowArrayVisitor<Int32Array>,
    IArrowArrayVisitor<Int64Array>,
    IArrowArrayVisitor<UInt8Array>,
    IArrowArrayVisitor<UInt16Array>,
    IArrowArrayVisitor<UInt32Array>,
    IArrowArrayVisitor<UInt64Array>,
    IArrowArrayVisitor<DoubleArray>,
    IArrowArrayVisitor<FloatArray>,
    IArrowArrayVisitor<TimestampArray>,
    IArrowArrayVisitor<Date32Array>,
    IArrowArrayVisitor<Date64Array>,
    IArrowArrayVisitor<BinaryArray>,
    IArrowArrayVisitor<YearMonthIntervalArray>,
    IArrowArrayVisitor<ListArray>,
    IArrowArrayVisitor<MapArray>,
    IArrowArrayVisitor<StructArray>
{
    private readonly int _columnIndex;
    private readonly int _readCount;
    public readonly List<object[]> Rows;

    public ArrowVisitor(List<object[]> rows, int columnIndex, int readCount)
    {
        Rows = rows;
        _columnIndex = columnIndex;
        _readCount = readCount;
    }

    public void Visit(BinaryArray array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(BooleanArray array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }


    public void Visit(Date32Array array)
    {
        var epoch = new DateTime(1970, 1, 1);
        var rowNumber = 0;
        foreach (var item in array.Cast<int?>())
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item.HasValue ? epoch.AddDays(item.Value) : null;
        }
    }

    public void Visit(Date64Array array)
    {
        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var rowNumber = 0;
        foreach (var item in array.Cast<long?>())
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item.HasValue ? epoch.AddMilliseconds(item.Value) : null;
        }
    }

    public void Visit(DoubleArray array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(FloatArray array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(Int16Array array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(Int32Array array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(Int64Array array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(Int8Array array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(ListArray array)
    {
        var rowNumber = 0;
        for (var i = 0; i < array.Length; i++)
        {
            var slice = array.GetSlicedValues(i);
            var data = GetArrayData(slice);
            Rows[_readCount + rowNumber++][_columnIndex] = data;
        }
    }

    public void Visit(MapArray array)
    {
        var visitor = new MapVisitor(array.ValueOffsets.ToArray());
        array.KeyValues.Accept(visitor);
        var dicts = new List<Dictionary<string, object>>(visitor.Items);
        var rowNumber = 0;
        foreach (var dict in dicts)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = dict;
        }
    }

    public void Visit(StringArray array)
    {
        var rowNumber = 0;
        foreach (var item in array.Cast<string>())
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(IArrowArray array)
    {
        Console.WriteLine("array");
    }

    public void Visit(StructArray array)
    {
        var columns = new List<List<object>>();
        var rows = new List<List<object>>();

        foreach (var item in array.Fields)
        {
            var valueVisitor = new MapValueVisitior();
            item.Accept(valueVisitor);
            columns.Add(valueVisitor.Values);
            Console.WriteLine("what to do here?");
        }

        var rowNumber = 0;
        for (var i = 0; i < array.Length; i++)
        {
            var currentRow = new List<object>();
            var columnNumber = 0;
            foreach (var column in columns)
            {
                currentRow.Add(column[rowNumber + i]);
            }

            rows.Add(currentRow);
        }

        rowNumber = 0;
        foreach (var row in rows)
        {
            Rows[_readCount + rowNumber][_columnIndex] = rows[rowNumber++].ToArray();
        }
    }

    public void Visit(TimestampArray array)
    {
        var rowNumber = 0;
        foreach (var item in array.Cast<long?>())
        {
            if ((array.Data.DataType as TimestampType).Unit == TimeUnit.Millisecond)
            {
                Rows[_readCount + rowNumber++][_columnIndex] =
                    item.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(item.Value) : null;
            }

            if ((array.Data.DataType as TimestampType).Unit == TimeUnit.Microsecond)
            {
                Rows[_readCount + rowNumber++][_columnIndex] =
                    item.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(item.Value / 1000) : null;
            }

            if ((array.Data.DataType as TimestampType).Unit == TimeUnit.Nanosecond)
            {
                Rows[_readCount + rowNumber++][_columnIndex] =
                    item.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(item.Value / 1000000) : null;
            }

            if ((array.Data.DataType as TimestampType).Unit == TimeUnit.Second)
            {
                Rows[_readCount + rowNumber++][_columnIndex] =
                    item.HasValue ? DateTimeOffset.FromUnixTimeSeconds(item.Value) : null;
            }
        }
    }

    public void Visit(UInt16Array array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(UInt32Array array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(UInt64Array array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(UInt8Array array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    public void Visit(YearMonthIntervalArray array)
    {
        var rowNumber = 0;
        foreach (var item in array)
        {
            Rows[_readCount + rowNumber++][_columnIndex] = item;
        }
    }

    private static dynamic GetArrayData(IArrowArray array)
    {
        return array switch
        {
            Int32Array int32array => int32array.Values.ToArray(), Int16Array int16array => int16array.Values.ToArray()
            , StringArray stringArray => stringArray.Cast<string>().Select(p => p).ToArray(), FloatArray floatArray => floatArray.Values.ToArray()
            , Int64Array int64Array => int64Array.Values.ToArray(), DoubleArray doubleArray => doubleArray.Values.ToArray(), Time32Array time32Array => time32Array.Values.ToArray()
            , Time64Array time64Array => time64Array.Values.ToArray(), BooleanArray booleanArray => booleanArray.Values.ToArray()
            , Date32Array date32Array => date32Array.Values.ToArray(), Date64Array date64Array => date64Array.Values.ToArray(), Int8Array int8Array => int8Array.Values.ToArray()
            , UInt16Array uint6Array => uint6Array.Values.ToArray(), UInt8Array uInt8Array => uInt8Array.Values.ToArray(), UInt64Array uInt64Array => uInt64Array.Values.ToArray()
            , ListArray listArray => GetArrayData(listArray.Values), StructArray structArray => structArray.Fields.Select(p => GetArrayData(p)).ToList()
            , _ => throw new NotImplementedException()
        };
    }
}

public class ListArrayVisitor : IArrowArrayVisitor, IArrowArrayVisitor<StringArray>
{
    public List<object> Data = new();

    public void Visit(IArrowArray array)
    {
        Console.WriteLine("STOP HERE");
    }

    public void Visit(StringArray array)
    {
        foreach (var item in array.Cast<string>())
        {
            Data.Add(item);
        }
    }
}

public class MapVisitor :
    IArrowArrayVisitor<StructArray>
{
    private readonly int[] _valueOffsets;

    public List<Dictionary<string, object>> Items = new();

    public MapVisitor(int[] valueOffsets)
    {
        _valueOffsets = valueOffsets;
    }

    public void Visit(StructArray array)
    {
        var key = array.Fields.First();
        var value = array.Fields.Last();

        var keyVisitor = new MapKeyVisitior();
        var valueVisitor = new MapValueVisitior();

        key.Accept(keyVisitor);
        value.Accept(valueVisitor);
        for (var i = 0; i < _valueOffsets.Length; i++)
        {
            var offset = _valueOffsets[i];
            if (_valueOffsets.Length > i + 1)
            {
                var nextOffset = _valueOffsets[i + 1];
                var keys = keyVisitor.Keys.GetRange(offset, nextOffset - offset);
                var values = valueVisitor.Values.GetRange(offset, nextOffset - offset);
                var dict = new Dictionary<string, object>();
                for (var j = 0; j < nextOffset - offset; j++)
                {
                    dict[keys[j]] = values[j];
                }

                Items.Add(dict);
            }
        }
    }

    public void Visit(IArrowArray array)
    {
    }

    public void Visit(StringArray array)
    {
        Console.WriteLine("HERE");
    }
}

public class MapKeyVisitior :
    IArrowArrayVisitor<StringArray>
{
    public List<string> Keys { get; private set; }

    public void Visit(StringArray array)
    {
        Keys = new List<string>();
        foreach (var str in array.Cast<string>().ToList())
        {
            Keys.Add(str);
        }

        ;
    }

    public void Visit(IArrowArray array)
    {
    }
}

public class MapValueVisitior :
    IArrowArrayVisitor<StringArray>,
    IArrowArrayVisitor<Int8Array>,
    IArrowArrayVisitor<Int16Array>,
    IArrowArrayVisitor<Int32Array>,
    IArrowArrayVisitor<Int64Array>,
    IArrowArrayVisitor<UInt8Array>,
    IArrowArrayVisitor<UInt16Array>,
    IArrowArrayVisitor<UInt32Array>,
    IArrowArrayVisitor<UInt64Array>,
    IArrowArrayVisitor<BooleanArray>,
    IArrowArrayVisitor<BinaryArray>,
    IArrowArrayVisitor<TimestampArray>,
    IArrowArrayVisitor<Date32Array>,
    IArrowArrayVisitor<Date64Array>,
    IArrowArrayVisitor<MapArray>,
    IArrowArrayVisitor<ListArray>,
    IArrowArrayVisitor<StructArray>
{
    public MapValueVisitior()
    {
        Values = new List<object>();
    }

    public List<object> Values { get; }

    public void Visit(BinaryArray array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(BooleanArray array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(Date32Array array)
    {
        foreach (var item in array.Cast<int?>())
        {
            Values.Add(item.HasValue ? DateTimeOffset.FromUnixTimeSeconds(item.Value) : null);
        }
    }

    public void Visit(Date64Array array)
    {
        foreach (var item in array.Cast<long?>())
        {
            Values.Add(item.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(item.Value) : null);
        }
    }

    public void Visit(Int16Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(Int32Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(Int64Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(Int8Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(ListArray array)
    {
    }

    public void Visit(MapArray array)
    {
        throw new NotImplementedException("Cannot have a dict of values of dict, could be implemented");
    }

    public void Visit(StringArray array)
    {
        foreach (var str in array.Cast<string>().ToList())
        {
            Values.Add(str);
        }

        ;
    }

    public void Visit(IArrowArray array)
    {
        Console.WriteLine("New Row??");
    }

    public void Visit(StructArray array)
    {
        Console.WriteLine("HERE!!!!");
    }

    public void Visit(TimestampArray array)
    {
        foreach (var item in array.Cast<long?>())
        {
            Values.Add(item.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(item.Value / 1000) : null);
        }
    }

    public void Visit(UInt16Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(UInt32Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(UInt64Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(UInt8Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }
}

public class StructValueVisitior :
    IArrowArrayVisitor<StringArray>,
    IArrowArrayVisitor<Int8Array>,
    IArrowArrayVisitor<Int16Array>,
    IArrowArrayVisitor<Int32Array>,
    IArrowArrayVisitor<Int64Array>,
    IArrowArrayVisitor<UInt8Array>,
    IArrowArrayVisitor<UInt16Array>,
    IArrowArrayVisitor<UInt32Array>,
    IArrowArrayVisitor<UInt64Array>,
    IArrowArrayVisitor<BooleanArray>,
    IArrowArrayVisitor<BinaryArray>,
    IArrowArrayVisitor<TimestampArray>,
    IArrowArrayVisitor<Date32Array>,
    IArrowArrayVisitor<Date64Array>,
    IArrowArrayVisitor<MapArray>,
    IArrowArrayVisitor<ListArray>,
    IArrowArrayVisitor<StructArray>
{
    public StructValueVisitior()
    {
        Values = new List<object>();
    }

    public List<object> Values { get; }

    public void Visit(BinaryArray array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(BooleanArray array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(Date32Array array)
    {
        foreach (var item in array.Cast<int?>())
        {
            Values.Add(item.HasValue ? DateTimeOffset.FromUnixTimeSeconds(item.Value) : null);
        }
    }

    public void Visit(Date64Array array)
    {
        foreach (var item in array.Cast<long?>())
        {
            Values.Add(item.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(item.Value) : null);
        }
    }

    public void Visit(Int16Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(Int32Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(Int64Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(Int8Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(ListArray array)
    {
    }

    public void Visit(MapArray array)
    {
        throw new NotImplementedException("Cannot have a dict of values of dict, could be implemented");
    }

    public void Visit(StringArray array)
    {
        foreach (var str in array.Cast<string>().ToList())
        {
            Values.Add(str);
        }

        ;
    }

    public void Visit(IArrowArray array)
    {
        Console.WriteLine("New Row??");
    }

    public void Visit(StructArray array)
    {
        Console.WriteLine("HERE!!!!");
    }

    public void Visit(TimestampArray array)
    {
        foreach (var item in array.Cast<long?>())
        {
            Values.Add(item.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(item.Value / 1000) : null);
        }
    }

    public void Visit(UInt16Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(UInt32Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(UInt64Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }

    public void Visit(UInt8Array array)
    {
        foreach (var item in array)
        {
            Values.Add(item);
        }
    }
}