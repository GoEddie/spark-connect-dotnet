using Apache.Arrow;
using Apache.Arrow.Types;
using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;

namespace Spark.Connect.Dotnet.Tests.Arrow;

public class ApacheArrowToRowsTests : E2ETestBase
{
    public ApacheArrowToRowsTests(ITestOutputHelper logger) : base(logger)
    {
    }

    [Fact]
    public void CanConvertInt32Arrow_ToInt()
    {
        var numbers = new[] { 1, 2, 3, 4, 5, 6, 7 };

        var intBuilder = new Int32Array.Builder();
        intBuilder.AppendRange(numbers); // Adding all elements at once
        var intArray = intBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("Integers").DataType(Int32Type.Default).Nullable(false))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { intArray }, intArray.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(numbers.Length, rows.Count());
        for (var i = 0; i < numbers.Length; i++)
        {
            Assert.Equal(numbers[i], rows[i][0]);
        }
    }

    [Fact]
    public async Task CanConvertInt32ArrowWithNulls_ToInt()
    {
        var numbers = new[] { 1, 2, 3, 4, 5 };

        var intBuilder = new Int32Array.Builder();
        intBuilder.AppendRange(numbers); // Adding all elements at once
        intBuilder.AppendNull();
        intBuilder.Append(999);
        var intArray = intBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("Integers").DataType(Int32Type.Default).Nullable(true))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { intArray }, intArray.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(numbers.Length + 2, rows.Count());
        for (var i = 0; i < numbers.Length; i++)
        {
            Assert.Equal(numbers[i], rows[i][0]);
        }

        Assert.Null(rows[5][0]);
        Assert.Equal(999, rows[6][0]);
    }

    [Fact]
    public async Task CanConvertStringArrow_ToStrings()
    {
        var words = new[] { "hello", "old", "friend" };

        var stringBuilder = new StringArray.Builder();
        stringBuilder.AppendRange(words); // Adding all elements at once
        var stringArray = stringBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("String").DataType(StringType.Default).Nullable(false))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { stringArray }, stringArray.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(words.Length, rows.Count());
        for (var i = 0; i < words.Length; i++)
        {
            Assert.Equal(words[i], rows[i][0]);
        }
    }

    [Fact]
    public async Task CanConvertStringArrowWithNulls_ToStrings()
    {
        var words = new[] { "hello", "old", "friend" };

        var stringBuilder = new StringArray.Builder();
        stringBuilder.AppendRange(words);
        stringBuilder.AppendNull();
        stringBuilder.AppendNull();
        stringBuilder.Append("Not null");
        var stringArray = stringBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("String").DataType(StringType.Default).Nullable(false))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { stringArray }, stringArray.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(words.Length + 3, rows.Count());
        for (var i = 0; i < words.Length; i++)
        {
            Assert.Equal(words[i], rows[i][0]);
        }

        Assert.Null(rows[3][0]);
        Assert.Null(rows[4][0]);
        Assert.Equal(rows[5][0], "Not null");
    }


    [Fact]
    public async Task CanConvertMultipleArrowColumnsWithNulls_ToInt_Strings()
    {
        var words = new[] { "hello", "old", "friend" };

        var stringBuilder = new StringArray.Builder();
        stringBuilder.AppendRange(words);
        stringBuilder.AppendNull();
        stringBuilder.AppendNull();
        stringBuilder.Append("Not null");
        var stringArray = stringBuilder.Build();

        var numbers = new[] { 1, 2, 3 };

        var intBuilder = new Int32Array.Builder();
        intBuilder.AppendRange(numbers);
        intBuilder.AppendNull();
        intBuilder.AppendNull();
        intBuilder.Append(999);
        var intArray = intBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("Integers").DataType(Int32Type.Default).Nullable(false))
            .Field(f => f.Name("Strings").DataType(StringType.Default).Nullable(false))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { intArray, stringArray }, stringArray.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(words.Length + 3, rows.Count());
        for (var i = 0; i < words.Length; i++)
        {
            Assert.Equal(words[i], rows[i][1]);
        }

        Assert.Null(rows[3][1]);
        Assert.Null(rows[4][1]);
        Assert.Equal(rows[5][1], "Not null");

        for (var i = 0; i < words.Length; i++)
        {
            Assert.Equal(numbers[i], rows[i][0]);
        }

        Assert.Null(rows[3][0]);
        Assert.Null(rows[4][0]);
        Assert.Equal(999, rows[5][0]);
    }

    [Fact]
    public async Task CanConvertTimestampArrow_DateTimeOffset()
    {
        var dates = new DateTimeOffset[]
        {
            DateTime.Parse("2021-01-01 01:01:01"), DateTime.Parse("2022-02-02 02:02:02"), DateTime.Parse("2023-03-03 03:03:03")
        };

        var maxArrowTimestamp = DateTimeOffset.Parse("9999-12-31T23:59:59.9990000+00:00");
        var timestampBuilder = new TimestampArray.Builder();
        timestampBuilder.AppendRange(dates);
        timestampBuilder.AppendNull();
        timestampBuilder.Append(maxArrowTimestamp);

        var timestampArray = timestampBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("Integers").DataType(TimestampType.Default).Nullable(true))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { timestampArray }, timestampArray.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(dates.Length + 2, rows.Count());
        for (var i = 0; i < dates.Length; i++)
        {
            Assert.Equal(dates[i], rows[i][0]);
        }

        Assert.Null(rows[3][0]);
        Assert.Equal(maxArrowTimestamp, rows[4][0]);
    }

    [Fact]
    public async Task CanConvertDate32Arrow_DateTime()
    {
        var dates = new[]
        {
            DateTime.Parse("2021-01-01 00:00:00"), DateTime.Parse("2022-02-02 00:00:00"), DateTime.Parse("2023-03-03 00:00:00")
        };

        var maxArrowDateTime = DateTime.MaxValue.Date;
        var dateBuilder = new Date32Array.Builder();
        dateBuilder.AppendRange(dates);
        dateBuilder.AppendNull();
        dateBuilder.Append(maxArrowDateTime);

        var date32Array = dateBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("Dates").DataType(Date32Type.Default).Nullable(true))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { date32Array }, date32Array.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(dates.Length + 2, rows.Count());
        for (var i = 0; i < dates.Length; i++)
        {
            Assert.Equal(dates[i], rows[i][0]);
        }

        Assert.Null(rows[3][0]);
        Assert.Equal(maxArrowDateTime, rows[4][0]);
    }

    [Fact]
    public async Task CanConvertDate64Arrow_DateTime()
    {
        var dates = new[]
        {
            DateTime.Parse("2021-01-01 00:00:00"), DateTime.Parse("2022-02-02 00:00:00"), DateTime.Parse("2023-03-03 00:00:00")
        };

        var maxArrowDateTime = DateTime.MaxValue.Date;
        var dateBuilder = new Date64Array.Builder();
        dateBuilder.AppendRange(dates);
        dateBuilder.AppendNull();
        dateBuilder.Append(maxArrowDateTime);

        var date64Array = dateBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("Dates").DataType(Date64Type.Default).Nullable(true))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { date64Array }, date64Array.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(dates.Length + 2, rows.Count());
        for (var i = 0; i < dates.Length; i++)
        {
            Assert.Equal(dates[i], rows[i][0]);
        }

        Assert.Null(rows[3][0]);
        Assert.Equal(maxArrowDateTime, rows[4][0]);
    }


    [Fact]
    public async Task CanConvertMapArrow_Dictionary()
    {
        var type = new MapType(new Field("the_key", StringType.Default, false),
            new Field("the_value", StringType.Default, false));

        var mapBuilder = new MapArray.Builder(type);

        mapBuilder.Append();

        var keyBuilder = mapBuilder.KeyBuilder as StringArray.Builder;
        keyBuilder.Append("a");
        keyBuilder.Append("b");
        keyBuilder.Append("c");

        var valueBuilder = mapBuilder.ValueBuilder as StringArray.Builder;
        valueBuilder.Append("AAA");
        valueBuilder.Append("BBB");
        valueBuilder.Append("CCC");

        mapBuilder.Append();

        keyBuilder = mapBuilder.KeyBuilder as StringArray.Builder;
        keyBuilder.Append("z1");
        keyBuilder.Append("z2");
        keyBuilder.Append("z3");

        valueBuilder = mapBuilder.ValueBuilder as StringArray.Builder;
        valueBuilder.Append("ZZZ111");
        valueBuilder.Append("ZZZ222");
        valueBuilder.Append("ZZZ333");

        var mapArray = mapBuilder.Build();

        var schema = new Schema.Builder()
            .Field(f => f.Name("mapped").DataType(type).Nullable(true))
            .Build();

        var recordBatch = new RecordBatch(schema, new IArrowArray[] { mapArray }, mapArray.Length);

        var wrapper = new ArrowWrapper();
        var rows = wrapper.ArrowBatchesToRows(new List<RecordBatch> { recordBatch }, schema);
        Assert.Equal(2, rows.Count());
    }
}