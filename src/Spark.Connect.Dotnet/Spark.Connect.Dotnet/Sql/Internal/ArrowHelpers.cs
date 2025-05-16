using Apache.Arrow;
using Apache.Arrow.Types;

namespace Spark.Connect.Dotnet.Sql;

public class ArrowHelpers
{
    public static IArrowArrayBuilder GetArrowBuilderForArrowType(IArrowType type) => type.TypeId switch
    {
        ArrowTypeId.Boolean => new BooleanArray.Builder(),
        ArrowTypeId.Int8 => new Int8Array.Builder(),
        ArrowTypeId.Int16 => new Int16Array.Builder(),
        ArrowTypeId.Int32 => new Int32Array.Builder(),
        ArrowTypeId.Int64 => new Int64Array.Builder(),
        ArrowTypeId.Float => new FloatArray.Builder(),
        ArrowTypeId.Double => new DoubleArray.Builder(),
        ArrowTypeId.Decimal128 => new Decimal128Array.Builder((Decimal128Type)type),
        ArrowTypeId.Decimal256 => new Decimal256Array.Builder((Decimal256Type)type),
        ArrowTypeId.String => new StringArray.Builder(),
        ArrowTypeId.Binary => new BinaryArray.Builder(),
        ArrowTypeId.Date32 => new Date32Array.Builder(),
        ArrowTypeId.Date64 => new Date64Array.Builder(),
        ArrowTypeId.Timestamp => new TimestampArray.Builder(),
        ArrowTypeId.Struct => throw new NotImplementedException("Structs don't need a builder, build the children and then new a struct array"),
        ArrowTypeId.List => new ListArray.Builder(((ListType)type).ValueField.DataType),
        ArrowTypeId.Map => throw new NotImplementedException("NEED map builder"),
        ArrowTypeId.Interval => new DayTimeIntervalArray.Builder(),
        
        _ => throw new ArgumentOutOfRangeException($"Cannot convert Arrow Type '{type}'")
    };
    
    public static void WriteToBuilder(IArrowArrayBuilder currentBuilder, object? data)
    {
        switch (currentBuilder)
        {
            case BooleanArray.Builder boolBuilder:
                
                if (data is null)
                {
                    boolBuilder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case bool b:
                            boolBuilder.Append(b);
                            break;
                        case bool[] bArray:
                            boolBuilder.AppendRange(bArray);
                            break;
                        case IList<bool> bList:
                            boolBuilder.AppendRange(bList);
                            break;
                    }
                }
                break;
            case Int8Array.Builder int8Builder:
                
                if (data is null)
                {
                    int8Builder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case sbyte s:
                            int8Builder.Append(s);
                            break;
                        case sbyte[] byteArray:
                            int8Builder.AppendRange(byteArray);
                            break;
                        case IList<sbyte> sbyteList:
                            int8Builder.AppendRange(sbyteList);
                            break;
                    }
                }

                break;
            case Int16Array.Builder int16Builder:
                if (data is null)
                {
                    int16Builder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case short s:
                            int16Builder.Append(s);
                            break;
                        case short[] sarray:
                            int16Builder.AppendRange(sarray);
                            break;
                        case IList<short> shortList:
                            int16Builder.AppendRange(shortList);
                            break;
                    }
                }

                break;

            case Int32Array.Builder intBuilder:
                if (data is null)
                {
                    intBuilder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case int[] a:
                            intBuilder.AppendRange(a);
                            break;
                        case IList<int> intList:
                            intBuilder.AppendRange(intList);
                            break;
                        case int i:
                            intBuilder.Append(i);
                            break;       
                    }
                }

                break;
            
            case Int64Array.Builder int64Builder:
                if (data is null)
                {
                    int64Builder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case long[] a:
                            int64Builder.AppendRange(a);
                            break;
                        case IList<long> intList:
                            int64Builder.AppendRange(intList);
                            break;
                        case long i:
                            int64Builder.Append(i);
                            break;       
                    }
                }

                break;
            
            case DoubleArray.Builder doubleBuilder:
               
                if (data is null)
                {
                    doubleBuilder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case double[] doubleArray:
                            doubleBuilder.AppendRange(doubleArray);
                            break;
                        case IList<double> doubleList:
                            doubleBuilder.AppendRange(doubleList);
                            break;
                        case double val:
                            doubleBuilder.Append(val);
                            break;
                    }
                }
                break;
            case FloatArray.Builder floatBuilder:
               
                if (data is null)
                {
                    floatBuilder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case float[] floatArray:
                            floatBuilder.AppendRange(floatArray);
                            break;
                        case IList<float> floatList:
                            floatBuilder.AppendRange(floatList);
                            break;
                        case float val:
                            floatBuilder.Append(val);
                            break;
                        default:
                            Console.WriteLine($"Unknown field type: {currentBuilder}");
                            break;
                    }
                }

                break;
            case StringArray.Builder stringBuilder:
                if (data is null)
                {
                    stringBuilder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case char c:
                            stringBuilder.Append(c.ToString());
                            break;
                        case char[] charArray:
                            stringBuilder.Append(string.Join("", charArray));
                            break;
                        case string[] stringArray:
                            stringBuilder.AppendRange(stringArray);
                            break;
                        case List<char> charList:
                            stringBuilder.Append(string.Join("", charList));
                            break;
                        case List<string> stringList:
                            stringBuilder.AppendRange(stringList);
                            break;
                        case string str:
                            stringBuilder.Append(str);
                            break;
                        case Guid guid:
                            stringBuilder.Append(guid.ToString());
                            break;
                    }
                }
                break;
            case Decimal128Array.Builder decimal128Builder:
                if (data is null)
                {
                    decimal128Builder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case decimal d:
                            decimal128Builder.Append(d);
                            break;
                        case decimal[] decimalArray:
                            decimal128Builder.AppendRange(decimalArray);
                            break;
                        case IList<decimal> decimalList:
                            decimal128Builder.AppendRange(decimalList);
                            break;
                    }
                }
                break;
            case Date32Array.Builder date32Builder:
                if (data is null)
                {
                    date32Builder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case DateTime dateTime:
                            date32Builder.Append(dateTime.Date);
                            break;
                        case DateTime[] dateArray:
                            date32Builder.AppendRange(dateArray.Select(p => p.Date));
                            break;
                        case IList<DateTime> dateList:
                            date32Builder.AppendRange(dateList.Select(p => p.Date));
                            break;
                    }
                }
                break;
            
            case TimestampArray.Builder timestampBuilder:
                if (data is null)
                {
                    timestampBuilder.AppendNull();
                }
                else
                {
                    switch (data)
                    {
                        case DateTime dateTime:
                            var timestamp = new DateTimeOffset(dateTime, TimeSpan.Zero);
                            timestampBuilder.Append(timestamp);
                            break;
                        case DateTimeOffset dateTimeOffset:
                            timestampBuilder.Append(dateTimeOffset);
                            break;
                        case DateTime[] dateTimeArray:
                            timestampBuilder.AppendRange(dateTimeArray.Select(p => new DateTimeOffset(p, TimeSpan.Zero)).ToList());
                            break;
                        case IList<DateTime> dateTimeList:
                            timestampBuilder.AppendRange(dateTimeList.Select(p => new DateTimeOffset(p, TimeSpan.Zero)).ToList());
                            break;
                        case DateTimeOffset[] dateTimeOffsetArray:
                            timestampBuilder.AppendRange(dateTimeOffsetArray);
                            break;
                        case List<DateTimeOffset> dateTimeOffsetList:
                            timestampBuilder.AppendRange(dateTimeOffsetList);
                            break;
                    }
                }

                break;
                
            default:
                 throw new NotImplementedException($"Unknown field type: {currentBuilder} - You will need to create your own ArrowBatch data to pass to CreateDataFrame until the type is supported");
        }
    }
}