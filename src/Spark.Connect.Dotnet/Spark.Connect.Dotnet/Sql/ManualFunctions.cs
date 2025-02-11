using Google.Protobuf;
using Spark.Connect.Dotnet.Grpc;
using StructType = Spark.Connect.Dotnet.Sql.Types.StructType;
using TernaryFunction = System.Linq.Expressions.Expression<System.Func<Spark.Connect.Dotnet.Sql.Column, Spark.Connect.Dotnet.Sql.Column,
    Spark.Connect.Dotnet.Sql.Column, Spark.Connect.Dotnet.Sql.Column>>;
using BinaryFunction = System.Linq.Expressions.Expression<System.Func<Spark.Connect.Dotnet.Sql.Column, Spark.Connect.Dotnet.Sql.Column,
    Spark.Connect.Dotnet.Sql.Column>>;
using UnaryFunction = System.Linq.Expressions.Expression<System.Func<Spark.Connect.Dotnet.Sql.Column, Spark.Connect.Dotnet.Sql.Column>>;


namespace Spark.Connect.Dotnet.Sql;

public partial class Functions : FunctionsWrapper
{
    private static readonly DateOnly UnixEpoch = new(1970, 1, 1);
    private static readonly TimeSpan UnixEpochTimespan = new(1970, 1, 1);
    
    public static Column Lit(Dictionary<string, float> dict)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Map = new Expression.Types.Literal.Types.Map
                {
                    Keys =
                    {
                        dict.Keys.Select(p => new Expression.Types.Literal
                        {
                            String = p
                        })
                    }
                    , Values =
                    {
                        dict.Values.Select(p => new Expression.Types.Literal
                        {
                            Float = p
                        })
                    }
                    , KeyType = new DataType
                    {
                        String = new DataType.Types.String()
                    }
                    , ValueType = new DataType
                    {
                        Integer = new DataType.Types.Integer()
                    }
                }
            }
        });
    }

    public static Column Lit(Dictionary<string, double> dict)
    {
        return new Column(
            new Expression
            {
                Literal = new Expression.Types.Literal
                {
                    Map = new Expression.Types.Literal.Types.Map
                    {
                        Keys =
                        {
                            dict.Keys.Select(p => new Expression.Types.Literal
                            {
                                String = p
                            })
                        }
                        , Values =
                        {
                            dict.Values.Select(p => new Expression.Types.Literal
                            {
                                Double = p
                            })
                        }
                        , KeyType = new DataType
                        {
                            String = new DataType.Types.String()
                        }
                        , ValueType = new DataType
                        {
                            Integer = new DataType.Types.Integer()
                        }
                    }
                }
            }
        );
    }

    public static Column Lit(Dictionary<string, string> dict)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Map = new Expression.Types.Literal.Types.Map
                {
                    Keys =
                    {
                        dict.Keys.Select(p => new Expression.Types.Literal
                        {
                            String = p
                        })
                    }
                    , Values =
                    {
                        dict.Values.Select(p => new Expression.Types.Literal
                        {
                            String = p
                        })
                    }
                    , KeyType = new DataType
                    {
                        String = new DataType.Types.String()
                    }
                    , ValueType = new DataType
                    {
                        Integer = new DataType.Types.Integer()
                    }
                }
            }
        });
    }

    public static Column Lit(Dictionary<string, int> dict)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Map = new Expression.Types.Literal.Types.Map
                {
                    Keys =
                    {
                        dict.Keys.Select(p => new Expression.Types.Literal
                        {
                            String = p
                        })
                    }
                    , Values =
                    {
                        dict.Values.Select(p => new Expression.Types.Literal
                        {
                            Integer = p
                        })
                    }
                    , KeyType = new DataType
                    {
                        String = new DataType.Types.String()
                    }
                    , ValueType = new DataType
                    {
                        Integer = new DataType.Types.Integer()
                    }
                }
            }
        });
    }

    public static Column Lit(string value)
    {
        if (value == null)
        {
            return new Column(new Expression
            {
                Literal = new Expression.Types.Literal
                {
                    Null = new DataType
                    {
                        String = new DataType.Types.String()
                    }
                }
            });
        }

        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                String = value
            }
        });
    }

    public static Column Lit(IDictionary<string, object> dict)
    {
        var values = new List<Expression>();
        foreach (var value in dict)
        {
            values.Add(Lit(value.Key).Expression);
            values.Add(Lit(value.Value).Expression);
        }


        var functionCall = new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "map", IsDistinct = false, IsUserDefinedFunction = false, Arguments = { values }
            }
        };

        return new Column(functionCall);
    }

    public static Column Lit(object o)
    {
        if (o is null)
        {
            return new Column(new Expression
            {
                Literal = new Expression.Types.Literal
                {
                    Null = new DataType()
                    {
                        Null = new DataType.Types.NULL()
                    }
                }
            });
        }

        if (o.GetType().IsArray)
        {
            var lits = new List<Column>();
            foreach (var object_ in o as Array)
            {
                lits.Add(Lit((object)object_));
            }

            return Array(lits.ToArray());
        }

        return o switch
        {
            int i => Lit(i), string s => Lit(s), double d => Lit(d), float f => Lit(f), short s => Lit(s), long l => Lit(l), _ => Lit(o.ToString()) //TODO not great
        };
    }

    public static Column Lit(DateOnly value)
    {
        var daysSinceUnixEpoch = value.DayNumber - UnixEpoch.DayNumber;

        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Date = daysSinceUnixEpoch
            }
        });
    }

    public static Column Lit(TimeSpan value)
    {
        var durationSinceEpoch = value - UnixEpochTimespan;
        var microseconds = durationSinceEpoch.Ticks / 10;

        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Timestamp = microseconds
            }
        });
    }

    public static Column Lit(DateTime value)
    {
        var durationSinceEpoch = value - UnixEpochTimespan;
        var microseconds = durationSinceEpoch.Ticks / 10;

        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Timestamp = microseconds
            }
        });
    }

    public static Column Lit(bool value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Boolean = value
            }
        });
    }

    public static Column Lit(DateTime? value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal()
            {
                Null = new DataType()
                {
                    Timestamp = new DataType.Types.Timestamp()
                }
            }
        });
    }
    public static Column Lit(float? value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal()
            {
                Null = new DataType()
                {
                    Float = new DataType.Types.Float()
                }
            }
        });
    }
    public static Column Lit(long? value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal()
            {
                Null = new DataType()
                {
                    Long = new DataType.Types.Long()
                }
            }
        });
    }
    public static Column Lit(double? value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal()
            {
                Null = new DataType()
                {
                    Double = new DataType.Types.Double()
                }
            }
        });
    }
    
    public static Column Lit(double value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Double = value
            }
        });
    }

    public static Column Lit(float value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Float = value
            }
        });
    }

    public static Column Lit(long value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Long = value
            }
        });
    }

    public static Column Lit(int[] values)
    {
        var elements = values.Select(value => new Expression
        {
            Literal = new Expression.Types.Literal { Integer = value }
        }).ToList();

        return new Column(new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "array", IsDistinct = false, Arguments =
                {
                    elements
                }
            }
        });
    }

    public static Column Lit(bool[] values)
    {
        var elements = values.Select(value => new Expression
        {
            Literal = new Expression.Types.Literal { Boolean = value }
        }).ToList();

        return new Column(new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "array", IsDistinct = false, Arguments =
                {
                    elements
                }
            }
        });
    }

    public static Column Lit(string[] values)
    {
        var elements = values.Select(value => new Expression
        {
            Literal = new Expression.Types.Literal { String = value }
        }).ToList();

        return new Column(new Expression
        {
            UnresolvedFunction = new Expression.Types.UnresolvedFunction
            {
                FunctionName = "array", IsDistinct = false, Arguments =
                {
                    elements
                }
            }
        });
    }

    public static Column Lit(int value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Integer = value
            }
        });
    }

    public static Column Lit(int? value)
    {
        if (!value.HasValue)
        {
            return new Column(new Expression
            {
                Literal = new Expression.Types.Literal
                {
                    Null = new DataType
                    {
                        Integer = new DataType.Types.Integer()
                    }
                }
            });
        }

        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Integer = value.Value
            }
        });
    }

    public static Column Lit(byte[] value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                Binary = ByteString.CopyFrom(value)
            }
        });
    }

    public static Column Col(string name)
    {
        return Column(name);
    }

    public static Column Column(string name)
    {
        return new Column(name);
    }

    /// <summary>
    /// If you want to pass `Lit(null)` then you can use this or do `Lit(null as type)`
    /// </summary>
    /// <returns>Lit</returns>
    public static Column LitNull() => Lit();

    /// <summary>
    /// If you want to pass `Lit(null)` then you can use this or do `Lit(null as type)`
    /// </summary>
    /// <returns>Lit</returns>
    public static Column Lit()
    {
        var expr = new Expression()
        {
           Literal = new Expression.Types.Literal()
           {
               Null = new DataType()
               {
                   Null = new DataType.Types.NULL()
               }
           }
        };
        
        return new Column(expr);
    }

    /// <param name="cols">List&lt;String&gt;</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static Column CountDistinct(List<string> cols)
    {
        return new Column(CreateExpression("count", true, cols.ToArray()));
    }

    /// <param name="cols">List&lt;Column&gt;</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static Column CountDistinct(List<Column> cols)
    {
        return new Column(CreateExpression("count", true, cols.ToArray()));
    }

    /// <param name="col">String</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static Column CountDistinct(string col)
    {
        return new Column(CreateExpression("count", true, col));
    }

    /// <param name="col">Column</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static Column CountDistinct(Column col)
    {
        return new Column(CreateExpression("count", true, col));
    }

    /// <summary>
    ///     Extracts a part of the date/timestamp or interval source.
    /// </summary>
    /// <param name="field">
    ///     Must be a string Lit, selects which part of the source should be extracted, and supported string
    ///     values are as same as the fields of the equivalent function extract.
    /// </param>
    /// <param name="source">Col, a date/timestamp or interval column from where field should be extracted.</param>
    /// <returns>Col</returns>
    public static Column DatePart(Expression field, Column source)
    {
        return new Column(CreateExpression("date_part", false, field, source));
    }

    /// <summary>
    ///     Extracts a part of the date/timestamp or interval source.
    /// </summary>
    /// <param name="field">
    ///     Must be a string Lit, selects which part of the source should be extracted, and supported string
    ///     values are as same as the fields of the equivalent function extract.
    /// </param>
    /// <param name="source">String column name, a date/timestamp or interval column from where field should be extracted.</param>
    /// <returns>Col</returns>
    public static Column DatePart(Expression field, string source)
    {
        return new Column(CreateExpression("date_part", false, field, Column(source)));
    }

    /// <summary>
    ///     Extracts a part of the date/timestamp or interval source.
    /// </summary>
    /// <param name="field">
    ///     Must be a string Lit, selects which part of the source should be extracted, and supported string
    ///     values are as same as the fields of the equivalent function extract.
    /// </param>
    /// <param name="source">Col, a date/timestamp or interval column from where field should be extracted.</param>
    /// <returns>Col</returns>
    public static Column DatePart(Column field, Column source)
    {
        return new Column(CreateExpression("date_part", false, field, source));
    }

    /// <summary>
    ///     Extracts a part of the date/timestamp or interval source.
    /// </summary>
    /// <param name="field">
    ///     Must be a string Lit, selects which part of the source should be extracted, and supported string
    ///     values are as same as the fields of the equivalent function extract.
    /// </param>
    /// <param name="source">String column name, a date/timestamp or interval column from where field should be extracted.</param>
    /// <returns>Col</returns>
    public static Column DatePart(Column field, string source)
    {
        return new Column(CreateExpression("date_part", false, field, Column(source)));
    }


    /// <summary>
    ///     Extracts a part of the date/timestamp or interval source.
    /// </summary>
    /// <param name="field">
    ///     Must be a string Lit, selects which part of the source should be extracted, and supported string
    ///     values are as same as the fields of the equivalent function extract.
    /// </param>
    /// <param name="source">Col, a date/timestamp or interval column from where field should be extracted.</param>
    /// <returns>Col</returns>
    public static Column Extract(Column field, Column source)
    {
        return new Column(CreateExpression("extract", false, field, source));
    }

    /// <summary>
    ///     Extracts a part of the date/timestamp or interval source.
    /// </summary>
    /// <param name="field">
    ///     Must be a string Lit, selects which part of the source should be extracted, and supported string
    ///     values are as same as the fields of the equivalent function extract.
    /// </param>
    /// <param name="source">String column name, a date/timestamp or interval column from where field should be extracted.</param>
    /// <returns>Col</returns>
    public static Column Extract(Column field, string source)
    {
        return new Column(CreateExpression("extract", false, field, Column(source)));
    }


    /// <Summary>
    ///     TryToNumber
    ///     Convert string 'col' to a number based on the string format `format`. Returns NULL if the string 'col' does not
    ///     match the expected format. The format follows the same semantics as the to_number function.
    /// </Summary>
    public static Column TryToNumber(string col, string format)
    {
        return new Column(CreateExpression("try_to_number", false, Col(col), Lit(format)));
    }

    /// <Summary>
    ///     TryToNumber
    ///     Convert string 'col' to a number based on the string format `format`. Returns NULL if the string 'col' does not
    ///     match the expected format. The format follows the same semantics as the to_number function.
    /// </Summary>
    public static Column TryToNumber(Column col, string format)
    {
        return new Column(CreateExpression("try_to_number", false, col, Lit(format)));
    }

    /// <Summary>
    ///     TryToNumber
    ///     Convert string 'col' to a number based on the string format `format`. Returns NULL if the string 'col' does not
    ///     match the expected format. The format follows the same semantics as the to_number function.
    /// </Summary>
    public static Column TryToNumber(Column col, Column format)
    {
        return new Column(CreateExpression("try_to_number", false, col, format));
    }

    /// <Summary>
    ///     TryElementAt
    ///     (array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will throw an error. If
    ///     index lt; 0, accesses elements from the last to the first. The function always returns NULL if the index exceeds
    ///     the length of the array.
    /// </Summary>
    public static Column TryElementAt(Column col, Column extraction)
    {
        return new Column(CreateExpression("try_element_at", false, col, extraction));
    }

    /// <Summary>
    ///     TryElementAt
    ///     (array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will throw an error. If
    ///     index lt; 0, accesses elements from the last to the first. The function always returns NULL if the index exceeds
    ///     the length of the array.
    /// </Summary>
    public static Column TryElementAt(string col, Column extraction)
    {
        return new Column(CreateExpression("try_element_at", false, Col(col), extraction));
    }


    /// <Summary>
    ///     ToVarchar
    ///     Convert `col` to a string based on the `format`. Throws an exception if the conversion fails. The format can
    ///     consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A
    ///     sequence of 0 or 9 in the format string matches a sequence of digits in the input value, generating a result string
    ///     of the same length as the corresponding sequence in the format string. The result string is left-padded with zeros
    ///     if the 0/9 sequence comprises more digits than the matching part of the decimal value, starts with 0, and is before
    ///     the decimal point. Otherwise, it is padded with spaces. '.' or 'D': Specifies the position of the decimal point
    ///     (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There
    ///     must be a 0 or 9 to the left and right of each grouping separator. '$': Specifies the location of the $ currency
    ///     sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign
    ///     (optional, only allowed once at the beginning or end of the format string). Note that 'S' prints '+' for positive
    ///     values but 'MI' prints a space. 'PR': Only allowed at the end of the format string; specifies that the result
    ///     string will be wrapped by angle brackets if the input value is negative.
    /// </Summary>
    public static Column ToVarchar(string col, string format)
    {
        return new Column(CreateExpression("to_varchar", false, Col(col), Lit(format)));
    }

    /// <Summary>
    ///     ToVarchar
    ///     Convert `col` to a string based on the `format`. Throws an exception if the conversion fails. The format can
    ///     consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A
    ///     sequence of 0 or 9 in the format string matches a sequence of digits in the input value, generating a result string
    ///     of the same length as the corresponding sequence in the format string. The result string is left-padded with zeros
    ///     if the 0/9 sequence comprises more digits than the matching part of the decimal value, starts with 0, and is before
    ///     the decimal point. Otherwise, it is padded with spaces. '.' or 'D': Specifies the position of the decimal point
    ///     (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There
    ///     must be a 0 or 9 to the left and right of each grouping separator. '$': Specifies the location of the $ currency
    ///     sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign
    ///     (optional, only allowed once at the beginning or end of the format string). Note that 'S' prints '+' for positive
    ///     values but 'MI' prints a space. 'PR': Only allowed at the end of the format string; specifies that the result
    ///     string will be wrapped by angle brackets if the input value is negative.
    /// </Summary>
    public static Column ToVarchar(Column col, string format)
    {
        return new Column(CreateExpression("to_varchar", false, col, Lit(format)));
    }

    /// <Summary>
    ///     ToVarchar
    ///     Convert `col` to a string based on the `format`. Throws an exception if the conversion fails. The format can
    ///     consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A
    ///     sequence of 0 or 9 in the format string matches a sequence of digits in the input value, generating a result string
    ///     of the same length as the corresponding sequence in the format string. The result string is left-padded with zeros
    ///     if the 0/9 sequence comprises more digits than the matching part of the decimal value, starts with 0, and is before
    ///     the decimal point. Otherwise, it is padded with spaces. '.' or 'D': Specifies the position of the decimal point
    ///     (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There
    ///     must be a 0 or 9 to the left and right of each grouping separator. '$': Specifies the location of the $ currency
    ///     sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign
    ///     (optional, only allowed once at the beginning or end of the format string). Note that 'S' prints '+' for positive
    ///     values but 'MI' prints a space. 'PR': Only allowed at the end of the format string; specifies that the result
    ///     string will be wrapped by angle brackets if the input value is negative.
    /// </Summary>
    public static Column ToVarchar(Column col, Column format)
    {
        return new Column(CreateExpression("to_varchar", false, col, format));
    }

    /// <Summary>
    ///     SplitPart
    ///     Splits `str` by delimiter and return requested part of the split (1-based). If any input is null, returns null. if
    ///     `partNum` is out of range of split parts, returns empty string. If `partNum` is 0, throws an error. If `partNum` is
    ///     negative, the parts are counted backward from the end of the string. If the `delimiter` is an empty string, the
    ///     `str` is not split.
    /// </Summary>
    public static Column SplitPart(string src, Column delimiter, Column partNum)
    {
        return new Column(CreateExpression("split_part", false, Col(src), delimiter, partNum));
    }

    /// <Summary>
    ///     SplitPart
    ///     Splits `str` by delimiter and return requested part of the split (1-based). If any input is null, returns null. if
    ///     `partNum` is out of range of split parts, returns empty string. If `partNum` is 0, throws an error. If `partNum` is
    ///     negative, the parts are counted backward from the end of the string. If the `delimiter` is an empty string, the
    ///     `str` is not split.
    /// </Summary>
    public static Column SplitPart(Column src, Column delimiter, Column partNum)
    {
        return new Column(CreateExpression("split_part", false, src, delimiter, partNum));
    }

    /// <Summary>
    ///     SplitPart
    ///     Splits `str` by delimiter and return requested part of the split (1-based). If any input is null, returns null. if
    ///     `partNum` is out of range of split parts, returns empty string. If `partNum` is 0, throws an error. If `partNum` is
    ///     negative, the parts are counted backward from the end of the string. If the `delimiter` is an empty string, the
    ///     `str` is not split.
    /// </Summary>
    public static Column SplitPart(string src, string delimiter, string partNum)
    {
        return new Column(CreateExpression("split_part", false, Col(src), Col(delimiter), Col(partNum)));
    }

    /// <Summary>
    ///     HistogramNumeric
    ///     Computes a histogram on numeric 'col' using nb bins. The return value is an array of (x,y) pairs representing the
    ///     centers of the histogram's bins. As the value of 'nb' is increased, the histogram approximation gets finer-grained,
    ///     but may yield artifacts around outliers. In practice, 20-40 histogram bins appear to work well, with more bins
    ///     being required for skewed or smaller datasets. Note that this function creates a histogram with non-uniform bin
    ///     widths. It offers no guarantees in terms of the mean-squared-error of the histogram, but in practice is comparable
    ///     to the histograms produced by the R/S-Plus statistical computing packages. Note: the output type of the 'x' field
    ///     in the return value is propagated from the input value consumed in the aggregate function.
    /// </Summary>
    public static Column HistogramNumeric(string col, Column nBins)
    {
        return new Column(CreateExpression("histogram_numeric", false, Col(col), nBins));
    }

    /// <Summary>
    ///     HistogramNumeric
    ///     Computes a histogram on numeric 'col' using nb bins. The return value is an array of (x,y) pairs representing the
    ///     centers of the histogram's bins. As the value of 'nb' is increased, the histogram approximation gets finer-grained,
    ///     but may yield artifacts around outliers. In practice, 20-40 histogram bins appear to work well, with more bins
    ///     being required for skewed or smaller datasets. Note that this function creates a histogram with non-uniform bin
    ///     widths. It offers no guarantees in terms of the mean-squared-error of the histogram, but in practice is comparable
    ///     to the histograms produced by the R/S-Plus statistical computing packages. Note: the output type of the 'x' field
    ///     in the return value is propagated from the input value consumed in the aggregate function.
    /// </Summary>
    public static Column HistogramNumeric(Column col, Column nBins)
    {
        return new Column(CreateExpression("histogram_numeric", false, col, nBins));
    }

    /// <Summary>
    ///     Sha2
    ///     Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512). The
    ///     numBits indicates the desired bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which
    ///     is equivalent to 256).
    /// </Summary>
    public static Column Sha2(string col, Column numBits)
    {
        return new Column(CreateExpression("sha2", false, col, numBits));
    }

    /// <Summary>
    ///     Sha2
    ///     Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512). The
    ///     numBits indicates the desired bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which
    ///     is equivalent to 256).
    /// </Summary>
    public static Column Sha2(Column col, Column numBits)
    {
        return new Column(CreateExpression("sha2", false, col, numBits));
    }


    /// <summary>
    ///     the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in Cartesian
    ///     coordinates, as if computed by java.lang.Math.atan2()
    /// </summary>
    /// <param name="col1"></param>
    /// <param name="col2"></param>
    /// <returns></returns>
    public static Column Atan2(string col1, string col2)
    {
        return new Column(CreateExpression("atan2", false, Col(col1), Col(col2)));
    }


    /// <summary>
    ///     the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in Cartesian
    ///     coordinates, as if computed by java.lang.Math.atan2()
    /// </summary>
    /// <param name="col1"></param>
    /// <param name="col2"></param>
    /// <returns></returns>
    public static Column Atan2(Column col1, Column col2)
    {
        return new Column(CreateExpression("atan2", false, col1, col2));
    }


    /// <summary>
    ///     the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in Cartesian
    ///     coordinates, as if computed by java.lang.Math.atan2()
    /// </summary>
    /// <param name="col1"></param>
    /// <param name="col2"></param>
    /// <returns></returns>
    public static Column Atan2(Column col1, string col2)
    {
        return new Column(CreateExpression("atan2", false, col1, Col(col2)));
    }

    /// <summary>
    ///     the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in Cartesian
    ///     coordinates, as if computed by java.lang.Math.atan2()
    /// </summary>
    /// <param name="col1"></param>
    /// <param name="col2"></param>
    /// <returns></returns>
    public static Column Atan2(string col1, Column col2)
    {
        return new Column(CreateExpression("atan2", false, Col(col1), col2));
    }

    /// <summary>
    ///     the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in Cartesian
    ///     coordinates, as if computed by java.lang.Math.atan2()
    /// </summary>
    /// <param name="col1"></param>
    /// <param name="col2"></param>
    /// <returns></returns>
    public static Column Atan2(float col1, float col2)
    {
        return new Column(CreateExpression("atan2", false, Lit(col1), Lit(col2)));
    }

    /// <summary>
    ///     the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in Cartesian
    ///     coordinates, as if computed by java.lang.Math.atan2()
    /// </summary>
    /// <param name="col1"></param>
    /// <param name="col2"></param>
    /// <returns></returns>
    public static Column Atan2(Column col1, float col2)
    {
        return new Column(CreateExpression("atan2", false, col1, Lit(col2)));
    }

    /// <summary>
    ///     the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in Cartesian
    ///     coordinates, as if computed by java.lang.Math.atan2()
    /// </summary>
    /// <param name="col1"></param>
    /// <param name="col2"></param>
    /// <returns></returns>
    public static Column Atan2(float col1, Column col2)
    {
        return new Column(CreateExpression("atan2", false, Lit(col1), col2));
    }


    /// <summary>
    ///     the theta component of the point (r, theta) in polar coordinates that corresponds to the point (x, y) in Cartesian
    ///     coordinates, as if computed by java.lang.Math.atan2()
    /// </summary>
    /// <param name="col1"></param>
    /// <param name="col2"></param>
    /// <returns></returns>
    public static Column Atan2(string col1, float col2)
    {
        return new Column(CreateExpression("atan2", false, Col(col1), Lit(col2)));
    }

    /// <Summary>
    ///     Reflect
    ///     Calls a method with reflection.
    /// </Summary>
    public static Column Reflect(params Column[] cols)
    {
        return new Column(CreateExpression("reflect", false, cols));
    }

    /// <Summary>
    ///     Reflect
    ///     Calls a method with reflection.
    /// </Summary>
    public static Column Reflect(params string[] cols)
    {
        return new Column(CreateExpression("reflect", false, cols.Select(Col).ToArray()));
    }

    /// <Summary>
    ///     JavaMethod
    ///     Calls a method with reflection.
    /// </Summary>
    public static Column JavaMethod(params Column[] cols)
    {
        return Reflect(cols);
    }

    /// <Summary>
    ///     JavaMethod
    ///     Calls a method with reflection.
    /// </Summary>
    public static Column JavaMethod(params string[] cols)
    {
        return Reflect(cols);
    }

    /// <Summary>
    ///     When
    /// </Summary>
    public static Column When(Column condition, Column value)
    {
        return new Column(CreateExpression("when", false, condition, value));
    }

    public static Column When(Column condition, object value)
    {
        return new Column(CreateExpression("when", false, condition, Lit(value)));
    }

    public static Column Expr(string expression)
    {
        return new Column(new Expression
        {
            ExpressionString = new Expression.Types.ExpressionString
            {
                Expression = expression
            }
        });
    }

    public static Column AddMonths(Column start, Column months)
    {
        return new Column(CreateExpression("add_months", false, start, months));
    }

    public static Column AddMonths(Column start, int months)
    {
        return AddMonths(start, Lit(months));
    }

    public static Column AddMonths(string start, Column months)
    {
        return AddMonths(Col(start), months);
    }

    public static Column AddMonths(string start, int months)
    {
        return AddMonths(Col(start), Lit(months));
    }

    public static Column ApproxCountDistinct(Column col, double rsd = 0.05F)
    {
        return new Column(CreateExpression("approx_count_distinct", false, col, rsd));
    }

    public static Column ApproxCountDistinct(string col, double rsd = 0.05F)
    {
        return new Column(CreateExpression("approx_count_distinct", false, Col(col), rsd));
    }

    public static Column ApproxCountDistinct(string col, Column rsd)
    {
        return new Column(CreateExpression("approx_count_distinct", false, Col(col), rsd));
    }

    public static Column ApproxCountDistinct(Column col, Column rsd)
    {
        return new Column(CreateExpression("approx_count_distinct", false, col, rsd));
    }

    public static Column ApproxPercentile(Column col, float percentage, long accuracy = 10000)
    {
        return new Column(CreateExpression("approx_percentile", false, col, Lit(percentage), Lit(accuracy)));
    }


    public static Column ApproxPercentile(Column col, float[] percentages, long accuracy = 10000)
    {
        return new Column(CreateExpression("approx_percentile", false, col, Lit(percentages), Lit(accuracy)));
    }

    public static Column ApproxPercentile(string col, float percentage, long accuracy = 10000)
    {
        return new Column(CreateExpression("approx_percentile", false, Col(col), Lit(percentage), Lit(accuracy)));
    }

    public static Column ApproxPercentile(Column col, Column percentages, Column accuracy)
    {
        return new Column(CreateExpression("approx_percentile", false, col, percentages, accuracy));
    }

    public static Column ArrayJoin(string col, string delimiter, string? nullReplacement = null)
    {
        if (string.IsNullOrEmpty(nullReplacement))
        {
            return new Column(CreateExpression("array_join", false, Col(col), Lit(delimiter)));
        }

        return new Column(CreateExpression("array_join", false, Col(col), Lit(delimiter), Lit(nullReplacement)));
    }

    public static Column ArrayJoin(Column col, string delimiter, string? nullReplacement = null)
    {
        if (string.IsNullOrEmpty(nullReplacement))
        {
            return new Column(CreateExpression("array_join", false, col, Lit(delimiter)));
        }

        return new Column(CreateExpression("array_join", false, col, Lit(delimiter), Lit(nullReplacement)));
    }

    public static Column ArrayJoin(Column col, Column delimiter)
    {
        return new Column(CreateExpression("array_join", false, col, delimiter));
    }

    public static Column ArrayJoin(Column col, Column delimiter, Column nullReplacement)
    {
        return new Column(CreateExpression("array_join", false, col, delimiter, nullReplacement));
    }

    public static Column ArrayInsert(Column col, int pos, Column value)
    {
        return new Column(CreateExpression("array_insert", false, col, Lit(pos), value));
    }

    public static Column ArrayInsert(Column col, Column pos, Column value)
    {
        return new Column(CreateExpression("array_insert", false, col, pos, value));
    }

    public static Column ArrayRepeat(Column col, int count)
    {
        return new Column(CreateExpression("array_repeat", false, col, Lit(count)));
    }

    public static Column ArrayRepeat(Column col, Column count)
    {
        return new Column(CreateExpression("array_repeat", false, col, count));
    }

    public static Column ArrayRepeat(string col, int count)
    {
        return new Column(CreateExpression("array_repeat", false, Col(col), Lit(count)));
    }

    public static Column ArrayRepeat(string col, Column count)
    {
        return new Column(CreateExpression("array_repeat", false, Col(col), count));
    }

    public static Column AssertTrue(Column col, string? errorMessage = null)
    {
        return new Column(CreateExpression("assert_true", false, col, errorMessage));
    }

    public static Column BTrim(Column col, string? trim = null)
    {
        if (string.IsNullOrEmpty(trim))
        {
            return new Column(CreateExpression("btrim", false, col));
        }

        return new Column(CreateExpression("btrim", false, col, trim));
    }

    public static Column BTrim(string col, string? trim = null)
    {
        if (string.IsNullOrEmpty(trim))
        {
            return new Column(CreateExpression("btrim", false, Col(col)));
        }

        return new Column(CreateExpression("btrim", false, Col(col), trim));
    }

    public static Column Bucket(int numOfBuckets, Column col)
    {
        return new Column(CreateExpression("bucket", false, Lit(numOfBuckets), col));
    }

    public static Column Bucket(int numOfBuckets, string col)
    {
        return new Column(CreateExpression("bucket", false, Lit(numOfBuckets), Col(col)));
    }

    public static Column ConcatWs(string sep, params Column[] cols)
    {
        return new Column(CreateExpression("concat_ws", false, Lit(sep), cols.Select(p => p.Expression).ToArray()));
    }

    public static Column ConcatWs(string sep, params string[] cols)
    {
        return new Column(CreateExpression("concat_ws", false, Lit(sep),
            cols.Select(p => Col(p).Expression).ToArray()));
    }

    public static DataFrame Broadcast(DataFrame src)
    {
        return src.Hint("broadcast");
    }

    public static Column Conv(string col, int fromBase, int toBase)
    {
        return Conv(Col(col), fromBase, toBase);
    }

    public static Column Conv(Column col, int fromBase, int toBase)
    {
        return new Column(CreateExpression("conv", false, col, Lit(fromBase), Lit(toBase)));
    }

    public static Column ConvertTimezone(Column sourceTzColumn, Column targetTz, Column sourceTz)
    {
        if (ReferenceEquals(null, sourceTzColumn))
        {
            return new Column(CreateExpression("convert_timezone", false, targetTz, sourceTz));
        }

        return new Column(CreateExpression("convert_timezone", false, sourceTzColumn, targetTz, sourceTz));
    }

    public static Column CreateMap(string cola, string colb)
    {
        return new Column(CreateExpression("map", false, cola, colb));
    }

    public static Column CreateMap(IDictionary<string, object> options)
    {
        var litOptions = new List<Column>();

        foreach (var option in options)
        {
            litOptions.Add(Lit(option.Key));
            litOptions.Add(Lit(option.Value));
        }

        return new Column(CreateExpression("map", false, litOptions.ToArray()));
    }
    
    public static Column CreateMap(IDictionary<string, string> options)
    {
        var litOptions = new List<Column>();

        foreach (var option in options)
        {
            litOptions.Add(Lit(option.Key));
            litOptions.Add(Lit(option.Value));
        }

        return new Column(CreateExpression("map", false, litOptions.ToArray()));
    }

    public static Column CreateMap(Column cola, Column colb)
    {
        return new Column(CreateExpression("map", false, cola, colb));
    }

    public static Column DateAdd(string start, int days)
    {
        return new Column(CreateExpression("date_add", false, Col(start), Lit(days)));
    }

    public static Column DateAdd(Column start, int days)
    {
        return new Column(CreateExpression("date_add", false, start, Lit(days)));
    }

    public static Column DateSub(string start, int days)
    {
        return new Column(CreateExpression("date_sub", false, Col(start), Lit(days)));
    }

    public static Column DateSub(Column start, int days)
    {
        return new Column(CreateExpression("date_sub", false, start, Lit(days)));
    }

    public static Column DateSub(Column start, Column days)
    {
        return new Column(CreateExpression("date_sub", false, start, days));
    }

    public static Column DateSub(string start, Column days)
    {
        return new Column(CreateExpression("date_sub", false, Col(start), days));
    }

    public static Column DateTrunc(string format, Column timestamp)
    {
        return new Column(CreateExpression("date_trunc", false, Lit(format), timestamp));
    }

    public static Column DateTrunc(string format, string timestamp)
    {
        return new Column(CreateExpression("date_trunc", false, Lit(format), Col(timestamp)));
    }

    public static Column First(string col, bool ignoreNulls = false)
    {
        return new Column(CreateExpression("first", false, Col(col), Lit(ignoreNulls)));
    }

    public static Column First(Column col, bool ignoreNulls = false)
    {
        return new Column(CreateExpression("first", false, col, Lit(ignoreNulls)));
    }

    public static Column Last(Column col, bool ignoreNulls = false)
    {
        return new Column(CreateExpression("last", false, col, Lit(ignoreNulls)));
    }

    public static Column Last(string col, bool ignoreNulls = false)
    {
        return new Column(CreateExpression("last", false, Col(col), Lit(ignoreNulls)));
    }

    public static Column FormatString(string format, params Column[] cols)
    {
        var newList = new List<Column>();
        newList.Add(Lit(format));
        newList.AddRange(cols);
        return new Column(CreateExpression("format_string", false, newList.ToArray()));
    }

    public static Column FormatString(string format, params string[] cols)
    {
        var newList = new List<Column>();
        newList.Add(Lit(format));
        newList.AddRange(cols.Select(Col));
        return new Column(CreateExpression("format_string", false, newList.ToArray()));
    }

    public static Column FromCsv(Column col, Column ddlSchema, IDictionary<string, object>? options = null)
    {
        if (options == null)
        {
            return new Column(CreateExpression("from_csv", false, col, ddlSchema));
        }

        var mappedOptions = CreateMap(options);
        return new Column(CreateExpression("from_csv", false, col, ddlSchema, mappedOptions));
    }

    public static Column FromJson(Column col, Column ddlSchema, IDictionary<string, object>? options = null)
    {
        if (options == null)
        {
            return new Column(CreateExpression("from_json", false, col, ddlSchema));
        }

        var mappedOptions = CreateMap(options);
        return new Column(CreateExpression("from_json", false, col, ddlSchema, mappedOptions));
    }

    /// <summary>
    ///     ddlSchema is a string with a DDL schema in such as "COLNAME INT, ANOTHERCOL STRING"
    /// </summary>
    /// <param name="col"></param>
    /// <param name="ddlSchema"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static Column FromJson(Column col, string ddlSchema, IDictionary<string, object>? options = null)
    {
        if (options == null)
        {
            return new Column(CreateExpression("from_json", false, col, Lit(ddlSchema)));
        }

        var mappedOptions = CreateMap(options);
        return new Column(CreateExpression("from_json", false, col, Lit(ddlSchema), mappedOptions));
    }

    /// <summary>
    ///     Column with the json value in, a schema as a StructType, or ArrayType of StructType
    /// </summary>
    /// <param name="col"></param>
    /// <param name="schema"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static Column FromJson(Column col, StructType schema, IDictionary<string, object>? options = null)
    {
        var ddlSchema = Lit(schema.Json());

        if (options == null)
        {
            return new Column(CreateExpression("from_json", false, col, ddlSchema));
        }

        var mappedOptions = CreateMap(options);
        return new Column(CreateExpression("from_json", false, col, ddlSchema, mappedOptions));
    }

    public static Column FromUtcTimestamp(string timestamp, string tz)
    {
        return new Column(CreateExpression("from_utc_timestamp", false, Lit(timestamp), Lit(tz)));
    }

    public static Column FromUtcTimestamp(Column timestamp, Column tz)
    {
        return new Column(CreateExpression("from_utc_timestamp", false, timestamp, tz));
    }

    public static Column Grouping(string col)
    {
        return Grouping(Col(col));
    }

    public static Column Grouping(Column col)
    {
        return new Column(CreateExpression("grouping", false, col));
    }

    public static Column JsonTuple(Column col, params string[] fields)
    {
        return new Column(
            CreateExpression("json_tuple", false, col, fields.Select(p => Lit(p).Expression).ToArray()));
    }

    public static Column JsonTuple(string col, params string[] fields)
    {
        return new Column(CreateExpression("json_tuple", false, Col(col),
            fields.Select(p => Lit(p).Expression).ToArray()));
    }

    public static Column Lag(Column col, int offset, object defaultValue)
    {
        return new Column(CreateExpression("lag", false, col, Lit(offset), Lit(defaultValue)));
    }

    public static Column Lag(string col, int offset, object defaultValue)
    {
        return new Column(CreateExpression("lag", false, Col(col), Lit(offset), Lit(defaultValue)));
    }

    public static Column Lag(Column col, int offset)
    {
        return new Column(CreateExpression("lag", false, col, Lit(offset)));
    }

    public static Column Lag(string col, int offset)
    {
        return new Column(CreateExpression("lag", false, Col(col), Lit(offset)));
    }


    public static Column Lag(Column col)
    {
        return new Column(CreateExpression("lag", false, col));
    }

    public static Column Lag(string col)
    {
        return new Column(CreateExpression("lag", false, Col(col)));
    }

    public static Column Lead(Column col, int offset, object defaultValue)
    {
        return new Column(CreateExpression("lead", false, col, Lit(offset), Lit(defaultValue)));
    }

    public static Column Lead(string col, int offset, object defaultValue)
    {
        return new Column(CreateExpression("lead", false, Col(col), Lit(offset), Lit(defaultValue)));
    }

    public static Column Lead(Column col, int offset)
    {
        return new Column(CreateExpression("lead", false, col, Lit(offset)));
    }

    public static Column Lead(string col, int offset)
    {
        return new Column(CreateExpression("lead", false, Col(col), Lit(offset)));
    }


    public static Column Lead(Column col)
    {
        return new Column(CreateExpression("lead", false, col));
    }

    public static Column Lead(string col)
    {
        return new Column(CreateExpression("lead", false, Col(col)));
    }

    public static Column Levenshtein(string left, string right, int? threshold = null)
    {
        return Levenshtein(Col(left), Col(right), threshold);
    }

    public static Column Levenshtein(Column left, Column right, int? threshold = null)
    {
        if (!threshold.HasValue)
        {
            return new Column(CreateExpression("levenshtein", false, left, right));
        }

        return new Column(CreateExpression("levenshtein", false, left, right, Lit(threshold.Value)));
    }

    public static Column Like(string col, string pattern, string? escape = null)
    {
        return Like(Col(col), Lit(pattern), escape == null ? null : Lit(escape));
    }

    public static Column Like(Column col, Column pattern, Column? escape = null)
    {
        if (Equals(null, escape))
        {
            return new Column(CreateExpression("like", false, col, pattern));
        }

        return new Column(CreateExpression("like", false, col, pattern, escape));
    }

    /// <summary>
    ///     Find the occurence of substr in col - the PySpark docs say that pos is 0-based but you need to use 1 for the first
    ///     char
    /// </summary>
    /// <param name="substr"></param>
    /// <param name="col"></param>
    /// <param name="pos"></param>
    /// <returns>Column</returns>
    public static Column Locate(string substr, string col, int? pos = null)
    {
        return Locate(substr, Col(col), pos);
    }

    /// <summary>
    ///     Find the occurence of substr in col - the PySpark docs say that pos is 0-based but you need to use 1 for the first
    ///     char
    /// </summary>
    /// <param name="substr"></param>
    /// <param name="col"></param>
    /// <param name="pos"></param>
    /// <returns>Column</returns>
    public static Column Locate(string substr, Column col, int? pos = null)
    {
        return Locate(Lit(substr), col, pos);
    }

    /// <summary>
    ///     Find the occurence of substr in col - the PySpark docs say that pos is 0-based but you need to use 1 for the first
    ///     char
    /// </summary>
    /// <param name="substr"></param>
    /// <param name="col"></param>
    /// <param name="pos"></param>
    /// <returns>Column</returns>
    public static Column Locate(Column substr, Column col, int? pos = null)
    {
        if (pos.HasValue)
        {
            return new Column(CreateExpression("locate", false, substr, col, Lit(pos.Value)));
        }

        return new Column(CreateExpression("locate", false, substr, col));
    }

    public static Column LPad(string col, int len, string pad)
    {
        return LPad(Col(col), len, pad);
    }

    public static Column LPad(Column col, int len, string pad)
    {
        return new Column(CreateExpression("lpad", false, col, Lit(len), Lit(pad)));
    }


    public static Column RPad(string col, int len, string pad)
    {
        return RPad(Col(col), len, pad);
    }

    public static Column RPad(Column col, int len, string pad)
    {
        return new Column(CreateExpression("rpad", false, col, Lit(len), Lit(pad)));
    }


    public static Column MakeDtInterval()
    {
        return new Column(CreateExpression("make_dt_interval", false));
    }

    public static Column MakeDtInterval(string day)
    {
        return MakeDtInterval(Col(day));
    }

    public static Column MakeDtInterval(Column day)
    {
        return new Column(CreateExpression("make_dt_interval", false, day));
    }

    public static Column MakeDtInterval(string day, string hour)
    {
        return MakeDtInterval(Col(day), Col(hour));
    }

    public static Column MakeDtInterval(Column day, Column hour)
    {
        return new Column(CreateExpression("make_dt_interval", false, day, hour));
    }

    public static Column MakeDtInterval(string day, string hour, string minute)
    {
        return MakeDtInterval(Col(day), Col(hour), Col(minute));
    }

    public static Column MakeDtInterval(Column day, Column hour, Column minute)
    {
        return new Column(CreateExpression("make_dt_interval", false, day, hour, minute));
    }

    public static Column MakeDtInterval(string day, string hour, string minute, double seconds)
    {
        return MakeDtInterval(Col(day), Col(hour), Col(minute));
    }

    public static Column MakeDtInterval(Column day, Column hour, Column minute, Column seconds)
    {
        return new Column(CreateExpression("make_dt_interval", false, day, hour, minute, seconds));
    }

    public static Column MakeTimestamp(string years, string months, string days, string hours, string mins, string secs,
        string? timezone = null)
    {
        return MakeTimestamp(Col(years), Col(months), Col(days), Col(hours), Col(mins), Col(secs),
            timezone == null ? null : Col(timezone));
    }

    public static Column MakeTimestamp(Column years, Column months, Column days, Column hours, Column mins, Column secs,
        Column? timezone = null)
    {
        if (Equals(null, timezone))
        {
            return new Column(CreateExpression("make_timestamp", false, years, months, days, hours, mins, secs));
        }

        return new Column(
            CreateExpression("make_timestamp", false, years, months, days, hours, mins, secs, timezone));
    }

    public static Column MakeTimestampLtz(string years, string months, string days, string hours, string mins,
        string secs,
        string? timezone = null)
    {
        return MakeTimestampLtz(Col(years), Col(months), Col(days), Col(hours), Col(mins), Col(secs),
            timezone == null ? null : Col(timezone));
    }

    public static Column MakeTimestampLtz(Column years, Column months, Column days, Column hours, Column mins,
        Column secs, Column? timezone = null)
    {
        if (Equals(null, timezone))
        {
            return new Column(CreateExpression("make_timestamp_ltz", false, years, months, days, hours, mins, secs));
        }

        return new Column(CreateExpression("make_timestamp_ltz", false, years, months, days, hours, mins, secs,
            timezone));
    }

    /// <summary>
    ///     col is the name of the column to apply the mask to
    /// </summary>
    /// <param name="col"></param>
    /// <param name="upperChar"></param>
    /// <param name="lowerChar"></param>
    /// <param name="digitChar"></param>
    /// <param name="otherChar"></param>
    /// <returns></returns>
    public static Column Mask(string col, string? upperChar = null, string? lowerChar = null, string? digitChar = null,
        string? otherChar = null)
    {
        if (string.IsNullOrEmpty(upperChar))
        {
            upperChar = "X";
        }

        if (string.IsNullOrEmpty(lowerChar))
        {
            lowerChar = "x";
        }

        if (string.IsNullOrEmpty(digitChar))
        {
            digitChar = "n";
        }

        return Mask(Col(col), Lit(upperChar), Lit(lowerChar), Lit(digitChar), Lit(otherChar));
    }

    /// <summary>
    ///     To use the default specify null instead of a column
    /// </summary>
    /// <param name="col"></param>
    /// <param name="upperChar"></param>
    /// <param name="lowerChar"></param>
    /// <param name="digitChar"></param>
    /// <param name="otherChar"></param>
    /// <returns></returns>
    public static Column Mask(Column col, Column? upperChar = null, Column? lowerChar = null, Column? digitChar = null,
        Column? otherChar = null)
    {
        if (Equals(null, upperChar))
        {
            upperChar = Lit("X");
        }

        if (Equals(null, lowerChar))
        {
            lowerChar = Lit("x");
        }

        if (Equals(null, digitChar))
        {
            digitChar = Lit("n");
        }

        if (Equals(null, otherChar))
        {
            otherChar = Lit(null as string);
        }

        if (upperChar.Expression.Literal.String == null)
        {
            throw new SparkException("upperChar must be a Lit(\"string\"), cannot be any other type (including char!)");
        }

        if (lowerChar.Expression.Literal.String == null)
        {
            throw new SparkException("lowerChar must be a Lit(\"string\"), cannot be any other type (including char!)");
        }

        if (digitChar.Expression.Literal.String == null)
        {
            throw new SparkException("digitChar must be a Lit(\"string\"), cannot be any other type (including char!)");
        }

        if (otherChar.Expression.Literal.String == null)
        {
            throw new SparkException("otherChar must be a Lit(\"string\"), cannot be any other type (including char!)");
        }

        return new Column(CreateExpression("mask", false, col, upperChar, lowerChar, digitChar, otherChar));
    }

    public static Column MonthsBetween(string date1Col, string date2Col, bool? roundOff = null)
    {
        return MonthsBetween(Col(date1Col), Col(date2Col), roundOff);
    }

    public static Column MonthsBetween(Column date1Col, Column date2Col, bool? roundOff = null)
    {
        if (roundOff.HasValue)
        {
            return new Column(CreateExpression("months_between", false, date1Col, date2Col, Lit(roundOff.Value)));
        }

        return new Column(CreateExpression("months_between", false, date1Col, date2Col));
    }

    public static Column NthValue(string col, int offset, bool? ignoreNulls = null)
    {
        return NthValue(Col(col), offset, ignoreNulls);
    }

    public static Column NthValue(Column col, int offset, bool? ignoreNulls = null)
    {
        return NthValue(col, Lit(offset), ignoreNulls);
    }

    public static Column NthValue(Column col, Column offset, bool? ignoreNulls = null)
    {
        if (ignoreNulls.HasValue)
        {
            return new Column(CreateExpression("nth_value", false, col, offset, Lit(ignoreNulls.Value)));
        }

        return new Column(CreateExpression("nth_value", false, col, offset));
    }

    public static Column Ntile(int n)
    {
        return new Column(CreateExpression("ntile", false, Lit(n)));
    }


    public static Column Overlay(string src, string replace, int pos, int? len = null)
    {
        return Overlay(Col(src), Col(replace), Lit(pos), len.HasValue ? Lit(len.Value) : null);
    }

    public static Column Overlay(Column src, Column replace, int pos, int? len = null)
    {
        return Overlay(src, replace, Lit(pos), len.HasValue ? Lit(len.Value) : null);
    }

    public static Column Overlay(Column src, Column replace, Column pos, Column? len = null)
    {
        if (Equals(null, len))
        {
            return new Column(CreateExpression("overlay", false, src, replace, pos));
        }

        return new Column(CreateExpression("overlay", false, src, replace, pos, len));
    }

    public static Column Percentile(string col, float[] percentage, int frequency)
    {
        return Percentile(Col(col), Lit(percentage), Lit(frequency));
    }

    public static Column Percentile(string col, float percentage, int frequency)
    {
        return Percentile(Col(col), Lit(percentage), Lit(frequency));
    }

    public static Column Percentile(Column col, float[] percentage, int frequency)
    {
        return Percentile(col, Lit(percentage), Lit(frequency));
    }

    public static Column Percentile(Column col, float percentage, int frequency)
    {
        return Percentile(col, Lit(percentage), Lit(frequency));
    }

    public static Column Percentile(Column col, Column percentage, Column frequency)
    {
        return new Column(CreateExpression("percentile", false, col, percentage, frequency));
    }

    public static Column PercentileApprox(string col, float[] percentage, int accuracy)
    {
        return Percentile(Col(col), Lit(percentage), Lit(accuracy));
    }

    public static Column PercentileApprox(string col, float percentage, int accuracy)
    {
        return Percentile(Col(col), Lit(percentage), Lit(accuracy));
    }

    public static Column PercentileApprox(Column col, float[] percentage, int accuracy)
    {
        return Percentile(col, Lit(percentage), Lit(accuracy));
    }

    public static Column PercentileApprox(Column col, float percentage, int accuracy)
    {
        return Percentile(col, Lit(percentage), Lit(accuracy));
    }

    public static Column PercentileApprox(Column col, Column percentage, Column accuracy)
    {
        return new Column(CreateExpression("percentile_approx", false, col, percentage, accuracy));
    }

    public static Column ParseUrl(string urlCol, string partToExtractCol, string? keyCol = null)
    {
        return ParseUrl(Col(urlCol), Col(partToExtractCol), keyCol == null ? null : Col(keyCol));
    }

    public static Column ParseUrl(Column col, Column partToExtract, Column? key = null)
    {
        if (Equals(null, key))
        {
            return new Column(CreateExpression("parse_url", false, col, partToExtract));
        }

        return new Column(CreateExpression("parse_url", false, col, partToExtract, key));
    }

    public static Column Position(string substrColumn, string strColumn, string? startColumn = null)
    {
        return Position(Col(substrColumn), Col(strColumn), startColumn == null ? null : Col(startColumn));
    }

    public static Column Position(Column substrColumn, Column strColumn, Column? startColumn = null)
    {
        if (Equals(null, startColumn))
        {
            return new Column(CreateExpression("position", false, substrColumn, strColumn));
        }

        return new Column(CreateExpression("position", false, substrColumn, strColumn, startColumn));
    }

    public static Column PrintF(string format, params string[] cols)
    {
        return PrintF(Col(format), cols.Select(Col).ToArray());
    }

    public static Column PrintF(Column format, params Column[] cols)
    {
        return new Column(CreateExpression("printf", false, cols.Prepend(format).ToArray()));
    }

    public static Column RaiseError(string message)
    {
        return new Column(CreateExpression("raise_error", false, Lit(message)));
    }

    public static Column RaiseError(Column message)
    {
        return new Column(CreateExpression("raise_error", false, message));
    }

    public static Column RegexpExtract(string col, string pattern, int idx)
    {
        return RegexpExtract(Col(col), pattern, idx);
    }

    public static Column RegexpExtract(Column col, string pattern, int idx)
    {
        return RegexpExtract(col, Lit(pattern), Lit(idx));
    }

    public static Column RegexpExtract(Column col, Column pattern, Column idx)
    {
        return new Column(CreateExpression("regexp_extract", false, col, pattern, idx));
    }

    public static Column RegexpExtractAll(string col, string pattern, int idx)
    {
        return RegexpExtractAll(Col(col), pattern, idx);
    }

    public static Column RegexpExtractAll(Column col, string pattern, int idx)
    {
        return RegexpExtractAll(col, Lit(pattern), Lit(idx));
    }

    public static Column RegexpExtractAll(Column col, Column pattern, Column idx)
    {
        return new Column(CreateExpression("regexp_extract_all", false, col, pattern, idx));
    }

    public static Column RegexpExtractInstr(string col, string pattern, int idx)
    {
        return RegexpExtractInstr(Col(col), pattern, idx);
    }

    public static Column RegexpExtractInstr(Column col, string pattern, int idx)
    {
        return RegexpExtractInstr(col, Lit(pattern), Lit(idx));
    }

    public static Column RegexpExtractInstr(Column col, Column pattern, Column idx)
    {
        return new Column(CreateExpression("regexp_instr", false, col, pattern, idx));
    }

    public static Column RegexpReplace(string col, string pattern, string replacement)
    {
        return RegexpReplace(Col(col), Lit(pattern), Lit(replacement));
    }

    public static Column RegexpReplace(Column col, string pattern, string replacement)
    {
        return RegexpReplace(col, Lit(pattern), Lit(replacement));
    }

    public static Column RegexpReplace(Column col, Column pattern, Column replacement)
    {
        return new Column(CreateExpression("regexp_replace", false, col, pattern, replacement));
    }

    public static Column Replace(Column src, string search, string replace)
    {
        return Replace(src, Lit(search), Lit(replace));
    }

    public static Column Replace(string src, string search, string replace)
    {
        return Replace(Col(src), search, replace);
    }

    public static Column Replace(Column src, Column search, Column replace)
    {
        return new Column(CreateExpression("replace", false, src, search, replace));
    }

    public static Column SchemaOfCsv(string schema, IDictionary<string, object>? dict = null)
    {
        return SchemaOfCsv(Lit(schema), dict);
    }

    public static Column SchemaOfCsv(Column schema, IDictionary<string, object>? dict = null)
    {
        if (dict != null)
        {
            return new Column(CreateExpression("schema_of_csv", false, schema, Lit(dict)));
        }

        return new Column(CreateExpression("schema_of_csv", false, schema));
    }

    public static Column SchemaOfJson(string json, IDictionary<string, object>? dict = null)
    {
        return SchemaOfJson(Lit(json), dict);
    }

    public static Column SchemaOfJson(Column json, IDictionary<string, object>? dict = null)
    {
        if (dict != null)
        {
            return new Column(CreateExpression("schema_of_json", false, json, Lit(dict)));
        }

        return new Column(CreateExpression("schema_of_json", false, json));
    }

    public static Column Sentences(string col, string? language = null, string? country = null)
    {
        return Sentences(Col(col), string.IsNullOrEmpty(language) ? null : Lit(language),
            string.IsNullOrEmpty(country) ? null : Lit(country));
    }

    public static Column Sentences(Column col, Column? language = null, Column? country = null)
    {
        if (Equals(null, language))
        {
            language = Lit("");
        }

        if (Equals(null, country))
        {
            country = Lit("");
        }

        return new Column(CreateExpression("sentences", false, col, language, country));
    }

    public static Column ToDate(string col, string? format = null)
    {
        return ToDate(Col(col), string.IsNullOrEmpty(format) ? null : Lit(format));
    }


    public static Column ToDate(Column col, Column? format = null)
    {
        if (Equals(null, format))
        {
            return new Column(CreateExpression("to_Date", false, col));
        }

        return new Column(CreateExpression("to_Date", false, col, format));
    }


    public static Column Struct(params Column[] cols)
    {
        return new Column(CreateExpression("struct", false, cols));
    }

    /// <Summary>
    ///     Round
    ///     Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0 or at integral part
    ///     when `scale` &lt; 0.
    /// </Summary>
    public static Column Round(Column col, int scale)
    {
        return new Column(CreateExpression("round", false, col, Lit(scale)));
    }

    /// <Summary>
    ///     Round
    ///     Round the given value to `scale` default scale = 0
    /// </Summary>
    public static Column Round(string col)
    {
        return new Column(CreateExpression("round", false, Col(col)));
    }

    public static Column Get(string col, int index)
    {
        return Get(Col(col), Lit(index));
    }

    public static Column Get(string col, string index)
    {
        return Get(Col(col), Col(index));
    }

    public static Column Get(Column col, Column index)
    {
        return new Column(CreateExpression("get", false, col, index));
    }

    public static Column Sequence(Column start, Column stop, Column? step = null)
    {
        if (Equals(null, step))
        {
            return new Column(CreateExpression("sequence", false, start, stop));
        }

        return new Column(CreateExpression("sequence", false, start, stop, step));
    }

    public static Column Sequence(string start, string end, string? step = null)
    {
        return Sequence(Col(start), Col(end), string.IsNullOrEmpty(step) ? null : Col(step));
    }

    /// <summary>
    ///     Generates session window given a timestamp specifying column. Session window is one of dynamic windows, which means
    ///     the length of window is varying according to the given inputs. The length of session window is defined as “the
    ///     timestamp of latest input of the session + gap duration”, so when the new inputs are bound to the current session
    ///     window, the end time of session window can be expanded according to the new inputs. Windows can support microsecond
    ///     precision. Windows in the order of months are not supported. For a streaming query, you may use the function
    ///     current_timestamp to generate windows on processing time. gapDuration is provided as strings, e.g. ‘1 second’, ‘1
    ///     day 12 hours’, ‘2 minutes’. Valid interval strings are ‘week’, ‘day’, ‘hour’, ‘minute’, ‘second’, ‘millisecond’,
    ///     ‘microsecond’. It could also be a Column which can be evaluated to gap duration dynamically based on the input row.
    ///     The output column will be a struct called ‘session_window’ by default with the nested columns ‘start’ and ‘end’,
    ///     where ‘start’ and ‘end’ will be of pyspark.sql.types.TimestampType.
    ///     Example:
    ///     ```csharp
    ///     var df = Spark.CreateDataFrame(ToRows((ToRow(DateTime.Parse("2016-03-11 09:00:07"), 1)))).ToDf("date", "val");
    ///     var w = df.GroupBy(SessionWindow("date", "5 seconds")).Agg(Sum("val").Alias("sum"));
    ///     w.Select(w["session_window"]["start"].Cast("string").Alias("start"),
    ///     w["session_window"]["end"].Cast("string").Alias("stop"), Col("sum")).Show();
    ///     ```
    /// </summary>
    /// <param name="timeColumn"></param>
    /// <param name="gapDuration"></param>
    /// <returns></returns>
    public static Column SessionWindow(Column timeColumn, Column gapDuration)
    {
        return new Column(CreateExpression("session_window", false, timeColumn, gapDuration));
    }

    /// <summary>
    ///     Generates session window given a timestamp specifying column. Session window is one of dynamic windows, which means
    ///     the length of window is varying according to the given inputs. The length of session window is defined as “the
    ///     timestamp of latest input of the session + gap duration”, so when the new inputs are bound to the current session
    ///     window, the end time of session window can be expanded according to the new inputs. Windows can support microsecond
    ///     precision. Windows in the order of months are not supported. For a streaming query, you may use the function
    ///     current_timestamp to generate windows on processing time. gapDuration is provided as strings, e.g. ‘1 second’, ‘1
    ///     day 12 hours’, ‘2 minutes’. Valid interval strings are ‘week’, ‘day’, ‘hour’, ‘minute’, ‘second’, ‘millisecond’,
    ///     ‘microsecond’. It could also be a Column which can be evaluated to gap duration dynamically based on the input row.
    ///     The output column will be a struct called ‘session_window’ by default with the nested columns ‘start’ and ‘end’,
    ///     where ‘start’ and ‘end’ will be of pyspark.sql.types.TimestampType.
    ///     Example:
    ///     ```csharp
    ///     var df = Spark.CreateDataFrame(ToRows((ToRow(DateTime.Parse("2016-03-11 09:00:07"), 1)))).ToDf("date", "val");
    ///     var w = df.GroupBy(SessionWindow("date", "5 seconds")).Agg(Sum("val").Alias("sum"));
    ///     w.Select(w["session_window"]["start"].Cast("string").Alias("start"),
    ///     w["session_window"]["end"].Cast("string").Alias("stop"), Col("sum")).Show();
    ///     ```
    /// </summary>
    /// <param name="timeColumn"></param>
    /// <param name="gapDuration"></param>
    /// <returns></returns>
    public static Column SessionWindow(string timeColumn, string gapDuration)
    {
        return SessionWindow(Col(timeColumn), Lit(gapDuration));
    }

    public static Column Slice(Column x, Column start, Column length)
    {
        return new Column(CreateExpression("slice", false, x, start, length));
    }

    public static Column Slice(string x, string start, string length)
    {
        return Slice(Col(x), Col(start), Col(length));
    }

    public static Column Slice(string x, int start, int length)
    {
        return Slice(Col(x), Lit(start), Lit(length));
    }

    public static Column Slice(Column x, int start, int length)
    {
        return Slice(x, Lit(start), Lit(length));
    }

    /// <Summary>
    ///     Array
    /// </Summary>
    public static Column Array(params int?[] cols)
    {
        return new Column(CreateExpression("array", false, cols.Select(Lit).ToArray()));
    }

    public static Column SortArray(Column col, bool? asc = null)
    {
        if (asc.HasValue)
        {
            return new Column(CreateExpression("sort_array", false, col, Lit(asc.Value)));
        }

        return new Column(CreateExpression("sort_array", false, col));
    }

    public static Column Split(Column str, string pattern, int? limit = null)
    {
        if (limit.HasValue)
        {
            return new Column(CreateExpression("split", false, str, Lit(pattern), Lit(limit.Value)));
        }

        return new Column(CreateExpression("split", false, str, Lit(pattern)));
    }

    public static Column Split(string str, string pattern, int? limit = null)
    {
        return Split(Col(str), pattern, limit);
    }

    public static Column StrToMap(Column text, Column? pairDelim = null, Column? keyValueDelim = null)
    {
        if (Equals(null, pairDelim))
        {
            pairDelim = Lit(",");
        }

        if (Equals(null, keyValueDelim))
        {
            keyValueDelim = Lit(":");
        }

        return new Column(CreateExpression("str_to_map", false, text, pairDelim, keyValueDelim));
    }

    public static Column StrToMap(string text, string? pairDelim = null, string? keyValueDelim = null)
    {
        return StrToMap(Col(text), string.IsNullOrEmpty(pairDelim) ? null : Col(pairDelim),
            string.IsNullOrEmpty(keyValueDelim) ? null : Col(keyValueDelim));
    }


    public static Column Substr(Column src, Column pos, Column len)
    {
        return new Column(CreateExpression("substr", false, src.Expression, pos.Expression, len.Expression));
    }

    public static Column Substr(string src, string pos, string len)
    {
        return Substr(Col(src), Col(pos), Col(len));
    }


    public static Column Substring(Column src, int pos, int len)
    {
        return new Column(CreateExpression("substring", false, src.Expression, Lit(pos).Expression,
            Lit(len).Expression));
    }

    public static Column Substring(string src, int pos, int len)
    {
        return Substring(Col(src), pos, len);
    }

    public static Column SubstringIndex(Column str, string delim, int count)
    {
        return new Column(CreateExpression("substring_index", false, str.Expression, Lit(delim).Expression,
            Lit(count).Expression));
    }

    public static Column SubstringIndex(string str, string delim, int count)
    {
        return SubstringIndex(Col(str), delim, count);
    }

    public static Column ToBinary(Column col, Column? format = null)
    {
        if (Equals(null, format))
        {
            return new Column(CreateExpression("to_binary", false, col));
        }

        return new Column(CreateExpression("to_binary", false, col, format));
    }

    public static Column ToBinary(string col, string? format = null)
    {
        if (string.IsNullOrEmpty(format))
        {
            return ToBinary(Col(col));
        }

        return ToBinary(Col(col), Lit(format));
    }

    public static Column TryToBinary(Column col, Column? format = null)
    {
        if (Equals(null, format))
        {
            return new Column(CreateExpression("try_to_binary", false, col));
        }

        return new Column(CreateExpression("try_to_binary", false, col, format));
    }

    public static Column TryToBinary(string col, string? format = null)
    {
        if (string.IsNullOrEmpty(format))
        {
            return TryToBinary(Col(col));
        }

        return TryToBinary(Col(col), Lit(format));
    }

    public static Column ToChar(Column col, Column? format = null)
    {
        if (Equals(null, format))
        {
            return new Column(CreateExpression("to_char", false, col));
        }

        return new Column(CreateExpression("to_char", false, col, format));
    }

    public static Column ToChar(string col, string? format = null)
    {
        if (string.IsNullOrEmpty(format))
        {
            return ToChar(Col(col));
        }

        return ToChar(Col(col), Lit(format));
    }

    public static Column ToNumber(Column col, Column? format = null)
    {
        if (Equals(null, format))
        {
            return new Column(CreateExpression("to_number", false, col));
        }

        return new Column(CreateExpression("to_number", false, col, format));
    }

    public static Column ToNumber(string col, string? format = null)
    {
        if (string.IsNullOrEmpty(format))
        {
            return ToNumber(Col(col));
        }

        return ToNumber(Col(col), Lit(format));
    }

    public static Column ToCsv(Column col, IDictionary<string, object>? options = null)
    {
        if (options == null)
        {
            return new Column(CreateExpression("to_csv", false, col));
        }

        return new Column(CreateExpression("to_csv", false, col, Lit(options)));
    }

    public static Column ToCsv(string col, IDictionary<string, object>? options = null)
    {
        return ToCsv(Col(col), options);
    }


    public static Column ToJson(Column col, IDictionary<string, object>? options = null)
    {
        if (options == null)
        {
            return new Column(CreateExpression("to_json", false, col));
        }

        return new Column(CreateExpression("to_json", false, col, Lit(options)));
    }

    public static Column ToJson(string col, IDictionary<string, object>? options = null)
    {
        return ToJson(Col(col), options);
    }

    public static Column ToTimestampLtz(Column timestamp, Column? format = null)
    {
        if (Equals(null, format))
        {
            return new Column(CreateExpression("to_timestamp_ltz", false, timestamp));
        }

        return new Column(CreateExpression("to_timestamp_ltz", false, timestamp, format));
    }

    public static Column ToTimestampLtz(string timestamp, string? format = null)
    {
        return ToTimestampLtz(Col(timestamp), string.IsNullOrEmpty(format) ? null : Lit(format));
    }

    public static Column ToTimestampNtz(Column timestamp, Column? format = null)
    {
        if (Equals(null, format))
        {
            return new Column(CreateExpression("to_timestamp_ntz", false, timestamp));
        }

        return new Column(CreateExpression("to_timestamp_ntz", false, timestamp, format));
    }

    public static Column ToTimestampNtz(string timestamp, string? format = null)
    {
        return ToTimestampNtz(Col(timestamp), string.IsNullOrEmpty(format) ? null : Lit(format));
    }

    public static Column ToUnixTimestamp(Column timestamp, Column? format = null)
    {
        if (Equals(null, format))
        {
            return new Column(CreateExpression("to_unix_timestamp", false, timestamp));
        }

        return new Column(CreateExpression("to_unix_timestamp", false, timestamp, format));
    }

    public static Column ToUnixTimestamp(string timestamp, string? format = null)
    {
        return ToUnixTimestamp(Col(timestamp), string.IsNullOrEmpty(format) ? null : Lit(format));
    }

    public static Column ToUtcTimestamp(Column timestamp, Column tz)
    {
        return new Column(CreateExpression("to_utc_timestamp", false, timestamp, tz));
    }

    public static Column ToUtcTimestamp(string timestamp, string tz)
    {
        return ToUtcTimestamp(Col(timestamp), Lit(tz));
    }

    public static Column Translate(Column source, string matching, string replace)
    {
        return new Column(CreateExpression("translate", false, source, Lit(matching), Lit(replace)));
    }

    public static Column Translate(string source, string matching, string replace)
    {
        return Translate(Col(source), matching, replace);
    }

    public static Column WidthBucket(Column vstr, Column minstr, Column maxstr, Column numBuckets)
    {
        return new Column(CreateExpression("width_bucket", false, vstr, minstr, maxstr, numBuckets));
    }

    public static Column WidthBucket(Column vstr, Column minstr, Column maxstr, int numBuckets)
    {
        return WidthBucket(vstr, minstr, maxstr, numBuckets);
    }

    public static Column WidthBucket(string vstr, string minstr, string maxstr, int numBuckets)
    {
        return WidthBucket(Col(vstr), Col(minstr), Col(maxstr), numBuckets);
    }

    public static Column WidthBucket(string vstr, string minstr, string maxstr, string numBuckets)
    {
        return WidthBucket(Col(vstr), Col(minstr), Col(maxstr), Col(numBuckets));
    }

    public static Column WindowTime(Column windowColumn)
    {
        return new Column(CreateExpression("window_time", false, windowColumn));
    }

    public static Column CountMinSketch(Column col, Column eps, Column confidence, Column seed)
    {
        return new Column(CreateExpression("count_min_sketch", false, col, eps, confidence, seed));
    }

    public static Column CountMinSketch(string col, string eps, string confidence, string seed)
    {
        return CountMinSketch(Col(col), Col(eps), Col(confidence), Col(seed));
    }

    public static Column HllSketchAgg(string col, int? lgConfigKint = null)
    {
        return HllSketchAgg(Col(col), lgConfigKint);
    }

    public static Column HllSketchAgg(Column col, int? lgConfigKint = null)
    {
        if (lgConfigKint.HasValue)
        {
            return new Column(CreateExpression("hll_sketch_agg", false, col, Lit(lgConfigKint.Value)));
        }

        return new Column(CreateExpression("hll_sketch_agg", false, col));
    }

    public static Column HllSketchEstimate(Column col)
    {
        return new Column(CreateExpression("hll_sketch_estimate", false, col));
    }

    public static Column HllSketchEstimate(string col)
    {
        return HllSketchEstimate(Col(col));
    }

    public static Column HllUnion(Column col1, Column col2, bool? allowDifferentLgConfigKbool = null)
    {
        if (allowDifferentLgConfigKbool.HasValue)
        {
            return new Column(CreateExpression("hll_union", false, col1, col2,
                Lit(allowDifferentLgConfigKbool.Value)));
        }

        return new Column(CreateExpression("hll_union", false, col1, col2));
    }

    public static Column HllUnion(string col1, string col2, bool? allowDifferentLgConfigKbool = null)
    {
        return HllUnion(Col(col1), Col(col2), allowDifferentLgConfigKbool);
    }

    public static Column AesDecrypt(string input, string key, string? mode = null, string? padding = null,
        string? aad = null)
    {
        Column modeCol = null;
        Column paddingCol = null;
        Column aadCol = null;

        if (!string.IsNullOrEmpty(mode))
        {
            modeCol = Lit(mode);
        }

        if (!string.IsNullOrEmpty(padding))
        {
            paddingCol = Lit(padding);
        }

        if (!string.IsNullOrEmpty(aad))
        {
            aadCol = Lit(aad);
        }


        return AesDecrypt(Col(input), Col(key), modeCol, paddingCol, aadCol);
    }


    public static Column AesDecrypt(Column input, Column key, Column? mode = null, Column? padding = null,
        Column? aad = null)
    {
        if (Equals(null, mode))
        {
            mode = Lit("GCM");
        }

        if (Equals(null, padding))
        {
            padding = Lit("DEFAULT");
        }

        if (Equals(null, aad))
        {
            aad = Lit("");
        }

        return new Column(CreateExpression("aes_decrypt", false, input, key, mode, padding, aad));
    }

    public static Column TryAesDecrypt(string input, string key, string? mode = null, string? padding = null,
        string? aad = null)
    {
        Column modeCol = null;
        Column paddingCol = null;
        Column aadCol = null;

        if (!string.IsNullOrEmpty(mode))
        {
            modeCol = Lit(mode);
        }

        if (!string.IsNullOrEmpty(padding))
        {
            paddingCol = Lit(padding);
        }

        if (!string.IsNullOrEmpty(aad))
        {
            aadCol = Lit(aad);
        }


        return TryAesDecrypt(Col(input), Col(key), modeCol, paddingCol, aadCol);
    }


    public static Column TryAesDecrypt(Column input, Column key, Column? mode = null, Column? padding = null,
        Column? aad = null)
    {
        if (Equals(null, mode))
        {
            mode = Lit("GCM");
        }

        if (Equals(null, padding))
        {
            padding = Lit("DEFAULT");
        }

        if (Equals(null, aad))
        {
            aad = Lit("");
        }

        return new Column(CreateExpression("try_aes_decrypt", false, input, key, mode, padding, aad));
    }

    public static Column AesEncrypt(Column input, Column key, Column? mode = null, Column? padding = null,
        Column? iv = null, Column? aad = null)
    {
        if (Equals(null, mode))
        {
            mode = Lit("GCM");
        }

        if (Equals(null, padding))
        {
            padding = Lit("DEFAULT");
        }

        if (Equals(null, iv))
        {
            iv = Lit("");
        }

        if (Equals(null, aad))
        {
            aad = Lit("");
        }

        return new Column(CreateExpression("aes_encrypt", false, input, key, mode, padding, iv, aad));
    }


    public static Column AesEncrypt(string input, string key, string? mode = null, string? padding = null,
        string? iv = null, string? aad = null)
    {
        Column modeCol = null;
        Column paddingCol = null;
        Column ivCol = null;
        Column aadCol = null;

        if (!string.IsNullOrEmpty(mode))
        {
            modeCol = Lit(mode);
        }

        if (!string.IsNullOrEmpty(padding))
        {
            paddingCol = Lit(padding);
        }

        if (!string.IsNullOrEmpty(iv))
        {
            ivCol = Lit(iv);
        }

        if (!string.IsNullOrEmpty(aad))
        {
            aadCol = Lit(aad);
        }

        return AesEncrypt(Col(input), Col(key), modeCol, paddingCol, ivCol, aadCol);
    }


    public static Column MakeInterval(Column? years = null, Column? months = null, Column? weeks = null,
        
        Column? days = null, Column? hours = null, Column? mins = null, Column? secs = null)
    {
        if (Equals(null, years))
        {
            years = Lit(0);
        }

        if (Equals(null, months))
        {
            months = Lit(0);
        }

        if (Equals(null, weeks))
        {
            weeks = Lit(0);
        }

        if (Equals(null, days))
        {
            days = Lit(0);
        }

        if (Equals(null, hours))
        {
            hours = Lit(0);
        }

        if (Equals(null, mins))
        {
            mins = Lit(0);
        }

        if (Equals(null, secs))
        {
            secs = Lit(0.0d);
        }

        return new Column(CreateExpression("make_interval", false, years, months, weeks, days, hours, mins, secs));
    }

    public static Column MakeYmInterval(Column years, Column months)
    {
        return new Column(CreateExpression("make_ym_interval", false, years, months));
    }

    public static Column MakeYmInterval(string years, string months)
    {
        return new Column(CreateExpression("make_ym_interval", false, Col(years), Col(months)));
    }

    // Callable Functions...
    public static Column ArraySort(Column col, BinaryFunction comparator)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(comparator);
        return new Column(CreateExpression("array_sort", false, col, expression));
    }

    public static Column ArraySort(string col, BinaryFunction comparator)
    {
        return ArraySort(Col(col), comparator);
    }

    public static Column ForAll(Column col, UnaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);
        return new Column(CreateExpression("forall", false, col, expression));
    }

    public static Column ForAll(string col, UnaryFunction function)
    {
        return ForAll(Col(col), function);
    }

    public static Column Transform(Column col, UnaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);
        return new Column(CreateExpression("transform", false, col, expression));
    }

    public static Column Transform(Column col, BinaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);
        return new Column(CreateExpression("transform", false, col, expression));
    }

    public static Column Transform(string col, UnaryFunction function)
    {
        return Transform(Col(col), function);
    }

    public static Column Transform(string col, BinaryFunction function)
    {
        return Transform(Col(col), function);
    }

    public static Column Exists(Column col, UnaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);
        return new Column(CreateExpression("exists", false, col, expression));
    }

    public static Column Exists(string col, UnaryFunction function)
    {
        return Exists(Col(col), function);
    }

    public static Column Filter(Column col, UnaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);
        return new Column(CreateExpression("filter", false, col, expression));
    }

    public static Column Filter(string col, UnaryFunction function)
    {
        return Filter(Col(col), function);
    }

    public static Column Filter(Column col, BinaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);
        return new Column(CreateExpression("filter", false, col, expression));
    }

    public static Column Filter(string col, BinaryFunction function)
    {
        return Filter(Col(col), function);
    }

    public static Column Aggregate(Column col, Column initialValue, BinaryFunction merge, UnaryFunction finish)
    {
        var sparkLambdaFunction = new CallableHelper();
        var mergeExpression = sparkLambdaFunction.GetLambdaExpression(merge);
        var finishExpression = sparkLambdaFunction.GetLambdaExpression(finish);

        return new Column(CreateExpression("aggregate", false, col, initialValue.Expression, mergeExpression,
            finishExpression));
    }

    public static Column Aggregate(string col, Column initialValue, BinaryFunction merge, UnaryFunction finish)
    {
        return Aggregate(Col(col), initialValue, merge, finish);
    }

    public static Column Aggregate(Column col, Column initialValue, BinaryFunction merge)
    {
        var sparkLambdaFunction = new CallableHelper();
        var mergeExpression = sparkLambdaFunction.GetLambdaExpression(merge);

        return new Column(CreateExpression("aggregate", false, col, initialValue.Expression, mergeExpression));
    }

    public static Column Aggregate(string col, Column initialValue, BinaryFunction merge)
    {
        return Aggregate(Col(col), initialValue, merge);
    }

    //
    public static Column Reduce(Column col, Column initialValue, BinaryFunction merge, UnaryFunction finish)
    {
        var sparkLambdaFunction = new CallableHelper();
        var mergeExpression = sparkLambdaFunction.GetLambdaExpression(merge);
        var finishExpression = sparkLambdaFunction.GetLambdaExpression(finish);

        return new Column(CreateExpression("reduce", false, col, initialValue.Expression, mergeExpression,
            finishExpression));
    }

    public static Column Reduce(string col, Column initialValue, BinaryFunction merge, UnaryFunction finish)
    {
        return Reduce(Col(col), initialValue, merge, finish);
    }

    public static Column Reduce(Column col, Column initialValue, BinaryFunction merge)
    {
        var sparkLambdaFunction = new CallableHelper();
        var mergeExpression = sparkLambdaFunction.GetLambdaExpression(merge);

        return new Column(CreateExpression("reduce", false, col, initialValue.Expression, mergeExpression));
    }

    public static Column Reduce(string col, Column initialValue, BinaryFunction merge)
    {
        return Reduce(Col(col), initialValue, merge);
    }


    public static Column ZipWith(Column left, Column right, BinaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);

        return new Column(CreateExpression("zip_with", false, left.Expression, right.Expression, expression));
    }

    public static Column ZipWith(string left, string right, BinaryFunction function)
    {
        return ZipWith(Col(left), Col(right), function);
    }

    public static Column TransformKeys(Column col, BinaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);

        return new Column(CreateExpression("transform_keys", false, col.Expression, expression));
    }

    public static Column TransformKeys(string col, BinaryFunction function)
    {
        return TransformKeys(Col(col), function);
    }


    public static Column TransformValues(Column col, BinaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);

        return new Column(CreateExpression("transform_values", false, col.Expression, expression));
    }

    public static Column TransformValues(string col, BinaryFunction function)
    {
        return TransformValues(Col(col), function);
    }

    public static Column MapFilter(Column col, BinaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);

        return new Column(CreateExpression("map_filter", false, col.Expression, expression));
    }

    public static Column MapFilter(string col, BinaryFunction function)
    {
        return MapFilter(Col(col), function);
    }

    public static Column MapZipWith(Column col1, Column col2, TernaryFunction function)
    {
        var sparkLambdaFunction = new CallableHelper();
        var expression = sparkLambdaFunction.GetLambdaExpression(function);

        return new Column(CreateExpression("map_zip_with", false, col1.Expression, col2.Expression, expression));
    }

    public static Column MapZipWith(string col1, string col2, TernaryFunction function)
    {
        return MapZipWith(Col(col1), Col(col2), function);
    }

    /// <Summary>ToTimestamp</Summary>
    public static Column TryToTimestamp(string col)
    {
        return new Column(CreateExpression("try_to_timestamp", false, col));
    }

    /// <Summary>ToTimestamp</Summary>
    public static Column TryToTimestamp(Column col)
    {
        return new Column(CreateExpression("try_to_timestamp", false, col));
    }

    /// <Summary>
    ///     ToTimestamp
    /// </Summary>
    public static Column TryToTimestamp(string col, string format)
    {
        return new Column(CreateExpression("try_to_timestamp", false, Col(col), Lit(format)));
    }

    /// <Summary>
    ///     ToTimestamp
    /// </Summary>
    public static Column TryToTimestamp(Column col, string format)
    {
        return new Column(CreateExpression("try_to_timestamp", false, col, Lit(format)));
    }

    /// <Summary>
    ///     ToTimestamp
    /// </Summary>
    public static Column TryToTimestamp(Column col, Column format)
    {
        return new Column(CreateExpression("try_to_timestamp", false, col, format));
    }

    /// <Summary>
    ///     ToTimestamp
    /// </Summary>
    public static Column TryToTimestamp(string col, Column format)
    {
        return new Column(CreateExpression("try_to_timestamp", false, Col(col), format));
    }
    
    public static Column ParseJson(Column col)
    {
        return new Column(CreateExpression("parse_json", false, col));
    }
    
    public static Column TryParseJson(Column col)
    {
        return new Column(CreateExpression("try_parse_json", false, col));
    }

    public static Column VariantGet(string col, string path, string? targetType = null) => VariantGet(Col(col), Lit(path), targetType == null ? null : Lit(targetType));
    public static Column VariantGet(Column col, string path, string? targetType = null) => VariantGet(col, Lit(path), targetType == null ? null : Lit(targetType));

    public static Column VariantGet(Column col, Column path, Column? targetType = null)
    {
        if (Object.Equals(null, targetType))
        {
            return new Column(CreateExpression("variant_get", false, col, path));
        }
        
        return new Column(CreateExpression("variant_get", false, col, path, targetType));
    }
    
    public static Column TryVariantGet(string col, string path, string? targetType = null) => TryVariantGet(Col(col), Lit(path), targetType == null ? null : Lit(targetType));
    public static Column TryVariantGet(Column col, string path, string? targetType = null) => TryVariantGet(col, Lit(path), targetType == null ? null : Lit(targetType));

    public static Column TryVariantGet(Column col, Column path, Column? targetType = null)
    {
        if (Object.Equals(null, targetType))
        {
            return new Column(CreateExpression("try_variant_get", false, col, path));
        }
        
        return new Column(CreateExpression("try_variant_get", false, col, path, targetType));
    }

    public static Column IsVariantNull(string v) => IsVariantNull(Col(v)); 
    public static Column IsVariantNull(Column v)
    {
        return new Column(CreateExpression("is_variant_null", false, v));
    }
    
    public static Column SchemaOfVariant(string v) => SchemaOfVariant(Col(v));
    
    public static Column SchemaOfVariant(Column v)
    {
        return new Column(CreateExpression("schema_of_variant", false, v));
    }  
    
    public static Column SchemaOfVariantAgg(string v) => SchemaOfVariantAgg(Col(v));
    
    public static Column SchemaOfVariantAgg(Column v)
    {
        return new Column(CreateExpression("schema_of_variant_agg", false, v));
    }

    public static Column FromXml(Column col, StructType schema, Dictionary<string, string> options)
    {
        return FromXml(col, Lit(schema.Json()), options);
    }
    
    public static Column FromXml(Column col, Column schema, Dictionary<string, string> options)
    {
        return new Column(CreateExpression("from_xml", false, col, schema, Lit(options)));
    }

    public static Column SchemaOfXml(string xml, IDictionary<string, string>? options = null) => SchemaOfXml(Lit(xml), options);
    
    public static Column SchemaOfXml(Column xml, IDictionary<string, string>? options = null)
    {
        if (options == null)
        {
            return new Column(CreateExpression("schema_of_xml", false, xml));
        }
        
        return new Column(CreateExpression("schema_of_xml", false, xml, CreateMap(options)));
        
    }
    
    public static Column TryMod(string dividend, string divisor) => TryMod(Col(dividend), Col(divisor));
    
    public static Column TryMod(Column dividend, Column divisor) => new (CreateExpression("try_mod", false, dividend, divisor));
    
    /// <Summary>Ceil</Summary>
    public static Column Ceil(string col, int scale)
    {
        return new Column(CreateExpression("ceil", false, Col(col), Lit(scale)));
    }

    /// <Summary>Ceil</Summary>
    public static Column Ceil(Column col, Column scale)
    {
        return new Column(CreateExpression("ceil", false, col, scale));
    }
    
    /// <Summary>Ceiling</Summary>
    public static Column Ceiling(string col, int scale)
    {
        return new Column(CreateExpression("ceiling", false, Col(col), Lit(scale)));
    }

    /// <Summary>Ceiling</Summary>
    public static Column Ceiling(Column col, Column scale)
    {
        return new Column(CreateExpression("ceiling", false, col, scale));
    }
    
    /// <Summary>Floor</Summary>
    public static Column Floor(string col, int scale)
    {
        return new Column(CreateExpression("floor", false, Col(col), Lit(scale)));
    }

    /// <Summary>Floor</Summary>
    public static Column Floor(Column col, Column scale)
    {
        return new Column(CreateExpression("floor", false, col, scale));
    }


    public static Column Monthname(string col) => Monthname(Col(col));

    public static Column Monthname(Column col) => new(CreateExpression("monthname", false, col));
    
    
    public static Column Dayname(string col) => Dayname(Col(col));

    public static Column Dayname(Column col) => new(CreateExpression("dayname", false, col));

    public static Column TimestampDiff(string unit, string start, string end) => TimestampDiff(unit, Col(start), Col(end));
    public static Column TimestampDiff(string unit, Column start, Column end)
    {
        return new Column(CreateExpression("timestampdiff", false, Lit(unit), start, end));
    }
    
    public static Column TimestampAdd(string unit, int quantity, string ts) => TimestampAdd(unit, Lit(quantity), Col(ts));
    public static Column TimestampAdd(string unit, Column quantity, Column ts)
    {
        return new Column(CreateExpression("timestampadd", false, Lit(unit), quantity, ts));
    }

    public static Column SessionUser() => new Column(CreateExpression("session_user", false));

    
    public static Column Collate(string col, string collation) => Collate(Col(col), Lit(collation));
    public static Column Collate(Column col, Column collation)
    {
        return new Column(CreateExpression("collate", false, col, collation));
    }

    public static Column Collation(string col) => Collation(Col(col));
    public static Column Collation(Column col)
    {
        return new Column(CreateExpression("collation", false, col));
    }
}