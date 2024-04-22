using Google.Protobuf;

namespace Spark.Connect.Dotnet.Sql;

public partial class Functions : FunctionsWrapper
{
    private static readonly DateOnly UnixEpoch = new(1970, 1, 1);
    private static readonly TimeSpan UnixEpochTimespan = new(1970, 1, 1);

    public static Column ObjectToLit(object o)
    {
        return o switch
        {
            int i => Lit(i),
            string s => Lit(s),
            bool b => Lit(b),
            double d => Lit(d)
        };
    }

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
                    },
                    Values =
                    {
                        dict.Values.Select(p => new Expression.Types.Literal
                        {
                            Float = p
                        })
                    },

                    KeyType = new DataType
                    {
                        String = new DataType.Types.String()
                    },
                    ValueType = new DataType
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
                        },
                        Values =
                        {
                            dict.Values.Select(p => new Expression.Types.Literal
                            {
                                Double = p
                            })
                        },

                        KeyType = new DataType
                        {
                            String = new DataType.Types.String()
                        },
                        ValueType = new DataType
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
                    },
                    Values =
                    {
                        dict.Values.Select(p => new Expression.Types.Literal
                        {
                            String = p
                        })
                    },

                    KeyType = new DataType
                    {
                        String = new DataType.Types.String()
                    },
                    ValueType = new DataType
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
                    },
                    Values =
                    {
                        dict.Values.Select(p => new Expression.Types.Literal
                        {
                            Integer = p
                        })
                    },

                    KeyType = new DataType
                    {
                        String = new DataType.Types.String()
                    },
                    ValueType = new DataType
                    {
                        Integer = new DataType.Types.Integer()
                    }
                }
            }
        });
    }

    public static Column Lit(string value)
    {
        return new Column(new Expression
        {
            Literal = new Expression.Types.Literal
            {
                String = value
            }
        });
    }

    public static Column Lit(object o)
    {
        return o switch
        {
            int i => Lit(i),
            string s => Lit(s),
            double d => Lit(d),
            float f => Lit(f),
            short s => Lit(s),
            long l => Lit(l),

            _ => Lit(o.ToString()) //TODO not great
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
                FunctionName = "array",
                IsDistinct = false,
                Arguments =
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
                FunctionName = "array",
                IsDistinct = false,
                Arguments =
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
                FunctionName = "array",
                IsDistinct = false,
                Arguments =
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


    /// <param name="cols">List&lt;String&gt;</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static Column CountDistinct(List<string> cols)
    {
        return new Column(FunctionWrappedCall("count", true, cols.ToArray()));
    }

    /// <param name="cols">List&lt;Column&gt;</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static Column CountDistinct(List<Column> cols)
    {
        return new Column(FunctionWrappedCall("count", true, cols.ToArray()));
    }

    /// <param name="col">String</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static Column CountDistinct(string col)
    {
        return new Column(FunctionWrappedCall("count", true, col));
    }

    /// <param name="col">Column</param>
    /// <Summary>Returns a new :class:`Column` for distinct count of ``col`` or ``cols``.</Summary>
    public static Column CountDistinct(Column col)
    {
        return new Column(FunctionWrappedCall("count", true, col));
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
        return new Column(FunctionWrappedCall("date_part", false, field, source));
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
        return new Column(FunctionWrappedCall("date_part", false, field, Column(source)));
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
        return new Column(FunctionWrappedCall("date_part", false, field, source));
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
        return new Column(FunctionWrappedCall("date_part", false, field, Column(source)));
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
        return new Column(FunctionWrappedCall("extract", false, field, source));
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
        return new Column(FunctionWrappedCall("extract", false, field, Column(source)));
    }


    /// <Summary>
    ///     TryToNumber
    ///     Convert string 'col' to a number based on the string format `format`. Returns NULL if the string 'col' does not
    ///     match the expected format. The format follows the same semantics as the to_number function.
    /// </Summary>
    public static Column TryToNumber(string col, string format)
    {
        return new Column(FunctionWrappedCall("try_to_number", false, Col(col), Lit(format)));
    }

    /// <Summary>
    ///     TryToNumber
    ///     Convert string 'col' to a number based on the string format `format`. Returns NULL if the string 'col' does not
    ///     match the expected format. The format follows the same semantics as the to_number function.
    /// </Summary>
    public static Column TryToNumber(Column col, string format)
    {
        return new Column(FunctionWrappedCall("try_to_number", false, col, Lit(format)));
    }

    /// <Summary>
    ///     TryToNumber
    ///     Convert string 'col' to a number based on the string format `format`. Returns NULL if the string 'col' does not
    ///     match the expected format. The format follows the same semantics as the to_number function.
    /// </Summary>
    public static Column TryToNumber(Column col, Column format)
    {
        return new Column(FunctionWrappedCall("try_to_number", false, col, format));
    }

    /// <Summary>
    ///     TryElementAt
    ///     (array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will throw an error. If
    ///     index lt; 0, accesses elements from the last to the first. The function always returns NULL if the index exceeds
    ///     the length of the array.
    /// </Summary>
    public static Column TryElementAt(Column col, Column extraction)
    {
        return new Column(FunctionWrappedCall("try_element_at", false, col, extraction));
    }

    /// <Summary>
    ///     TryElementAt
    ///     (array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will throw an error. If
    ///     index lt; 0, accesses elements from the last to the first. The function always returns NULL if the index exceeds
    ///     the length of the array.
    /// </Summary>
    public static Column TryElementAt(string col, Column extraction)
    {
        return new Column(FunctionWrappedCall("try_element_at", false, Col(col), extraction));
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
        return new Column(FunctionWrappedCall("to_varchar", false, Col(col), Lit(format)));
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
        return new Column(FunctionWrappedCall("to_varchar", false, col, Lit(format)));
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
        return new Column(FunctionWrappedCall("to_varchar", false, col, format));
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
        return new Column(FunctionWrappedCall("split_part", false, Col(src), delimiter, partNum));
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
        return new Column(FunctionWrappedCall("split_part", false, src, delimiter, partNum));
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
        return new Column(FunctionWrappedCall("split_part", false, Col(src), Col(delimiter), Col(partNum)));
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
        return new Column(FunctionWrappedCall("histogram_numeric", false, Col(col), nBins));
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
        return new Column(FunctionWrappedCall("histogram_numeric", false, col, nBins));
    }

    /// <Summary>
    ///     Sha2
    ///     Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512). The
    ///     numBits indicates the desired bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which
    ///     is equivalent to 256).
    /// </Summary>
    public static Column Sha2(string col, Column numBits)
    {
        return new Column(FunctionWrappedCall("sha2", false, col, numBits));
    }

    /// <Summary>
    ///     Sha2
    ///     Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512). The
    ///     numBits indicates the desired bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which
    ///     is equivalent to 256).
    /// </Summary>
    public static Column Sha2(Column col, Column numBits)
    {
        return new Column(FunctionWrappedCall("sha2", false, col, numBits));
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
        return new Column(FunctionWrappedCall("atan2", false, Col(col1), Col(col2)));
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
        return new Column(FunctionWrappedCall("atan2", false, col1, col2));
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
        return new Column(FunctionWrappedCall("atan2", false, col1, Col(col2)));
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
        return new Column(FunctionWrappedCall("atan2", false, Col(col1), col2));
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
        return new Column(FunctionWrappedCall("atan2", false, Lit(col1), Lit(col2)));
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
        return new Column(FunctionWrappedCall("atan2", false, col1, Lit(col2)));
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
        return new Column(FunctionWrappedCall("atan2", false, Lit(col1), col2));
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
        return new Column(FunctionWrappedCall("atan2", false, Col(col1), Lit(col2)));
    }

    /// <Summary>
    ///     Reflect
    ///     Calls a method with reflection.
    /// </Summary>
    public static Column Reflect(params Column[] cols)
    {
        return new Column(FunctionWrappedCall("reflect", false, cols));
    }

    /// <Summary>
    ///     Reflect
    ///     Calls a method with reflection.
    /// </Summary>
    public static Column Reflect(params string[] cols)
    {
        return new Column(FunctionWrappedCall("reflect", false, cols.Select(Col).ToArray()));
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
        return new Column(FunctionWrappedCall("when", false, condition, value));
    }

    public static Column When(Column condition, object value)
    {
        return new Column(FunctionWrappedCall("when", false, condition, Lit(value)));
    }
}