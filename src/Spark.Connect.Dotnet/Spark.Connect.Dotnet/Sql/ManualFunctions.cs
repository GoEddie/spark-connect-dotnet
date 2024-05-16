using Apache.Arrow.Types;
using Google.Protobuf;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql.Types;
using StructType = Spark.Connect.Dotnet.Sql.Types.StructType;

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
        if (String.IsNullOrEmpty(value))
        {
            return new Column(new Expression
            {
                Literal = new Expression.Types.Literal
                {
                    Null = new DataType()
                    {
                        String = new DataType.Types.String()
                        {
                            
                        }
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

    public static Column Lit(object o)
    {
        if (o is null)
        {
            return new Column(new Expression
            {
                Literal = new Expression.Types.Literal
                {
                    Null = new DataType()
                }
            });
        }
        
        if (o.GetType().IsArray)
        {
            var lits = new List<Column>();
            foreach (var object_ in (o as Array))
            {
                lits.Add(Lit((object)object_));
            }
            
            return Functions.Array(lits.ToArray());
        }
        
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

    public static Column Expr(string expression)
    {
        return new Column(new Expression()
        {
            ExpressionString = new Expression.Types.ExpressionString()
            {
                Expression = expression
            }
        });
    }

    public static Column AddMonths(Column start, Column months)
    {
        return new Column(FunctionWrappedCall("add_months", false, start, months));
    }
    
    public static Column AddMonths(Column start, int months)  => AddMonths(start, Lit(months));
    
    public static Column AddMonths(string start, Column months)  => AddMonths(Col(start), months);
    public static Column AddMonths(string start, int months) => AddMonths(Col(start), Lit(months));

    public static Column ApproxCountDistinct(Column col, double rsd = 0.05F)
    {
        return new Column(FunctionWrappedCall("approx_count_distinct", false, col, rsd));
    }
    
    public static Column ApproxCountDistinct(string col, double rsd = 0.05F)
    {
        return new Column(FunctionWrappedCall("approx_count_distinct", false, Col(col), rsd));
    }
    
    public static Column ApproxCountDistinct(string col, Column rsd)
    {
        return new Column(FunctionWrappedCall("approx_count_distinct", false, Col(col), rsd));
    }
    
    public static Column ApproxCountDistinct(Column col, Column rsd)
    {
        return new Column(FunctionWrappedCall("approx_count_distinct", false, col, rsd));
    }

    public static Column ApproxPercentile(Column col, float percentage, long accuracy = 10000)
    {
        return new Column(FunctionWrappedCall("approx_percentile", false, col, Lit(percentage), Lit(accuracy)));
    }
    
    
    public static Column ApproxPercentile(Column col, float[] percentages, long accuracy = 10000)
    {
        return new Column(FunctionWrappedCall("approx_percentile", false, col, Lit(percentages), Lit(accuracy)));
    }

    public static Column ApproxPercentile(string col,  float percentage, long accuracy = 10000)
    {
        return new Column(FunctionWrappedCall("approx_percentile", false, Col(col), Lit(percentage), Lit(accuracy)));
    }
    
    public static Column ApproxPercentile(Column col, Column percentages, Column accuracy)
    {
        return new Column(FunctionWrappedCall("approx_percentile", false, col, percentages, accuracy));
    }
    
    public static Column ArrayJoin(string col, string delimiter, string? nullReplacement = null)
    {
        if (String.IsNullOrEmpty(nullReplacement))
        {
            return new Column(FunctionWrappedCall("array_join", false, Col(col), Lit(delimiter)));    
        }
        
        return new Column(FunctionWrappedCall("array_join", false, Col(col), Lit(delimiter), Lit(nullReplacement)));
    }
    
    public static Column ArrayJoin(Column col, string delimiter, string? nullReplacement = null)
    {
        if (String.IsNullOrEmpty(nullReplacement))
        {
            return new Column(FunctionWrappedCall("array_join", false, col, Lit(delimiter)));    
        }
        
        return new Column(FunctionWrappedCall("array_join", false, col, Lit(delimiter), Lit(nullReplacement)));
    }
    
    public static Column ArrayJoin(Column col, Column delimiter)
    {
        return new Column(FunctionWrappedCall("array_join", false, col, delimiter));    
    }
    
    public static Column ArrayJoin(Column col, Column delimiter, Column nullReplacement)
    {
        return new Column(FunctionWrappedCall("array_join", false, col, delimiter, nullReplacement));    
    }
    
    public static Column ArrayInsert(Column col, int pos, Column value)
    {
        return new Column(FunctionWrappedCall("array_insert", false, col, Lit(pos), value));
    }
    
    public static Column ArrayInsert(Column col, Column pos, Column value)
    {
        return new Column(FunctionWrappedCall("array_insert", false, col, pos, value));
    }
    
    public static Column ArrayRepeat(Column col, int count)
    {
        return new Column(FunctionWrappedCall("array_repeat", false, col, Lit(count)));
    }
    
    public static Column ArrayRepeat(Column col, Column count)
    {
        return new Column(FunctionWrappedCall("array_repeat", false, col, count));
    }
    
    public static Column ArrayRepeat(string col, int count)
    {
        return new Column(FunctionWrappedCall("array_repeat", false, Col(col), Lit(count)));
    }
    
    public static Column ArrayRepeat(string col, Column count)
    {
        return new Column(FunctionWrappedCall("array_repeat", false, Col(col), count));
    }

    public static Column AssertTrue(Column col, string? errorMessage = null)
    {
        return new Column(FunctionWrappedCall("assert_true", false, col, errorMessage));
    }
    
    public static Column BTrim(Column col, string? trim = null)
    {
        if (String.IsNullOrEmpty(trim))
        {
            return new Column(FunctionWrappedCall("btrim", false, col));    
        }
        
        return new Column(FunctionWrappedCall("btrim", false, col, trim));
    }
    
    public static Column BTrim(string col, string? trim = null)
    {
        if (String.IsNullOrEmpty(trim))
        {
            return new Column(FunctionWrappedCall("btrim", false, Col(col)));    
        }
        
        return new Column(FunctionWrappedCall("btrim", false, Col(col), trim));
    }

    public static Column Bucket(int numOfBuckets, Column col)
    {
        return new Column(FunctionWrappedCall("bucket", false, Lit(numOfBuckets), col));
    }
    
    public static Column Bucket(int numOfBuckets, string col)
    {
        return new Column(FunctionWrappedCall("bucket", false, Lit(numOfBuckets), Col(col)));
    }
    
    public static Column ConcatWs(string sep, params Column[] cols)
    {
        return new Column(FunctionWrappedCall("concat_ws", false, Lit(sep), cols.Select(p => p.Expression).ToArray()));
    }

    public static Column ConcatWs(string sep, params string[] cols)
    {
        return new Column(FunctionWrappedCall("concat_ws", false, Lit(sep), cols.Select(p => Col(p).Expression).ToArray()));
    }
    
    public static DataFrame Broadcast(DataFrame src)
    {
        return src.Hint("broadcast");
    }

    public static Column Conv(string col, int fromBase, int toBase) => Conv(Col(col), fromBase, toBase);
    
    public static Column Conv(Column col, int fromBase, int toBase)
    {
        return new Column(FunctionWrappedCall("conv", false, col, Lit(fromBase), Lit(toBase)));
    }

    public static Column ConvertTimezone(Column sourceTzColumn, Column targetTz, Column sourceTz)
    {
        if (object.ReferenceEquals(null, sourceTzColumn))
        {
            return new Column(FunctionWrappedCall("convert_timezone", false, targetTz, sourceTz));    
        }
        
        return new Column(FunctionWrappedCall("convert_timezone", false, sourceTzColumn, targetTz, sourceTz));
    }
    
    public static Column CreateMap(string cola, string colb)
    {
        return new Column(FunctionWrappedCall("map", false, cola, colb));
    }

    public static Column CreateMap(IDictionary<string, object> options)
    {
        var litOptions = new List<Column>();
        
        foreach (var option in options)
        {
            litOptions.Add(Lit(option.Key));
            litOptions.Add(Lit(option.Value));
        }
        return new Column(FunctionWrappedCall("map", false, litOptions.ToArray()));
    }
    
    public static Column CreateMap(Column cola, Column colb)
    {
        return new Column(FunctionWrappedCall("map", false, cola, colb));
    }
    
    public static Column DateAdd(string start, int days)
    {
        return new Column(FunctionWrappedCall("date_add", false, Col(start), Lit(days)));
    }
    
    public static Column DateAdd(Column start, int days)
    {
        return new Column(FunctionWrappedCall("date_add", false, start, Lit(days)));
    }
    
    public static Column DateSub(string start, int days)
    {
        return new Column(FunctionWrappedCall("date_sub", false, Col(start), Lit(days)));
    }
    
    public static Column DateSub(Column start, int days)
    {
        return new Column(FunctionWrappedCall("date_sub", false, start, Lit(days)));
    }
    
    public static Column DateSub(Column start, Column days)
    {
        return new Column(FunctionWrappedCall("date_sub", false, start, days));
    }
    
    public static Column DateSub(string start, Column days)
    {
        return new Column(FunctionWrappedCall("date_sub", false, Col(start), days));
    }
    
    public static Column DateTrunc(string format, Column timestamp)
    {
        return new Column(FunctionWrappedCall("date_trunc", false, Lit(format), timestamp));
    }
    public static Column DateTrunc(string format, string timestamp)
    {
        return new Column(FunctionWrappedCall("date_trunc", false, Lit(format), Col(timestamp)));
    }
    
    public static Column First(string col)
    {
        return new Column(FunctionWrappedCall("first", false, Col(col)));
    }
    
    public static Column First(Column col)
    {
        return new Column(FunctionWrappedCall("first", false, col));
    }
    
    public static Column Last(Column col)
    {
        return new Column(FunctionWrappedCall("last", false, col));
    }
    
    public static Column Last(string col)
    {
        return new Column(FunctionWrappedCall("last", false, Col(col)));
    }
    
    public static Column FormatString(string format, params Column[] cols)
    {
        var newList = new List<Column>();
        newList.Add(Lit(format));
        newList.AddRange(cols);
        return new Column(FunctionWrappedCall("format_string", false, newList.ToArray()));
    }
    
    public static Column FormatString(string format, params string[] cols)
    {
        var newList = new List<Column>();
        newList.Add(Lit(format));
        newList.AddRange(cols.Select(Col));
        return new Column(FunctionWrappedCall("format_string", false, newList.ToArray()));
    }

    public static Column FromCsv(Column col, Column ddlSchema, IDictionary<string, object>? options = null)
    {
        if (options == null)
        {
            return new Column(FunctionWrappedCall("from_csv", false, col, ddlSchema));    
        }

        var mappedOptions = CreateMap(options);
        return new Column(FunctionWrappedCall("from_csv", false, col, ddlSchema, mappedOptions));
    }
    
    public static Column FromJson(Column col, Column ddlSchema, IDictionary<string, object>? options = null)
    {
        if (options == null)
        {
            return new Column(FunctionWrappedCall("from_json", false, col, ddlSchema));    
        }

        var mappedOptions = CreateMap(options);
        return new Column(FunctionWrappedCall("from_json", false, col, ddlSchema, mappedOptions));
    }
    
    /// <summary>
    /// ddlSchema is a string with a DDL schema in such as "COLNAME INT, ANOTHERCOL STRING"
    /// </summary>
    /// <param name="col"></param>
    /// <param name="ddlSchema"></param>
    /// <param name="options"></param>
    /// <returns></returns>
    public static Column FromJson(Column col, string ddlSchema, IDictionary<string, object>? options = null)
    {
        if (options == null)
        {
            return new Column(FunctionWrappedCall("from_json", false, col, Lit(ddlSchema)));    
        }

        var mappedOptions = CreateMap(options);
        return new Column(FunctionWrappedCall("from_json", false, col, Lit(ddlSchema), mappedOptions));
    }
    
    /// <summary>
    /// Column with the json value in, a schema as a StructType, or ArrayType of StructType
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
            return new Column(FunctionWrappedCall("from_json", false, col, ddlSchema));    
        }

        var mappedOptions = CreateMap(options);
        return new Column(FunctionWrappedCall("from_json", false, col, ddlSchema, mappedOptions));
    }

    public static Column FromUtcTimestamp(string timestamp, string tz)
    {
        return new Column(FunctionWrappedCall("from_utc_timestamp", false, Lit(timestamp), Lit(tz)));
    }
    
    public static Column FromUtcTimestamp(Column timestamp, Column tz)
    {
        return new Column(FunctionWrappedCall("from_utc_timestamp", false, timestamp, tz));
    }

    public static Column Grouping(string col) => Grouping(Col(col));

    public static Column Grouping(Column col) => new Column(FunctionWrappedCall("grouping", false, col));

    public static Column JsonTuple(Column col, params string[] fields)
    {
        return new Column(FunctionWrappedCall("json_tuple", false, col, fields.Select(p => Lit(p).Expression).ToArray()));
    }
    
    public static Column JsonTuple(string col, params string[] fields)
    {
        return new Column(FunctionWrappedCall("json_tuple", false, Col(col), fields.Select(p => Lit(p).Expression).ToArray()));
    }

    public static Column Lag(Column col, int offset, object defaultValue)
    {
        return new Column(FunctionWrappedCall("lag", false, col, Lit(offset), Lit(defaultValue)));
    }
    
    public static Column Lag(string col, int offset, object defaultValue)
    {
        return new Column(FunctionWrappedCall("lag", false, Col(col), Lit(offset), Lit(defaultValue)));
    }
    
    public static Column Lag(Column col, int offset)
    {
        return new Column(FunctionWrappedCall("lag", false, col, Lit(offset)));
    }
    
    public static Column Lag(string col, int offset)
    {
        return new Column(FunctionWrappedCall("lag", false, Col(col), Lit(offset)));
    }

    
    public static Column Lag(Column col)
    {
        return new Column(FunctionWrappedCall("lag", false, col));
    }
    
    public static Column Lag(string col)
    {
        return new Column(FunctionWrappedCall("lag", false, Col(col)));
    }
    
    public static Column Lead(Column col, int offset, object defaultValue)
    {
        return new Column(FunctionWrappedCall("lead", false, col, Lit(offset), Lit(defaultValue)));
    }
    
    public static Column Lead(string col, int offset, object defaultValue)
    {
        return new Column(FunctionWrappedCall("lead", false, Col(col), Lit(offset), Lit(defaultValue)));
    }
    
    public static Column Lead(Column col, int offset)
    {
        return new Column(FunctionWrappedCall("lead", false, col, Lit(offset)));
    }
    
    public static Column Lead(string col, int offset)
    {
        return new Column(FunctionWrappedCall("lead", false, Col(col), Lit(offset)));
    }

    
    public static Column Lead(Column col)
    {
        return new Column(FunctionWrappedCall("lead", false, col));
    }
    
    public static Column Lead(string col)
    {
        return new Column(FunctionWrappedCall("lead", false, Col(col)));
    }

    public static Column Levenshtein(string left, string right, int? threshold = null) => Levenshtein(Col(left), Col(right), threshold);
    public static Column Levenshtein(Column left, Column right, int? threshold = null)
    {
        if (!threshold.HasValue)
        {
            return new Column(FunctionWrappedCall("levenshtein", false, left, right));
        }
        
        return new Column(FunctionWrappedCall("levenshtein", false, left, right, Lit(threshold.Value)));
    }

    public static Column Like(string col, string pattern, string? escape = null) => Like(Col(col), Lit(pattern), escape == null ? null : Lit(escape));

    public static Column Like(Column col, Column pattern, Column? escape = null)
    {
        if (Object.Equals(null, escape))
        {
            return new Column(FunctionWrappedCall("like", false, col, pattern));
        }
        
        return new Column(FunctionWrappedCall("like", false, col, pattern, escape));
    }

    /// <summary>
    /// Find the occurence of substr in col - the PySpark docs say that pos is 0-based but you need to use 1 for the first char
    /// </summary>
    /// <param name="substr"></param>
    /// <param name="col"></param>
    /// <param name="pos"></param>
    /// <returns>Column</returns>
    public static Column Locate(string substr, string col, int? pos = null) => Locate(substr, Col(col), pos);
    
    /// <summary>
    /// Find the occurence of substr in col - the PySpark docs say that pos is 0-based but you need to use 1 for the first char
    /// </summary>
    /// <param name="substr"></param>
    /// <param name="col"></param>
    /// <param name="pos"></param>
    /// <returns>Column</returns>
    public static Column Locate(string substr, Column col, int? pos = null) => Locate(Lit(substr), col, pos);
    
    /// <summary>
    /// Find the occurence of substr in col - the PySpark docs say that pos is 0-based but you need to use 1 for the first char
    /// </summary>
    /// <param name="substr"></param>
    /// <param name="col"></param>
    /// <param name="pos"></param>
    /// <returns>Column</returns>
    public static Column Locate(Column substr, Column col, int? pos = null)
    {
        if (pos.HasValue)
        {
            return new Column(FunctionWrappedCall("locate", false, substr, col, Lit(pos.Value)));    
        }
        
        return new Column(FunctionWrappedCall("locate", false, substr, col));
    }

    public static Column LPad(string col, int len, string pad) => LPad(Col(col), len, pad);
    
    public static Column LPad(Column col, int len, string pad) => new (FunctionWrappedCall("lpad", false, col, Lit(len), Lit(pad)));

    public static Column MakeDtInterval() => new(FunctionWrappedCall("make_dt_interval", false));

    public static Column MakeDtInterval(string day) => MakeDtInterval(Col(day));

    public static Column MakeDtInterval(Column day) => new(FunctionWrappedCall("make_dt_interval", false, day));
    
    public static Column MakeDtInterval(string day, string hour) => MakeDtInterval(Col(day), Col(hour));
    
    public static Column MakeDtInterval(Column day, Column hour) => new(FunctionWrappedCall("make_dt_interval", false, day, hour));
    
    public static Column MakeDtInterval(string day, string hour, string minute) => MakeDtInterval(Col(day), Col(hour), Col(minute));
    
    public static Column MakeDtInterval(Column day, Column hour, Column minute) => new(FunctionWrappedCall("make_dt_interval", false, day, hour, minute));
    
    public static Column MakeDtInterval(string day, string hour, string minute, double seconds) => MakeDtInterval(Col(day), Col(hour), Col(minute));
    
    public static Column MakeDtInterval(Column day, Column hour, Column minute, Column seconds) => new(FunctionWrappedCall("make_dt_interval", false, day, hour, minute, seconds));

    public static Column MakeTimestamp(string years, string months, string days, string hours, string mins, string secs,
        string? timezone = null) => MakeTimestamp(Col(years), Col(months), Col(days), Col(hours), Col(mins), Col(secs),
        timezone == null ? null : Col(timezone));

    public static Column MakeTimestamp(Column years, Column months, Column days, Column hours, Column mins, Column secs, Column? timezone = null)
    {
        if (Object.Equals(null, timezone))
        {
            return new(FunctionWrappedCall("make_timestamp", false, years, months, days, hours, mins, secs ));
        }
        
        return new(FunctionWrappedCall("make_timestamp", false, years, months, days, hours, mins, secs , timezone));
    }
    
    public static Column MakeTimestampLtz(string years, string months, string days, string hours, string mins, string secs,
        string? timezone = null) => MakeTimestampLtz(Col(years), Col(months), Col(days), Col(hours), Col(mins), Col(secs),
        timezone == null ? null : Col(timezone));

    public static Column MakeTimestampLtz(Column years, Column months, Column days, Column hours, Column mins, Column secs, Column? timezone = null)
    {
        if (Object.Equals(null, timezone))
        {
            return new(FunctionWrappedCall("make_timestamp_ltz", false, years, months, days, hours, mins, secs ));
        }
        
        return new(FunctionWrappedCall("make_timestamp_ltz", false, years, months, days, hours, mins, secs , timezone));
    }
    
    /// <summary>
    /// col is the name of the column to apply the mask to
    /// </summary>
    /// <param name="col"></param>
    /// <param name="upperChar"></param>
    /// <param name="lowerChar"></param>
    /// <param name="digitChar"></param>
    /// <param name="otherChar"></param>
    /// <returns></returns>
    public static Column Mask(string col, string upperChar=null, string lowerChar=null, string digitChar=null, string otherChar=null)
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
    /// To use the default specify null instead of a column
    /// </summary>
    /// <param name="col"></param>
    /// <param name="upperChar"></param>
    /// <param name="lowerChar"></param>
    /// <param name="digitChar"></param>
    /// <param name="otherChar"></param>
    /// <returns></returns>
    public static Column Mask(Column col, Column upperChar=null, Column lowerChar=null, Column digitChar=null, Column otherChar=null)
    {
        if (Object.Equals(null, upperChar))
        {
            upperChar = Lit("X");
        }
        
        if (Object.Equals(null, lowerChar))
        {
            lowerChar = Lit("x");
        }
        
        if (Object.Equals(null, digitChar))
        {
            digitChar = Lit("n");
        }
        
        if (Object.Equals(null, otherChar))
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
        
        return new Column(FunctionWrappedCall("mask", false, col, upperChar, lowerChar, digitChar, otherChar));
    }

    public static Column MonthsBetween(string date1Col, string date2Col, bool? roundOff = null) => MonthsBetween(Col(date1Col), Col(date2Col), roundOff);
    
    public static Column MonthsBetween(Column date1Col, Column date2Col, bool? roundOff = null)
    {
        if (roundOff.HasValue)
        {
            return new Column(FunctionWrappedCall("months_between", false, date1Col, date2Col, Lit(roundOff.Value)));    
        }
        
        return new Column(FunctionWrappedCall("months_between", false, date1Col, date2Col));
    }

    public static Column NthValue(string col, int offset, bool? ignoreNulls = null) => NthValue(Col(col), offset, ignoreNulls);
    
    public static Column NthValue(Column col, int offset, bool? ignoreNulls = null)  => NthValue(col, Lit(offset), ignoreNulls);

    public static Column NthValue(Column col, Column offset, bool? ignoreNulls = null)
    {
        if (ignoreNulls.HasValue)
        {
            return new Column(FunctionWrappedCall("nth_value", false, col, offset, Lit(ignoreNulls.Value)));  
        }
        
        return new Column(FunctionWrappedCall("nth_value", false, col, offset));
    }
    
    public static Column Ntile(int n) => new Column(FunctionWrappedCall("ntile", false, Lit(n)));


    public static Column Overlay(string src, string replace, int pos, int? len = null) => Overlay(Col(src), Col(replace), Lit(pos), len.HasValue ? Lit(len.Value) : null);
    
    public static Column Overlay(Column src, Column replace, int pos, int? len = null) => Overlay(src, replace, Lit(pos), len.HasValue ? Lit(len.Value) : null);

    public static Column Overlay(Column src, Column replace, Column pos, Column? len = null)
    {
        if (Object.Equals(null, len))
        {
            return new Column(FunctionWrappedCall("overlay", false, src, replace, pos));
        }
        
        return new Column(FunctionWrappedCall("overlay", false, src, replace, pos, len));
    }

    public static Column Percentile(string col, float[] percentage, int frequency) => Percentile(Col(col), Lit(percentage), Lit(frequency));
    
    public static Column Percentile(string col, float percentage, int frequency) => Percentile(Col(col), Lit(percentage), Lit(frequency));
    
    public static Column Percentile(Column col, float[] percentage, int frequency) => Percentile(col, Lit(percentage), Lit(frequency));
    
    public static Column Percentile(Column col, float percentage, int frequency) => Percentile(col, Lit(percentage), Lit(frequency));

    public static Column Percentile(Column col, Column percentage, Column frequency)
    {
        return new Column(FunctionWrappedCall("percentile", false, col, percentage, frequency));
    }
    
    public static Column PercentileApprox(string col, float[] percentage, int accuracy) => Percentile(Col(col), Lit(percentage), Lit(accuracy));
    
    public static Column PercentileApprox(string col, float percentage, int accuracy) => Percentile(Col(col), Lit(percentage), Lit(accuracy));
    
    public static Column PercentileApprox(Column col, float[] percentage, int accuracy) => Percentile(col, Lit(percentage), Lit(accuracy));
    
    public static Column PercentileApprox(Column col, float percentage, int accuracy) => Percentile(col, Lit(percentage), Lit(accuracy));

    public static Column PercentileApprox(Column col, Column percentage, Column accuracy)
    {
        return new Column(FunctionWrappedCall("percentile_approx", false, col, percentage, accuracy));
    }

    public static Column ParseUrl(string urlCol, string partToExtractCol, string? keyCol = null) => ParseUrl(Col(urlCol), Col(partToExtractCol), keyCol == null ? null : Col(keyCol));

    public static Column ParseUrl(Column col, Column partToExtract, Column? key = null)
    {
        if (Object.Equals(null, key))
        {
            return new Column(FunctionWrappedCall("parse_url", false, col, partToExtract));
        }
        
        return new Column(FunctionWrappedCall("parse_url", false, col, partToExtract, key));
    }

    public static Column Position(string substrColumn, string strColumn, string? startColumn = null) => Position(Col(substrColumn), Col(strColumn), startColumn == null ? null : Col(startColumn));
    
    public static Column Position(Column substrColumn, Column strColumn, Column? startColumn = null)
    {
        if (Object.Equals(null, startColumn))
        {
            return new Column(FunctionWrappedCall("position", false, substrColumn, strColumn));
        }
        
        return new Column(FunctionWrappedCall("position", false, substrColumn, strColumn, startColumn));
    }

    public static Column PrintF(string format, params string[] cols) => PrintF(Col(format), cols.Select(Col).ToArray());
    public static Column PrintF(Column format, params Column[] cols)
    {
        return new Column(FunctionWrappedCall("printf", false, cols.Prepend(format).ToArray()));
    }
    
    public static Column RaiseError(string message) => new Column(FunctionWrappedCall("raise_error", false, Lit(message)));
    public static Column RaiseError(Column message) => new Column(FunctionWrappedCall("raise_error", false, message));
    
    // substrColumn or str
    //     A column of string, substring.
    //
    //     strColumn or str
    //     A column of string.
    //
    // startColumn or str, optional
    //     A column of string, start position.

    /// <Summary>
    ///     Round
    ///     Round the given value to `scale` default scale = 0
    /// </Summary>
    public static Column Round(string col)
    {
        return new Column(FunctionWrappedCall("round", false, Col(col)));
    }
}