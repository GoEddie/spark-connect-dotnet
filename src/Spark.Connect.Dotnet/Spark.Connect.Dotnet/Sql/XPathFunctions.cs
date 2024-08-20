namespace Spark.Connect.Dotnet.Sql;

public partial class Functions : FunctionsWrapper
{
    /// <Summary>
    ///     Xpath
    ///     Returns a string array of values within the nodes of xml that match the XPath expression.
    /// </Summary>
    public static Column Xpath(string xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath", false, Col(xml), Lit(path)));
    }

    /// <Summary>
    ///     Xpath
    ///     Returns a string array of values within the nodes of xml that match the XPath expression.
    /// </Summary>
    public static Column Xpath(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath", false, Col(xml), path));
    }

    /// <Summary>
    ///     Xpath
    ///     Returns a string array of values within the nodes of xml that match the XPath expression.
    /// </Summary>
    public static Column Xpath(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath", false, xml, path));
    }

    /// <Summary>
    ///     XpathBoolean
    ///     Returns true if the XPath expression evaluates to true, or if a matching node is found.
    /// </Summary>
    public static Column XpathBoolean(string xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath_boolean", false, Col(xml), Lit(path)));
    }

    /// <Summary>
    ///     XpathBoolean
    ///     Returns true if the XPath expression evaluates to true, or if a matching node is found.
    /// </Summary>
    public static Column XpathBoolean(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_boolean", false, Col(xml), path));
    }


    /// <Summary>
    ///     XpathBoolean
    ///     Returns true if the XPath expression evaluates to true, or if a matching node is found.
    /// </Summary>
    public static Column XpathBoolean(Column xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath_boolean", false, xml, Lit(path)));
    }


    /// <Summary>
    ///     XpathBoolean
    ///     Returns true if the XPath expression evaluates to true, or if a matching node is found.
    /// </Summary>
    public static Column XpathBoolean(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_boolean", false, xml, path));
    }

    /// <Summary>
    ///     XpathDouble
    ///     Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathDouble(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_double", false, Col(xml), path));
    }

    /// <Summary>
    ///     XpathDouble
    ///     Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathDouble(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_double", false, xml, path));
    }

    /// <Summary>
    ///     XpathNumber
    ///     Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathNumber(string xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath_number", false, Col(xml), Lit(path)));
    }

    /// <Summary>
    ///     XpathNumber
    ///     Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathNumber(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_number", false, xml, path));
    }

    /// <Summary>
    ///     XpathNumber
    ///     Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathNumber(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_number", false, Col(xml), path));
    }

    /// <Summary>
    ///     XpathFloat
    ///     Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathFloat(string xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath_float", false, Col(xml), Lit(path)));
    }

    /// <Summary>
    ///     XpathFloat
    ///     Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathFloat(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_float", false, xml, path));
    }

    /// <Summary>
    ///     XpathFloat
    ///     Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathFloat(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_float", false, Col(xml), path));
    }

    /// <Summary>
    ///     XpathInt
    ///     Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    public static Column XpathInt(string xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath_int", false, Col(xml), Lit(path)));
    }

    /// <Summary>
    ///     XpathInt
    ///     Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    public static Column XpathInt(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_int", false, xml, path));
    }

    /// <Summary>
    ///     XpathInt
    ///     Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    public static Column XpathInt(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_int", false, Col(xml), path));
    }

    /// <Summary>
    ///     XpathLong
    ///     Returns a long integer value, or the value zero if no match is found, or a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathLong(string xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath_long", false, Col(xml), Lit(path)));
    }

    /// <Summary>
    ///     XpathLong
    ///     Returns a long integer value, or the value zero if no match is found, or a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathLong(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_long", false, xml, path));
    }

    /// <Summary>
    ///     XpathLong
    ///     Returns a long integer value, or the value zero if no match is found, or a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathLong(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_long", false, Col(xml), path));
    }

    /// <Summary>
    ///     XpathShort
    ///     Returns a short integer value, or the value zero if no match is found, or a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathShort(string xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath_short", false, Col(xml), Lit(path)));
    }

    /// <Summary>
    ///     XpathShort
    ///     Returns a short integer value, or the value zero if no match is found, or a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathShort(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_short", false, xml, path));
    }

    /// <Summary>
    ///     XpathShort
    ///     Returns a short integer value, or the value zero if no match is found, or a match is found but the value is
    ///     non-numeric.
    /// </Summary>
    public static Column XpathShort(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_short", false, Col(xml), path));
    }

    /// <Summary>
    ///     XpathString
    ///     Returns the text contents of the first xml node that matches the XPath expression.
    /// </Summary>
    public static Column XpathString(string xml, string path)
    {
        return new Column(FunctionWrappedCall("xpath_string", false, Col(xml), Lit(path)));
    }

    /// <Summary>
    ///     XpathString
    ///     Returns the text contents of the first xml node that matches the XPath expression.
    /// </Summary>
    public static Column XpathString(Column xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_string", false, xml, path));
    }

    /// <Summary>
    ///     XpathString
    ///     Returns the text contents of the first xml node that matches the XPath expression.
    /// </Summary>
    public static Column XpathString(string xml, Column path)
    {
        return new Column(FunctionWrappedCall("xpath_string", false, Col(xml), path));
    }
}