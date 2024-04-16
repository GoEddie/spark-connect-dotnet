namespace Spark.Connect.Dotnet.Sql;

public partial class Functions : FunctionsWrapper
{
    
    /// <Summary>
    /// Xpath
    /// Returns a string array of values within the nodes of xml that match the XPath expression.
    /// </Summary>
    public static SparkColumn Xpath(string xml,string path) => new(FunctionWrappedCall("xpath", false, Col(xml), Lit(path)));

    /// <Summary>
    /// Xpath
    /// Returns a string array of values within the nodes of xml that match the XPath expression.
    /// </Summary>
    public static SparkColumn Xpath(string xml, Expression path) => new(FunctionWrappedCall("xpath", false, Col(xml),path));

    /// <Summary>
    /// Xpath
    /// Returns a string array of values within the nodes of xml that match the XPath expression.
    /// </Summary>
    public static SparkColumn Xpath(SparkColumn xml, Expression path) => new(FunctionWrappedCall("xpath", false, xml,path));

    /// <Summary>
    /// XpathBoolean
    /// Returns true if the XPath expression evaluates to true, or if a matching node is found.
    /// </Summary>
    public static SparkColumn XpathBoolean(string xml,string path) => new(FunctionWrappedCall("xpath_boolean", false, Col(xml),Lit(path)));
    
    /// <Summary>
    /// XpathBoolean
    /// Returns true if the XPath expression evaluates to true, or if a matching node is found.
    /// </Summary>
    public static SparkColumn XpathBoolean(string xml, Expression path) => new(FunctionWrappedCall("xpath_boolean", false, Col(xml),path));

    
    /// <Summary>
    /// XpathBoolean
    /// Returns true if the XPath expression evaluates to true, or if a matching node is found.
    /// </Summary>
    public static SparkColumn XpathBoolean(SparkColumn xml,string path) => new(FunctionWrappedCall("xpath_boolean", false, xml,Lit(path)));

    
    /// <Summary>
    /// XpathBoolean
    /// Returns true if the XPath expression evaluates to true, or if a matching node is found.
    /// </Summary>
	public static SparkColumn XpathBoolean(SparkColumn xml,Expression path) => new(FunctionWrappedCall("xpath_boolean", false, xml,path));
    
    /// <Summary>
    /// XpathDouble
    /// Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathDouble(string xml, Expression path) => new(FunctionWrappedCall("xpath_double", false, Col(xml),path));

    /// <Summary>
    /// XpathDouble
    /// Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathDouble(SparkColumn xml,Expression path) => new(FunctionWrappedCall("xpath_double", false, xml,path));

    /// <Summary>
    /// XpathNumber
    /// Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    /// </Summary>
    public static SparkColumn XpathNumber(string xml,string path) => new(FunctionWrappedCall("xpath_number", false, Col(xml),Lit(path)));

    /// <Summary>
    /// XpathNumber
    /// Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathNumber(SparkColumn xml, Expression path) => new(FunctionWrappedCall("xpath_number", false, xml,path));

    /// <Summary>
    /// XpathNumber
    /// Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathNumber(string xml, Expression path) => new(FunctionWrappedCall("xpath_number", false, Col(xml), path));

    /// <Summary>
    /// XpathFloat
    /// Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    /// </Summary>
    public static SparkColumn XpathFloat(string xml,string path) => new(FunctionWrappedCall("xpath_float", false, Col(xml), Lit(path)));

    /// <Summary>
    /// XpathFloat
    /// Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathFloat(SparkColumn xml,Expression path) => new(FunctionWrappedCall("xpath_float", false, xml, path));

    /// <Summary>
    /// XpathFloat
    /// Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathFloat(string xml, Expression path) => new(FunctionWrappedCall("xpath_float", false, Col(xml), path));

    /// <Summary>
    /// XpathInt
    /// Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    public static SparkColumn XpathInt(string xml,string path) => new(FunctionWrappedCall("xpath_int", false, Col(xml), Lit(path)));

    /// <Summary>
    /// XpathInt
    /// Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathInt(SparkColumn xml,Expression path) => new(FunctionWrappedCall("xpath_int", false, xml,path));

    /// <Summary>
    /// XpathInt
    /// Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathInt(string xml,Expression path) => new(FunctionWrappedCall("xpath_int", false, Col(xml), path));

    /// <Summary>
    /// XpathLong
    /// Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    public static SparkColumn XpathLong(string xml,string path) => new(FunctionWrappedCall("xpath_long", false, Col(xml) ,Lit(path)));

    /// <Summary>
    /// XpathLong
    /// Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathLong(SparkColumn xml, Expression path) => new(FunctionWrappedCall("xpath_long", false, xml,path));

    /// <Summary>
    /// XpathLong
    /// Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathLong(string xml,Expression path) => new(FunctionWrappedCall("xpath_long", false, Col(xml),path));

    /// <Summary>
    /// XpathShort
    /// Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    public static SparkColumn XpathShort(string xml,string path) => new(FunctionWrappedCall("xpath_short", false, Col(xml), Lit(path)));

    /// <Summary>
    /// XpathShort
    /// Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathShort(SparkColumn xml,Expression path) => new(FunctionWrappedCall("xpath_short", false, xml,path));

    /// <Summary>
    /// XpathShort
    /// Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    /// </Summary>
    
	public static SparkColumn XpathShort(string xml,Expression path) => new(FunctionWrappedCall("xpath_short", false, Col(xml), path));

    /// <Summary>
    /// XpathString
    /// Returns the text contents of the first xml node that matches the XPath expression.
    /// </Summary>
    public static SparkColumn XpathString(string xml,string path) => new(FunctionWrappedCall("xpath_string", false, Col(xml), Lit(path)));

    /// <Summary>
    /// XpathString
    /// Returns the text contents of the first xml node that matches the XPath expression.
    /// </Summary>
    
	public static SparkColumn XpathString(SparkColumn xml,Expression path) => new(FunctionWrappedCall("xpath_string", false, xml,path));

    /// <Summary>
    /// XpathString
    /// Returns the text contents of the first xml node that matches the XPath expression.
    /// </Summary>
    
	public static SparkColumn XpathString(string xml,Expression path) => new(FunctionWrappedCall("xpath_string", false, Col(xml), path));

}