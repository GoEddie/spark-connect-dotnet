namespace Spark.Connect.Dotnet.Sql;
public partial class Functions : FunctionsInternal
{
	///AllColumn <Summary>Returns a sort expression based on the ascending order of the given column name.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Asc(String col) => new(FunctionCall2("asc", false, col));

	///AllColumn <Summary>Returns a sort expression based on the ascending order of the given column name.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Asc(SparkColumn col) => new(FunctionCall2("asc", false, col));

	///AllColumn <Summary>Returns a sort expression based on the descending order of the given column name.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Desc(String col) => new(FunctionCall2("desc", false, col));

	///AllColumn <Summary>Returns a sort expression based on the descending order of the given column name.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Desc(SparkColumn col) => new(FunctionCall2("desc", false, col));

	///AllColumn <Summary>Computes the square root of the specified float value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sqrt(String col) => new(FunctionCall2("sqrt", false, col));

	///AllColumn <Summary>Computes the square root of the specified float value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sqrt(SparkColumn col) => new(FunctionCall2("sqrt", false, col));

	///AllColumn <Summary>Returns the sum of `left`and `right` and the result is null on overflow. The acceptable input types are the same with the `+` operator.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn TryAdd(String left, String right) => new(FunctionCall2("try_add", false, left, right));

	///AllColumn <Summary>Returns the sum of `left`and `right` and the result is null on overflow. The acceptable input types are the same with the `+` operator.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn TryAdd(SparkColumn left, SparkColumn right) => new(FunctionCall2("try_add", false, left, right));

	///AllColumn <Summary>Returns the mean calculated from values of a group and the result is null on overflow.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TryAvg(String col) => new(FunctionCall2("try_avg", false, col));

	///AllColumn <Summary>Returns the mean calculated from values of a group and the result is null on overflow.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TryAvg(SparkColumn col) => new(FunctionCall2("try_avg", false, col));

	///AllColumn <Summary>Returns `dividend`/`divisor`. It always performs floating point division. Its result is always null if `divisor` is 0.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn TryDivide(String left, String right) => new(FunctionCall2("try_divide", false, left, right));

	///AllColumn <Summary>Returns `dividend`/`divisor`. It always performs floating point division. Its result is always null if `divisor` is 0.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn TryDivide(SparkColumn left, SparkColumn right) => new(FunctionCall2("try_divide", false, left, right));

	///AllColumn <Summary>Returns `left`*`right` and the result is null on overflow. The acceptable input types are the same with the `*` operator.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn TryMultiply(String left, String right) => new(FunctionCall2("try_multiply", false, left, right));

	///AllColumn <Summary>Returns `left`*`right` and the result is null on overflow. The acceptable input types are the same with the `*` operator.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn TryMultiply(SparkColumn left, SparkColumn right) => new(FunctionCall2("try_multiply", false, left, right));

	///AllColumn <Summary>Returns `left`-`right` and the result is null on overflow. The acceptable input types are the same with the `-` operator.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn TrySubtract(String left, String right) => new(FunctionCall2("try_subtract", false, left, right));

	///AllColumn <Summary>Returns `left`-`right` and the result is null on overflow. The acceptable input types are the same with the `-` operator.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn TrySubtract(SparkColumn left, SparkColumn right) => new(FunctionCall2("try_subtract", false, left, right));

	///AllColumn <Summary>Returns the sum calculated from values of a group and the result is null on overflow.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TrySum(String col) => new(FunctionCall2("try_sum", false, col));

	///AllColumn <Summary>Returns the sum calculated from values of a group and the result is null on overflow.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TrySum(SparkColumn col) => new(FunctionCall2("try_sum", false, col));

	///AllColumn <Summary>Computes the absolute value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Abs(String col) => new(FunctionCall2("abs", false, col));

	///AllColumn <Summary>Computes the absolute value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Abs(SparkColumn col) => new(FunctionCall2("abs", false, col));

	///AllColumn <Summary>Returns the most frequent value in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Mode(String col) => new(FunctionCall2("mode", false, col));

	///AllColumn <Summary>Returns the most frequent value in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Mode(SparkColumn col) => new(FunctionCall2("mode", false, col));

	///AllColumn <Summary>Aggregate function: returns the maximum value of the expression in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Max(String col) => new(FunctionCall2("max", false, col));

	///AllColumn <Summary>Aggregate function: returns the maximum value of the expression in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Max(SparkColumn col) => new(FunctionCall2("max", false, col));

	///AllColumn <Summary>Aggregate function: returns the minimum value of the expression in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Min(String col) => new(FunctionCall2("min", false, col));

	///AllColumn <Summary>Aggregate function: returns the minimum value of the expression in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Min(SparkColumn col) => new(FunctionCall2("min", false, col));

	///AllColumn <Summary>Returns the value associated with the maximum value of ord.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="ord">'ColumnOrName'</param>
	public static SparkColumn MaxBy(String col, String ord) => new(FunctionCall2("max_by", false, col, ord));

	///AllColumn <Summary>Returns the value associated with the maximum value of ord.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="ord">'ColumnOrName'</param>
	public static SparkColumn MaxBy(SparkColumn col, SparkColumn ord) => new(FunctionCall2("max_by", false, col, ord));

	///AllColumn <Summary>Returns the value associated with the minimum value of ord.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="ord">'ColumnOrName'</param>
	public static SparkColumn MinBy(String col, String ord) => new(FunctionCall2("min_by", false, col, ord));

	///AllColumn <Summary>Returns the value associated with the minimum value of ord.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="ord">'ColumnOrName'</param>
	public static SparkColumn MinBy(SparkColumn col, SparkColumn ord) => new(FunctionCall2("min_by", false, col, ord));

	///AllColumn <Summary>Aggregate function: returns the number of items in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Count(String col) => new(FunctionCall2("count", false, col));

	///AllColumn <Summary>Aggregate function: returns the number of items in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Count(SparkColumn col) => new(FunctionCall2("count", false, col));

	///AllColumn <Summary>Aggregate function: returns the sum of all values in the expression.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sum(String col) => new(FunctionCall2("sum", false, col));

	///AllColumn <Summary>Aggregate function: returns the sum of all values in the expression.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sum(SparkColumn col) => new(FunctionCall2("sum", false, col));

	///AllColumn <Summary>Aggregate function: returns the average of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Avg(String col) => new(FunctionCall2("avg", false, col));

	///AllColumn <Summary>Aggregate function: returns the average of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Avg(SparkColumn col) => new(FunctionCall2("avg", false, col));

	///AllColumn <Summary>Aggregate function: returns the average of the values in a group. An alias of :func:`avg`.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Mean(String col) => new(FunctionCall2("mean", false, col));

	///AllColumn <Summary>Aggregate function: returns the average of the values in a group. An alias of :func:`avg`.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Mean(SparkColumn col) => new(FunctionCall2("mean", false, col));

	///AllColumn <Summary>Returns the median of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Median(String col) => new(FunctionCall2("median", false, col));

	///AllColumn <Summary>Returns the median of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Median(SparkColumn col) => new(FunctionCall2("median", false, col));

	///AllColumn <Summary>Aggregate function: returns the sum of distinct values in the expression.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn SumDistinct(String col) => new(FunctionCall2("sum_distinct", false, col));

	///AllColumn <Summary>Aggregate function: returns the sum of distinct values in the expression.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn SumDistinct(SparkColumn col) => new(FunctionCall2("sum_distinct", false, col));

	///AllColumn <Summary>Aggregate function: returns the product of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Product(String col) => new(FunctionCall2("product", false, col));

	///AllColumn <Summary>Aggregate function: returns the product of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Product(SparkColumn col) => new(FunctionCall2("product", false, col));

	///AllColumn <Summary>Computes inverse cosine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Acos(String col) => new(FunctionCall2("acos", false, col));

	///AllColumn <Summary>Computes inverse cosine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Acos(SparkColumn col) => new(FunctionCall2("acos", false, col));

	///AllColumn <Summary>Computes inverse hyperbolic cosine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Acosh(String col) => new(FunctionCall2("acosh", false, col));

	///AllColumn <Summary>Computes inverse hyperbolic cosine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Acosh(SparkColumn col) => new(FunctionCall2("acosh", false, col));

	///AllColumn <Summary>Computes inverse sine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Asin(String col) => new(FunctionCall2("asin", false, col));

	///AllColumn <Summary>Computes inverse sine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Asin(SparkColumn col) => new(FunctionCall2("asin", false, col));

	///AllColumn <Summary>Computes inverse hyperbolic sine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Asinh(String col) => new(FunctionCall2("asinh", false, col));

	///AllColumn <Summary>Computes inverse hyperbolic sine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Asinh(SparkColumn col) => new(FunctionCall2("asinh", false, col));

	///AllColumn <Summary>Compute inverse tangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Atan(String col) => new(FunctionCall2("atan", false, col));

	///AllColumn <Summary>Compute inverse tangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Atan(SparkColumn col) => new(FunctionCall2("atan", false, col));

	///AllColumn <Summary>Computes inverse hyperbolic tangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Atanh(String col) => new(FunctionCall2("atanh", false, col));

	///AllColumn <Summary>Computes inverse hyperbolic tangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Atanh(SparkColumn col) => new(FunctionCall2("atanh", false, col));

	///AllColumn <Summary>Computes the cube-root of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cbrt(String col) => new(FunctionCall2("cbrt", false, col));

	///AllColumn <Summary>Computes the cube-root of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cbrt(SparkColumn col) => new(FunctionCall2("cbrt", false, col));

	///AllColumn <Summary>Computes the ceiling of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ceil(String col) => new(FunctionCall2("ceil", false, col));

	///AllColumn <Summary>Computes the ceiling of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ceil(SparkColumn col) => new(FunctionCall2("ceil", false, col));

	///AllColumn <Summary>Computes the ceiling of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ceiling(String col) => new(FunctionCall2("ceiling", false, col));

	///AllColumn <Summary>Computes the ceiling of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ceiling(SparkColumn col) => new(FunctionCall2("ceiling", false, col));

	///AllColumn <Summary>Computes cosine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cos(String col) => new(FunctionCall2("cos", false, col));

	///AllColumn <Summary>Computes cosine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cos(SparkColumn col) => new(FunctionCall2("cos", false, col));

	///AllColumn <Summary>Computes hyperbolic cosine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cosh(String col) => new(FunctionCall2("cosh", false, col));

	///AllColumn <Summary>Computes hyperbolic cosine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cosh(SparkColumn col) => new(FunctionCall2("cosh", false, col));

	///AllColumn <Summary>Computes cotangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cot(String col) => new(FunctionCall2("cot", false, col));

	///AllColumn <Summary>Computes cotangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cot(SparkColumn col) => new(FunctionCall2("cot", false, col));

	///AllColumn <Summary>Computes cosecant of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Csc(String col) => new(FunctionCall2("csc", false, col));

	///AllColumn <Summary>Computes cosecant of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Csc(SparkColumn col) => new(FunctionCall2("csc", false, col));

	/// <Summary>Returns Euler's number.</Summary>
	public static SparkColumn E() => new(FunctionCall2("e", false));

	///AllColumn <Summary>Computes the exponential of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Exp(String col) => new(FunctionCall2("exp", false, col));

	///AllColumn <Summary>Computes the exponential of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Exp(SparkColumn col) => new(FunctionCall2("exp", false, col));

	///AllColumn <Summary>Computes the exponential of the given value minus one.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Expm1(String col) => new(FunctionCall2("expm1", false, col));

	///AllColumn <Summary>Computes the exponential of the given value minus one.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Expm1(SparkColumn col) => new(FunctionCall2("expm1", false, col));

	///AllColumn <Summary>Computes the floor of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Floor(String col) => new(FunctionCall2("floor", false, col));

	///AllColumn <Summary>Computes the floor of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Floor(SparkColumn col) => new(FunctionCall2("floor", false, col));

	///AllColumn <Summary>Computes the natural logarithm of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Log(String col) => new(FunctionCall2("log", false, col));

	///AllColumn <Summary>Computes the natural logarithm of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Log(SparkColumn col) => new(FunctionCall2("log", false, col));

	///AllColumn <Summary>Computes the logarithm of the given value in Base 10.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Log10(String col) => new(FunctionCall2("log10", false, col));

	///AllColumn <Summary>Computes the logarithm of the given value in Base 10.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Log10(SparkColumn col) => new(FunctionCall2("log10", false, col));

	///AllColumn <Summary>Computes the natural logarithm of the QUOTgiven value plus oneQUOT.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Log1p(String col) => new(FunctionCall2("log1p", false, col));

	///AllColumn <Summary>Computes the natural logarithm of the QUOTgiven value plus oneQUOT.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Log1p(SparkColumn col) => new(FunctionCall2("log1p", false, col));

	///AllColumn <Summary>Returns the negative value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Negative(String col) => new(FunctionCall2("negative", false, col));

	///AllColumn <Summary>Returns the negative value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Negative(SparkColumn col) => new(FunctionCall2("negative", false, col));

	/// <Summary>Returns Pi.</Summary>
	public static SparkColumn Pi() => new(FunctionCall2("pi", false));

	///AllColumn <Summary>Returns the value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Positive(String col) => new(FunctionCall2("positive", false, col));

	///AllColumn <Summary>Returns the value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Positive(SparkColumn col) => new(FunctionCall2("positive", false, col));

	///AllColumn <Summary>Returns the double value that is closest in value to the argument and is equal to a mathematical integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Rint(String col) => new(FunctionCall2("rint", false, col));

	///AllColumn <Summary>Returns the double value that is closest in value to the argument and is equal to a mathematical integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Rint(SparkColumn col) => new(FunctionCall2("rint", false, col));

	///AllColumn <Summary>Computes secant of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sec(String col) => new(FunctionCall2("sec", false, col));

	///AllColumn <Summary>Computes secant of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sec(SparkColumn col) => new(FunctionCall2("sec", false, col));

	///AllColumn <Summary>Computes the signum of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Signum(String col) => new(FunctionCall2("signum", false, col));

	///AllColumn <Summary>Computes the signum of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Signum(SparkColumn col) => new(FunctionCall2("signum", false, col));

	///AllColumn <Summary>Computes the signum of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sign(String col) => new(FunctionCall2("sign", false, col));

	///AllColumn <Summary>Computes the signum of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sign(SparkColumn col) => new(FunctionCall2("sign", false, col));

	///AllColumn <Summary>Computes sine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sin(String col) => new(FunctionCall2("sin", false, col));

	///AllColumn <Summary>Computes sine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sin(SparkColumn col) => new(FunctionCall2("sin", false, col));

	///AllColumn <Summary>Computes hyperbolic sine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sinh(String col) => new(FunctionCall2("sinh", false, col));

	///AllColumn <Summary>Computes hyperbolic sine of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sinh(SparkColumn col) => new(FunctionCall2("sinh", false, col));

	///AllColumn <Summary>Computes tangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Tan(String col) => new(FunctionCall2("tan", false, col));

	///AllColumn <Summary>Computes tangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Tan(SparkColumn col) => new(FunctionCall2("tan", false, col));

	///AllColumn <Summary>Computes hyperbolic tangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Tanh(String col) => new(FunctionCall2("tanh", false, col));

	///AllColumn <Summary>Computes hyperbolic tangent of the input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Tanh(SparkColumn col) => new(FunctionCall2("tanh", false, col));

	///AllColumn <Summary>.. versionadded:: 1.4.0</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ToDegrees(String col) => new(FunctionCall2("toDegrees", false, col));

	///AllColumn <Summary>.. versionadded:: 1.4.0</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ToDegrees(SparkColumn col) => new(FunctionCall2("toDegrees", false, col));

	///AllColumn <Summary>.. versionadded:: 1.4.0</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ToRadians(String col) => new(FunctionCall2("toRadians", false, col));

	///AllColumn <Summary>.. versionadded:: 1.4.0</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ToRadians(SparkColumn col) => new(FunctionCall2("toRadians", false, col));

	///AllColumn <Summary>Computes bitwise not.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitwiseNOT(String col) => new(FunctionCall2("bitwiseNOT", false, col));

	///AllColumn <Summary>Computes bitwise not.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitwiseNOT(SparkColumn col) => new(FunctionCall2("bitwiseNOT", false, col));

	///AllColumn <Summary>Computes bitwise not.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitwiseNot(String col) => new(FunctionCall2("bitwise_not", false, col));

	///AllColumn <Summary>Computes bitwise not.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitwiseNot(SparkColumn col) => new(FunctionCall2("bitwise_not", false, col));

	///AllColumn <Summary>Returns the number of bits that are set in the argument expr as an unsigned 64-bit integer, or NULL if the argument is NULL.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitCount(String col) => new(FunctionCall2("bit_count", false, col));

	///AllColumn <Summary>Returns the number of bits that are set in the argument expr as an unsigned 64-bit integer, or NULL if the argument is NULL.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitCount(SparkColumn col) => new(FunctionCall2("bit_count", false, col));

	///AllColumn <Summary>Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="pos">'ColumnOrName'</param>
	public static SparkColumn BitGet(String col, String pos) => new(FunctionCall2("bit_get", false, col, pos));

	///AllColumn <Summary>Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="pos">'ColumnOrName'</param>
	public static SparkColumn BitGet(SparkColumn col, SparkColumn pos) => new(FunctionCall2("bit_get", false, col, pos));

	///AllColumn <Summary>Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="pos">'ColumnOrName'</param>
	public static SparkColumn Getbit(String col, String pos) => new(FunctionCall2("getbit", false, col, pos));

	///AllColumn <Summary>Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="pos">'ColumnOrName'</param>
	public static SparkColumn Getbit(SparkColumn col, SparkColumn pos) => new(FunctionCall2("getbit", false, col, pos));

	///AllColumn <Summary>Returns a sort expression based on the ascending order of the given column name, and null values return before non-null values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn AscNullsFirst(String col) => new(FunctionCall2("asc_nulls_first", false, col));

	///AllColumn <Summary>Returns a sort expression based on the ascending order of the given column name, and null values return before non-null values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn AscNullsFirst(SparkColumn col) => new(FunctionCall2("asc_nulls_first", false, col));

	///AllColumn <Summary>Returns a sort expression based on the ascending order of the given column name, and null values appear after non-null values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn AscNullsLast(String col) => new(FunctionCall2("asc_nulls_last", false, col));

	///AllColumn <Summary>Returns a sort expression based on the ascending order of the given column name, and null values appear after non-null values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn AscNullsLast(SparkColumn col) => new(FunctionCall2("asc_nulls_last", false, col));

	///AllColumn <Summary>Returns a sort expression based on the descending order of the given column name, and null values appear before non-null values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn DescNullsFirst(String col) => new(FunctionCall2("desc_nulls_first", false, col));

	///AllColumn <Summary>Returns a sort expression based on the descending order of the given column name, and null values appear before non-null values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn DescNullsFirst(SparkColumn col) => new(FunctionCall2("desc_nulls_first", false, col));

	///AllColumn <Summary>Returns a sort expression based on the descending order of the given column name, and null values appear after non-null values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn DescNullsLast(String col) => new(FunctionCall2("desc_nulls_last", false, col));

	///AllColumn <Summary>Returns a sort expression based on the descending order of the given column name, and null values appear after non-null values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn DescNullsLast(SparkColumn col) => new(FunctionCall2("desc_nulls_last", false, col));

	///AllColumn <Summary>Aggregate function: alias for stddev_samp.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Stddev(String col) => new(FunctionCall2("stddev", false, col));

	///AllColumn <Summary>Aggregate function: alias for stddev_samp.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Stddev(SparkColumn col) => new(FunctionCall2("stddev", false, col));

	///AllColumn <Summary>Aggregate function: alias for stddev_samp.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Std(String col) => new(FunctionCall2("std", false, col));

	///AllColumn <Summary>Aggregate function: alias for stddev_samp.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Std(SparkColumn col) => new(FunctionCall2("std", false, col));

	///AllColumn <Summary>Aggregate function: returns the unbiased sample standard deviation of the expression in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn StddevSamp(String col) => new(FunctionCall2("stddev_samp", false, col));

	///AllColumn <Summary>Aggregate function: returns the unbiased sample standard deviation of the expression in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn StddevSamp(SparkColumn col) => new(FunctionCall2("stddev_samp", false, col));

	///AllColumn <Summary>Aggregate function: returns population standard deviation of the expression in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn StddevPop(String col) => new(FunctionCall2("stddev_pop", false, col));

	///AllColumn <Summary>Aggregate function: returns population standard deviation of the expression in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn StddevPop(SparkColumn col) => new(FunctionCall2("stddev_pop", false, col));

	///AllColumn <Summary>Aggregate function: alias for var_samp</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Variance(String col) => new(FunctionCall2("variance", false, col));

	///AllColumn <Summary>Aggregate function: alias for var_samp</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Variance(SparkColumn col) => new(FunctionCall2("variance", false, col));

	///AllColumn <Summary>Aggregate function: returns the unbiased sample variance of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn VarSamp(String col) => new(FunctionCall2("var_samp", false, col));

	///AllColumn <Summary>Aggregate function: returns the unbiased sample variance of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn VarSamp(SparkColumn col) => new(FunctionCall2("var_samp", false, col));

	///AllColumn <Summary>Aggregate function: returns the population variance of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn VarPop(String col) => new(FunctionCall2("var_pop", false, col));

	///AllColumn <Summary>Aggregate function: returns the population variance of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn VarPop(SparkColumn col) => new(FunctionCall2("var_pop", false, col));

	///AllColumn <Summary>Aggregate function: returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrAvgx(String y, String x) => new(FunctionCall2("regr_avgx", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrAvgx(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_avgx", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrAvgy(String y, String x) => new(FunctionCall2("regr_avgy", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrAvgy(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_avgy", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the number of non-null number pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrCount(String y, String x) => new(FunctionCall2("regr_count", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the number of non-null number pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrCount(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_count", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the intercept of the univariate linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrIntercept(String y, String x) => new(FunctionCall2("regr_intercept", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the intercept of the univariate linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrIntercept(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_intercept", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the coefficient of determination for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrR2(String y, String x) => new(FunctionCall2("regr_r2", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the coefficient of determination for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrR2(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_r2", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the slope of the linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrSlope(String y, String x) => new(FunctionCall2("regr_slope", false, y, x));

	///AllColumn <Summary>Aggregate function: returns the slope of the linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrSlope(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_slope", false, y, x));

	///AllColumn <Summary>Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrSxx(String y, String x) => new(FunctionCall2("regr_sxx", false, y, x));

	///AllColumn <Summary>Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrSxx(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_sxx", false, y, x));

	///AllColumn <Summary>Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrSxy(String y, String x) => new(FunctionCall2("regr_sxy", false, y, x));

	///AllColumn <Summary>Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrSxy(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_sxy", false, y, x));

	///AllColumn <Summary>Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrSyy(String y, String x) => new(FunctionCall2("regr_syy", false, y, x));

	///AllColumn <Summary>Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.</Summary>
	/// <param name="y">'ColumnOrName'</param>
	/// <param name="x">'ColumnOrName'</param>
	public static SparkColumn RegrSyy(SparkColumn y, SparkColumn x) => new(FunctionCall2("regr_syy", false, y, x));

	///AllColumn <Summary>Aggregate function: returns true if all values of `col` are true.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Every(String col) => new(FunctionCall2("every", false, col));

	///AllColumn <Summary>Aggregate function: returns true if all values of `col` are true.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Every(SparkColumn col) => new(FunctionCall2("every", false, col));

	///AllColumn <Summary>Aggregate function: returns true if all values of `col` are true.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BoolAnd(String col) => new(FunctionCall2("bool_and", false, col));

	///AllColumn <Summary>Aggregate function: returns true if all values of `col` are true.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BoolAnd(SparkColumn col) => new(FunctionCall2("bool_and", false, col));

	///AllColumn <Summary>Aggregate function: returns true if at least one value of `col` is true.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Some(String col) => new(FunctionCall2("some", false, col));

	///AllColumn <Summary>Aggregate function: returns true if at least one value of `col` is true.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Some(SparkColumn col) => new(FunctionCall2("some", false, col));

	///AllColumn <Summary>Aggregate function: returns true if at least one value of `col` is true.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BoolOr(String col) => new(FunctionCall2("bool_or", false, col));

	///AllColumn <Summary>Aggregate function: returns true if at least one value of `col` is true.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BoolOr(SparkColumn col) => new(FunctionCall2("bool_or", false, col));

	///AllColumn <Summary>Aggregate function: returns the bitwise AND of all non-null input values, or null if none.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitAnd(String col) => new(FunctionCall2("bit_and", false, col));

	///AllColumn <Summary>Aggregate function: returns the bitwise AND of all non-null input values, or null if none.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitAnd(SparkColumn col) => new(FunctionCall2("bit_and", false, col));

	///AllColumn <Summary>Aggregate function: returns the bitwise OR of all non-null input values, or null if none.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitOr(String col) => new(FunctionCall2("bit_or", false, col));

	///AllColumn <Summary>Aggregate function: returns the bitwise OR of all non-null input values, or null if none.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitOr(SparkColumn col) => new(FunctionCall2("bit_or", false, col));

	///AllColumn <Summary>Aggregate function: returns the bitwise XOR of all non-null input values, or null if none.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitXor(String col) => new(FunctionCall2("bit_xor", false, col));

	///AllColumn <Summary>Aggregate function: returns the bitwise XOR of all non-null input values, or null if none.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitXor(SparkColumn col) => new(FunctionCall2("bit_xor", false, col));

	///AllColumn <Summary>Aggregate function: returns the skewness of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Skewness(String col) => new(FunctionCall2("skewness", false, col));

	///AllColumn <Summary>Aggregate function: returns the skewness of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Skewness(SparkColumn col) => new(FunctionCall2("skewness", false, col));

	///AllColumn <Summary>Aggregate function: returns the kurtosis of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Kurtosis(String col) => new(FunctionCall2("kurtosis", false, col));

	///AllColumn <Summary>Aggregate function: returns the kurtosis of the values in a group.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Kurtosis(SparkColumn col) => new(FunctionCall2("kurtosis", false, col));

	///AllColumn <Summary>Aggregate function: returns a list of objects with duplicates.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn CollectList(String col) => new(FunctionCall2("collect_list", false, col));

	///AllColumn <Summary>Aggregate function: returns a list of objects with duplicates.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn CollectList(SparkColumn col) => new(FunctionCall2("collect_list", false, col));

	///AllColumn <Summary>Aggregate function: returns a list of objects with duplicates.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayAgg(String col) => new(FunctionCall2("array_agg", false, col));

	///AllColumn <Summary>Aggregate function: returns a list of objects with duplicates.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayAgg(SparkColumn col) => new(FunctionCall2("array_agg", false, col));

	///AllColumn <Summary>Aggregate function: returns a set of objects with duplicate elements eliminated.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn CollectSet(String col) => new(FunctionCall2("collect_set", false, col));

	///AllColumn <Summary>Aggregate function: returns a set of objects with duplicate elements eliminated.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn CollectSet(SparkColumn col) => new(FunctionCall2("collect_set", false, col));

	///AllColumn <Summary>Converts an angle measured in radians to an approximately equivalent angle measured in degrees.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Degrees(String col) => new(FunctionCall2("degrees", false, col));

	///AllColumn <Summary>Converts an angle measured in radians to an approximately equivalent angle measured in degrees.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Degrees(SparkColumn col) => new(FunctionCall2("degrees", false, col));

	///AllColumn <Summary>Converts an angle measured in degrees to an approximately equivalent angle measured in radians.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Radians(String col) => new(FunctionCall2("radians", false, col));

	///AllColumn <Summary>Converts an angle measured in degrees to an approximately equivalent angle measured in radians.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Radians(SparkColumn col) => new(FunctionCall2("radians", false, col));

	/// <Summary>Window function: returns a sequential number starting at 1 within a window partition.</Summary>
	public static SparkColumn RowNumber() => new(FunctionCall2("row_number", false));

	/// <Summary>Window function: returns the rank of rows within a window partition, without any gaps.</Summary>
	public static SparkColumn DenseRank() => new(FunctionCall2("dense_rank", false));

	/// <Summary>Window function: returns the rank of rows within a window partition.</Summary>
	public static SparkColumn Rank() => new(FunctionCall2("rank", false));

	/// <Summary>Window function: returns the cumulative distribution of values within a window partition, i.e. the fraction of rows that are below the current row.</Summary>
	public static SparkColumn CumeDist() => new(FunctionCall2("cume_dist", false));

	/// <Summary>Window function: returns the relative rank (i.e. percentile) of rows within a window partition.</Summary>
	public static SparkColumn PercentRank() => new(FunctionCall2("percent_rank", false));

	///AllColumn <Summary>Returns the first column that is not null.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Coalesce(List<String> cols) => new(FunctionCall2("coalesce", false, cols));

	///AllColumn <Summary>Returns the first column that is not null.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Coalesce(List<SparkColumn> cols) => new(FunctionCall2("coalesce", false, cols));

	///AllColumn <Summary>Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for ``col1`` and ``col2``.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Corr(String col1, String col2) => new(FunctionCall2("corr", false, col1, col2));

	///AllColumn <Summary>Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for ``col1`` and ``col2``.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Corr(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("corr", false, col1, col2));

	///AllColumn <Summary>Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and ``col2``.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn CovarPop(String col1, String col2) => new(FunctionCall2("covar_pop", false, col1, col2));

	///AllColumn <Summary>Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and ``col2``.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn CovarPop(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("covar_pop", false, col1, col2));

	///AllColumn <Summary>Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and ``col2``.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn CovarSamp(String col1, String col2) => new(FunctionCall2("covar_samp", false, col1, col2));

	///AllColumn <Summary>Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and ``col2``.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn CovarSamp(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("covar_samp", false, col1, col2));

	/// <Summary>Column+SimpleType::Aggregate function: returns the first value in a group.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="ignorenulls">bool</param>
	public static SparkColumn First(String col, String ignorenulls) => new(FunctionCall2("first", false, col, ignorenulls));

	/// <Summary>Column+SimpleType::Aggregate function: returns the first value in a group.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="ignorenulls">bool</param>
	public static SparkColumn First(SparkColumn col, SparkColumn ignorenulls) => new(FunctionCall2("first", false, col, ignorenulls));

	///AllColumn <Summary>Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated or not, returns 1 for aggregated or 0 for not aggregated in the result set.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Grouping(String col) => new(FunctionCall2("grouping", false, col));

	///AllColumn <Summary>Aggregate function: indicates whether a specified column in a GROUP BY list is aggregated or not, returns 1 for aggregated or 0 for not aggregated in the result set.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Grouping(SparkColumn col) => new(FunctionCall2("grouping", false, col));

	///AllColumn <Summary>Aggregate function: returns the level of grouping, equals to</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn GroupingId(List<String> cols) => new(FunctionCall2("grouping_id", false, cols));

	///AllColumn <Summary>Aggregate function: returns the level of grouping, equals to</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn GroupingId(List<SparkColumn> cols) => new(FunctionCall2("grouping_id", false, cols));

	///AllColumn <Summary>Returns a count-min sketch of a column with the given esp, confidence and seed. The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="eps">'ColumnOrName'</param>
	/// <param name="confidence">'ColumnOrName'</param>
	/// <param name="seed">'ColumnOrName'</param>
	public static SparkColumn CountMinSketch(String col, String eps, String confidence, String seed) => new(FunctionCall2("count_min_sketch", false, col, eps, confidence, seed));

	///AllColumn <Summary>Returns a count-min sketch of a column with the given esp, confidence and seed. The result is an array of bytes, which can be deserialized to a `CountMinSketch` before usage. Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="eps">'ColumnOrName'</param>
	/// <param name="confidence">'ColumnOrName'</param>
	/// <param name="seed">'ColumnOrName'</param>
	public static SparkColumn CountMinSketch(SparkColumn col, SparkColumn eps, SparkColumn confidence, SparkColumn seed) => new(FunctionCall2("count_min_sketch", false, col, eps, confidence, seed));

	/// <Summary>Creates a string column for the file name of the current Spark task.</Summary>
	public static SparkColumn InputFileName() => new(FunctionCall2("input_file_name", false));

	///AllColumn <Summary>An expression that returns true if the column is NaN.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Isnan(String col) => new(FunctionCall2("isnan", false, col));

	///AllColumn <Summary>An expression that returns true if the column is NaN.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Isnan(SparkColumn col) => new(FunctionCall2("isnan", false, col));

	///AllColumn <Summary>An expression that returns true if the column is null.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Isnull(String col) => new(FunctionCall2("isnull", false, col));

	///AllColumn <Summary>An expression that returns true if the column is null.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Isnull(SparkColumn col) => new(FunctionCall2("isnull", false, col));

	/// <Summary>Column+SimpleType::Aggregate function: returns the last value in a group.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="ignorenulls">bool</param>
	public static SparkColumn Last(String col, String ignorenulls) => new(FunctionCall2("last", false, col, ignorenulls));

	/// <Summary>Column+SimpleType::Aggregate function: returns the last value in a group.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="ignorenulls">bool</param>
	public static SparkColumn Last(SparkColumn col, SparkColumn ignorenulls) => new(FunctionCall2("last", false, col, ignorenulls));

	/// <Summary>A column that generates monotonically increasing 64-bit integers.</Summary>
	public static SparkColumn MonotonicallyIncreasingId() => new(FunctionCall2("monotonically_increasing_id", false));

	///AllColumn <Summary>Returns col1 if it is not NaN, or col2 if col1 is NaN.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Nanvl(String col1, String col2) => new(FunctionCall2("nanvl", false, col1, col2));

	///AllColumn <Summary>Returns col1 if it is not NaN, or col2 if col1 is NaN.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Nanvl(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("nanvl", false, col1, col2));

	/// <Summary>Column+SimpleType::Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0 or at integral part when `scale` < 0.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="scale">int</param>
	public static SparkColumn Round(String col, String scale) => new(FunctionCall2("round", false, col, scale));

	/// <Summary>Column+SimpleType::Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0 or at integral part when `scale` < 0.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="scale">int</param>
	public static SparkColumn Round(SparkColumn col, SparkColumn scale) => new(FunctionCall2("round", false, col, scale));

	/// <Summary>Column+SimpleType::Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0 or at integral part when `scale` < 0.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="scale">int</param>
	public static SparkColumn Bround(String col, String scale) => new(FunctionCall2("bround", false, col, scale));

	/// <Summary>Column+SimpleType::Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0 or at integral part when `scale` < 0.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="scale">int</param>
	public static SparkColumn Bround(SparkColumn col, SparkColumn scale) => new(FunctionCall2("bround", false, col, scale));

	/// <Summary>Column+SimpleType::Shift the given value numBits left.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="numBits">int</param>
	public static SparkColumn ShiftLeft(String col, String numBits) => new(FunctionCall2("shiftLeft", false, col, numBits));

	/// <Summary>Column+SimpleType::Shift the given value numBits left.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="numBits">int</param>
	public static SparkColumn ShiftLeft(SparkColumn col, SparkColumn numBits) => new(FunctionCall2("shiftLeft", false, col, numBits));

	/// <Summary>Column+SimpleType::Shift the given value numBits left.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="numBits">int</param>
	public static SparkColumn Shiftleft(String col, String numBits) => new(FunctionCall2("shiftleft", false, col, numBits));

	/// <Summary>Column+SimpleType::Shift the given value numBits left.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="numBits">int</param>
	public static SparkColumn Shiftleft(SparkColumn col, SparkColumn numBits) => new(FunctionCall2("shiftleft", false, col, numBits));

	/// <Summary>Column+SimpleType::(Signed) shift the given value numBits right.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="numBits">int</param>
	public static SparkColumn ShiftRight(String col, String numBits) => new(FunctionCall2("shiftRight", false, col, numBits));

	/// <Summary>Column+SimpleType::(Signed) shift the given value numBits right.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="numBits">int</param>
	public static SparkColumn ShiftRight(SparkColumn col, SparkColumn numBits) => new(FunctionCall2("shiftRight", false, col, numBits));

	/// <Summary>Column+SimpleType::(Signed) shift the given value numBits right.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="numBits">int</param>
	public static SparkColumn Shiftright(String col, String numBits) => new(FunctionCall2("shiftright", false, col, numBits));

	/// <Summary>Column+SimpleType::(Signed) shift the given value numBits right.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="numBits">int</param>
	public static SparkColumn Shiftright(SparkColumn col, SparkColumn numBits) => new(FunctionCall2("shiftright", false, col, numBits));

	/// <Summary>Column+SimpleType::Unsigned shift the given value numBits right.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="numBits">int</param>
	public static SparkColumn ShiftRightUnsigned(String col, String numBits) => new(FunctionCall2("shiftRightUnsigned", false, col, numBits));

	/// <Summary>Column+SimpleType::Unsigned shift the given value numBits right.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="numBits">int</param>
	public static SparkColumn ShiftRightUnsigned(SparkColumn col, SparkColumn numBits) => new(FunctionCall2("shiftRightUnsigned", false, col, numBits));

	/// <Summary>Column+SimpleType::Unsigned shift the given value numBits right.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="numBits">int</param>
	public static SparkColumn Shiftrightunsigned(String col, String numBits) => new(FunctionCall2("shiftrightunsigned", false, col, numBits));

	/// <Summary>Column+SimpleType::Unsigned shift the given value numBits right.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="numBits">int</param>
	public static SparkColumn Shiftrightunsigned(SparkColumn col, SparkColumn numBits) => new(FunctionCall2("shiftrightunsigned", false, col, numBits));

	/// <Summary>A column for partition ID.</Summary>
	public static SparkColumn SparkPartitionId() => new(FunctionCall2("spark_partition_id", false));

	///AllColumn <Summary></Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Struct(List<String> cols) => new(FunctionCall2("struct", false, cols));

	///AllColumn <Summary></Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Struct(List<SparkColumn> cols) => new(FunctionCall2("struct", false, cols));

	/// <Summary>Creates a new struct column.</Summary>
	public static SparkColumn Struct() => new(FunctionCall2("struct", false));

	///AllColumn <Summary>Creates a struct with the given field names and values.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn NamedStruct(List<String> cols) => new(FunctionCall2("named_struct", false, cols));

	///AllColumn <Summary>Creates a struct with the given field names and values.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn NamedStruct(List<SparkColumn> cols) => new(FunctionCall2("named_struct", false, cols));

	///AllColumn <Summary>Returns the greatest value of the list of column names, skipping null values. This function takes at least 2 parameters. It will return null if all parameters are null.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Greatest(List<String> cols) => new(FunctionCall2("greatest", false, cols));

	///AllColumn <Summary>Returns the greatest value of the list of column names, skipping null values. This function takes at least 2 parameters. It will return null if all parameters are null.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Greatest(List<SparkColumn> cols) => new(FunctionCall2("greatest", false, cols));

	///AllColumn <Summary>Returns the least value of the list of column names, skipping null values. This function takes at least 2 parameters. It will return null if all parameters are null.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Least(List<String> cols) => new(FunctionCall2("least", false, cols));

	///AllColumn <Summary>Returns the least value of the list of column names, skipping null values. This function takes at least 2 parameters. It will return null if all parameters are null.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Least(List<SparkColumn> cols) => new(FunctionCall2("least", false, cols));

	///AllColumn <Summary>Returns the natural logarithm of the argument.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ln(String col) => new(FunctionCall2("ln", false, col));

	///AllColumn <Summary>Returns the natural logarithm of the argument.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ln(SparkColumn col) => new(FunctionCall2("ln", false, col));

	///AllColumn <Summary>Returns the base-2 logarithm of the argument.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Log2(String col) => new(FunctionCall2("log2", false, col));

	///AllColumn <Summary>Returns the base-2 logarithm of the argument.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Log2(SparkColumn col) => new(FunctionCall2("log2", false, col));

	///AllColumn <Summary>Computes the factorial of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Factorial(String col) => new(FunctionCall2("factorial", false, col));

	///AllColumn <Summary>Computes the factorial of the given value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Factorial(SparkColumn col) => new(FunctionCall2("factorial", false, col));

	///AllColumn <Summary>Returns the number of `TRUE` values for the `col`.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn CountIf(String col) => new(FunctionCall2("count_if", false, col));

	///AllColumn <Summary>Returns the number of `TRUE` values for the `col`.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn CountIf(SparkColumn col) => new(FunctionCall2("count_if", false, col));

	///AllColumn <Summary>Computes a histogram on numeric 'col' using nb bins. The return value is an array of (x,y) pairs representing the centers of the histogram's bins. As the value of 'nb' is increased, the histogram approximation gets finer-grained, but may yield artifacts around outliers. In practice, 20-40 histogram bins appear to work well, with more bins being required for skewed or smaller datasets. Note that this function creates a histogram with non-uniform bin widths. It offers no guarantees in terms of the mean-squared-error of the histogram, but in practice is comparable to the histograms produced by the R/S-Plus statistical computing packages. Note: the output type of the 'x' field in the return value is propagated from the input value consumed in the aggregate function.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="nBins">'ColumnOrName'</param>
	public static SparkColumn HistogramNumeric(String col, String nBins) => new(FunctionCall2("histogram_numeric", false, col, nBins));

	///AllColumn <Summary>Computes a histogram on numeric 'col' using nb bins. The return value is an array of (x,y) pairs representing the centers of the histogram's bins. As the value of 'nb' is increased, the histogram approximation gets finer-grained, but may yield artifacts around outliers. In practice, 20-40 histogram bins appear to work well, with more bins being required for skewed or smaller datasets. Note that this function creates a histogram with non-uniform bin widths. It offers no guarantees in terms of the mean-squared-error of the histogram, but in practice is comparable to the histograms produced by the R/S-Plus statistical computing packages. Note: the output type of the 'x' field in the return value is propagated from the input value consumed in the aggregate function.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="nBins">'ColumnOrName'</param>
	public static SparkColumn HistogramNumeric(SparkColumn col, SparkColumn nBins) => new(FunctionCall2("histogram_numeric", false, col, nBins));

	/// <Summary>Returns the current date at the start of query evaluation as a :class:`DateType` column. All calls of current_date within the same query return the same value.</Summary>
	public static SparkColumn Curdate() => new(FunctionCall2("curdate", false));

	/// <Summary>Returns the current date at the start of query evaluation as a :class:`DateType` column. All calls of current_date within the same query return the same value.</Summary>
	public static SparkColumn CurrentDate() => new(FunctionCall2("current_date", false));

	/// <Summary>Returns the current session local timezone.</Summary>
	public static SparkColumn CurrentTimezone() => new(FunctionCall2("current_timezone", false));

	/// <Summary>Returns the current timestamp at the start of query evaluation as a :class:`TimestampType` column. All calls of current_timestamp within the same query return the same value.</Summary>
	public static SparkColumn CurrentTimestamp() => new(FunctionCall2("current_timestamp", false));

	/// <Summary>Returns the current timestamp at the start of query evaluation.</Summary>
	public static SparkColumn Now() => new(FunctionCall2("now", false));

	/// <Summary>Returns the current timestamp without time zone at the start of query evaluation as a timestamp without time zone column. All calls of localtimestamp within the same query return the same value.</Summary>
	public static SparkColumn Localtimestamp() => new(FunctionCall2("localtimestamp", false));

	/// <Summary>Column+SimpleType::Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="format">string</param>
	public static SparkColumn DateFormat(String date, String format) => new(FunctionCall2("date_format", false, date, format));

	/// <Summary>Column+SimpleType::Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="format">string</param>
	public static SparkColumn DateFormat(SparkColumn date, SparkColumn format) => new(FunctionCall2("date_format", false, date, format));

	///AllColumn <Summary>Extract the year of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Year(String col) => new(FunctionCall2("year", false, col));

	///AllColumn <Summary>Extract the year of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Year(SparkColumn col) => new(FunctionCall2("year", false, col));

	///AllColumn <Summary>Extract the quarter of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Quarter(String col) => new(FunctionCall2("quarter", false, col));

	///AllColumn <Summary>Extract the quarter of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Quarter(SparkColumn col) => new(FunctionCall2("quarter", false, col));

	///AllColumn <Summary>Extract the month of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Month(String col) => new(FunctionCall2("month", false, col));

	///AllColumn <Summary>Extract the month of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Month(SparkColumn col) => new(FunctionCall2("month", false, col));

	///AllColumn <Summary>Extract the day of the week of a given date/timestamp as integer. Ranges from 1 for a Sunday through to 7 for a Saturday</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Dayofweek(String col) => new(FunctionCall2("dayofweek", false, col));

	///AllColumn <Summary>Extract the day of the week of a given date/timestamp as integer. Ranges from 1 for a Sunday through to 7 for a Saturday</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Dayofweek(SparkColumn col) => new(FunctionCall2("dayofweek", false, col));

	///AllColumn <Summary>Extract the day of the month of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Dayofmonth(String col) => new(FunctionCall2("dayofmonth", false, col));

	///AllColumn <Summary>Extract the day of the month of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Dayofmonth(SparkColumn col) => new(FunctionCall2("dayofmonth", false, col));

	///AllColumn <Summary>Extract the day of the month of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Day(String col) => new(FunctionCall2("day", false, col));

	///AllColumn <Summary>Extract the day of the month of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Day(SparkColumn col) => new(FunctionCall2("day", false, col));

	///AllColumn <Summary>Extract the day of the year of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Dayofyear(String col) => new(FunctionCall2("dayofyear", false, col));

	///AllColumn <Summary>Extract the day of the year of a given date/timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Dayofyear(SparkColumn col) => new(FunctionCall2("dayofyear", false, col));

	///AllColumn <Summary>Extract the hours of a given timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Hour(String col) => new(FunctionCall2("hour", false, col));

	///AllColumn <Summary>Extract the hours of a given timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Hour(SparkColumn col) => new(FunctionCall2("hour", false, col));

	///AllColumn <Summary>Extract the minutes of a given timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Minute(String col) => new(FunctionCall2("minute", false, col));

	///AllColumn <Summary>Extract the minutes of a given timestamp as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Minute(SparkColumn col) => new(FunctionCall2("minute", false, col));

	///AllColumn <Summary>Extract the seconds of a given date as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Second(String col) => new(FunctionCall2("second", false, col));

	///AllColumn <Summary>Extract the seconds of a given date as integer.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Second(SparkColumn col) => new(FunctionCall2("second", false, col));

	///AllColumn <Summary>Extract the week number of a given date as integer. A week is considered to start on a Monday and week 1 is the first week with more than 3 days, as defined by ISO 8601</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Weekofyear(String col) => new(FunctionCall2("weekofyear", false, col));

	///AllColumn <Summary>Extract the week number of a given date as integer. A week is considered to start on a Monday and week 1 is the first week with more than 3 days, as defined by ISO 8601</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Weekofyear(SparkColumn col) => new(FunctionCall2("weekofyear", false, col));

	///AllColumn <Summary>Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Weekday(String col) => new(FunctionCall2("weekday", false, col));

	///AllColumn <Summary>Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Weekday(SparkColumn col) => new(FunctionCall2("weekday", false, col));

	///AllColumn <Summary>Extracts a part of the date/timestamp or interval source.</Summary>
	/// <param name="field">'ColumnOrName'</param>
	/// <param name="source">'ColumnOrName'</param>
	public static SparkColumn Extract(String field, String source) => new(FunctionCall2("extract", false, field, source));

	///AllColumn <Summary>Extracts a part of the date/timestamp or interval source.</Summary>
	/// <param name="field">'ColumnOrName'</param>
	/// <param name="source">'ColumnOrName'</param>
	public static SparkColumn Extract(SparkColumn field, SparkColumn source) => new(FunctionCall2("extract", false, field, source));

	///AllColumn <Summary>Extracts a part of the date/timestamp or interval source.</Summary>
	/// <param name="field">'ColumnOrName'</param>
	/// <param name="source">'ColumnOrName'</param>
	public static SparkColumn DatePart(String field, String source) => new(FunctionCall2("date_part", false, field, source));

	///AllColumn <Summary>Extracts a part of the date/timestamp or interval source.</Summary>
	/// <param name="field">'ColumnOrName'</param>
	/// <param name="source">'ColumnOrName'</param>
	public static SparkColumn DatePart(SparkColumn field, SparkColumn source) => new(FunctionCall2("date_part", false, field, source));

	///AllColumn <Summary>Extracts a part of the date/timestamp or interval source.</Summary>
	/// <param name="field">'ColumnOrName'</param>
	/// <param name="source">'ColumnOrName'</param>
	public static SparkColumn Datepart(String field, String source) => new(FunctionCall2("datepart", false, field, source));

	///AllColumn <Summary>Extracts a part of the date/timestamp or interval source.</Summary>
	/// <param name="field">'ColumnOrName'</param>
	/// <param name="source">'ColumnOrName'</param>
	public static SparkColumn Datepart(SparkColumn field, SparkColumn source) => new(FunctionCall2("datepart", false, field, source));

	///AllColumn <Summary>Returns a column with a date built from the year, month and day columns.</Summary>
	/// <param name="year">'ColumnOrName'</param>
	/// <param name="month">'ColumnOrName'</param>
	/// <param name="day">'ColumnOrName'</param>
	public static SparkColumn MakeDate(String year, String month, String day) => new(FunctionCall2("make_date", false, year, month, day));

	///AllColumn <Summary>Returns a column with a date built from the year, month and day columns.</Summary>
	/// <param name="year">'ColumnOrName'</param>
	/// <param name="month">'ColumnOrName'</param>
	/// <param name="day">'ColumnOrName'</param>
	public static SparkColumn MakeDate(SparkColumn year, SparkColumn month, SparkColumn day) => new(FunctionCall2("make_date", false, year, month, day));

	///AllColumn <Summary>Returns the number of days from `start` to `end`.</Summary>
	/// <param name="end">'ColumnOrName'</param>
	/// <param name="start">'ColumnOrName'</param>
	public static SparkColumn Datediff(String end, String start) => new(FunctionCall2("datediff", false, end, start));

	///AllColumn <Summary>Returns the number of days from `start` to `end`.</Summary>
	/// <param name="end">'ColumnOrName'</param>
	/// <param name="start">'ColumnOrName'</param>
	public static SparkColumn Datediff(SparkColumn end, SparkColumn start) => new(FunctionCall2("datediff", false, end, start));

	///AllColumn <Summary>Returns the number of days from `start` to `end`.</Summary>
	/// <param name="end">'ColumnOrName'</param>
	/// <param name="start">'ColumnOrName'</param>
	public static SparkColumn DateDiff(String end, String start) => new(FunctionCall2("date_diff", false, end, start));

	///AllColumn <Summary>Returns the number of days from `start` to `end`.</Summary>
	/// <param name="end">'ColumnOrName'</param>
	/// <param name="start">'ColumnOrName'</param>
	public static SparkColumn DateDiff(SparkColumn end, SparkColumn start) => new(FunctionCall2("date_diff", false, end, start));

	///AllColumn <Summary>Create date from the number of `days` since 1970-01-01.</Summary>
	/// <param name="days">'ColumnOrName'</param>
	public static SparkColumn DateFromUnixDate(String days) => new(FunctionCall2("date_from_unix_date", false, days));

	///AllColumn <Summary>Create date from the number of `days` since 1970-01-01.</Summary>
	/// <param name="days">'ColumnOrName'</param>
	public static SparkColumn DateFromUnixDate(SparkColumn days) => new(FunctionCall2("date_from_unix_date", false, days));

	///AllColumn <Summary>Returns the number of days since 1970-01-01.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnixDate(String col) => new(FunctionCall2("unix_date", false, col));

	///AllColumn <Summary>Returns the number of days since 1970-01-01.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnixDate(SparkColumn col) => new(FunctionCall2("unix_date", false, col));

	///AllColumn <Summary>Returns the number of microseconds since 1970-01-01 00:00:00 UTC.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnixMicros(String col) => new(FunctionCall2("unix_micros", false, col));

	///AllColumn <Summary>Returns the number of microseconds since 1970-01-01 00:00:00 UTC.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnixMicros(SparkColumn col) => new(FunctionCall2("unix_micros", false, col));

	///AllColumn <Summary>Returns the number of milliseconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of precision.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnixMillis(String col) => new(FunctionCall2("unix_millis", false, col));

	///AllColumn <Summary>Returns the number of milliseconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of precision.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnixMillis(SparkColumn col) => new(FunctionCall2("unix_millis", false, col));

	///AllColumn <Summary>Returns the number of seconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of precision.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnixSeconds(String col) => new(FunctionCall2("unix_seconds", false, col));

	///AllColumn <Summary>Returns the number of seconds since 1970-01-01 00:00:00 UTC. Truncates higher levels of precision.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnixSeconds(SparkColumn col) => new(FunctionCall2("unix_seconds", false, col));

	///AllColumn <Summary></Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ToTimestamp(String col) => new(FunctionCall2("to_timestamp", false, col));

	///AllColumn <Summary></Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ToTimestamp(SparkColumn col) => new(FunctionCall2("to_timestamp", false, col));

	/// <Summary>Column+SimpleType::</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="format">string</param>
	public static SparkColumn ToTimestamp(String col, String format) => new(FunctionCall2("to_timestamp", false, col, format));

	/// <Summary>Column+SimpleType::</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="format">string</param>
	public static SparkColumn ToTimestamp(SparkColumn col, SparkColumn format) => new(FunctionCall2("to_timestamp", false, col, format));

	///AllColumn <Summary>Returns a string array of values within the nodes of xml that match the XPath expression.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn Xpath(String xml, String path) => new(FunctionCall2("xpath", false, xml, path));

	///AllColumn <Summary>Returns a string array of values within the nodes of xml that match the XPath expression.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn Xpath(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath", false, xml, path));

	///AllColumn <Summary>Returns true if the XPath expression evaluates to true, or if a matching node is found.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathBoolean(String xml, String path) => new(FunctionCall2("xpath_boolean", false, xml, path));

	///AllColumn <Summary>Returns true if the XPath expression evaluates to true, or if a matching node is found.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathBoolean(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath_boolean", false, xml, path));

	///AllColumn <Summary>Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathDouble(String xml, String path) => new(FunctionCall2("xpath_double", false, xml, path));

	///AllColumn <Summary>Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathDouble(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath_double", false, xml, path));

	///AllColumn <Summary>Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathNumber(String xml, String path) => new(FunctionCall2("xpath_number", false, xml, path));

	///AllColumn <Summary>Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathNumber(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath_number", false, xml, path));

	///AllColumn <Summary>Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathFloat(String xml, String path) => new(FunctionCall2("xpath_float", false, xml, path));

	///AllColumn <Summary>Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathFloat(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath_float", false, xml, path));

	///AllColumn <Summary>Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathInt(String xml, String path) => new(FunctionCall2("xpath_int", false, xml, path));

	///AllColumn <Summary>Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathInt(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath_int", false, xml, path));

	///AllColumn <Summary>Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathLong(String xml, String path) => new(FunctionCall2("xpath_long", false, xml, path));

	///AllColumn <Summary>Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathLong(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath_long", false, xml, path));

	///AllColumn <Summary>Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathShort(String xml, String path) => new(FunctionCall2("xpath_short", false, xml, path));

	///AllColumn <Summary>Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathShort(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath_short", false, xml, path));

	///AllColumn <Summary>Returns the text contents of the first xml node that matches the XPath expression.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathString(String xml, String path) => new(FunctionCall2("xpath_string", false, xml, path));

	///AllColumn <Summary>Returns the text contents of the first xml node that matches the XPath expression.</Summary>
	/// <param name="xml">'ColumnOrName'</param>
	/// <param name="path">'ColumnOrName'</param>
	public static SparkColumn XpathString(SparkColumn xml, SparkColumn path) => new(FunctionCall2("xpath_string", false, xml, path));

	/// <Summary>Column+SimpleType::Returns date truncated to the unit specified by the format.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="format">string</param>
	public static SparkColumn Trunc(String date, String format) => new(FunctionCall2("trunc", false, date, format));

	/// <Summary>Column+SimpleType::Returns date truncated to the unit specified by the format.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="format">string</param>
	public static SparkColumn Trunc(SparkColumn date, SparkColumn format) => new(FunctionCall2("trunc", false, date, format));

	/// <Summary>Column+SimpleType::Returns the first date which is later than the value of the date column based on second `week day` argument.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="dayOfWeek">string</param>
	public static SparkColumn NextDay(String date, String dayOfWeek) => new(FunctionCall2("next_day", false, date, dayOfWeek));

	/// <Summary>Column+SimpleType::Returns the first date which is later than the value of the date column based on second `week day` argument.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="dayOfWeek">string</param>
	public static SparkColumn NextDay(SparkColumn date, SparkColumn dayOfWeek) => new(FunctionCall2("next_day", false, date, dayOfWeek));

	///AllColumn <Summary>Returns the last day of the month which the given date belongs to.</Summary>
	/// <param name="date">'ColumnOrName'</param>
	public static SparkColumn LastDay(String date) => new(FunctionCall2("last_day", false, date));

	///AllColumn <Summary>Returns the last day of the month which the given date belongs to.</Summary>
	/// <param name="date">'ColumnOrName'</param>
	public static SparkColumn LastDay(SparkColumn date) => new(FunctionCall2("last_day", false, date));

	/// <Summary>Column+SimpleType::Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the given format.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="format">string</param>
	public static SparkColumn FromUnixtime(String timestamp, String format) => new(FunctionCall2("from_unixtime", false, timestamp, format));

	/// <Summary>Column+SimpleType::Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string representing the timestamp of that moment in the current system time zone in the given format.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="format">string</param>
	public static SparkColumn FromUnixtime(SparkColumn timestamp, SparkColumn format) => new(FunctionCall2("from_unixtime", false, timestamp, format));

	/// <Summary>Column+SimpleType::</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="format">string</param>
	public static SparkColumn UnixTimestamp(String timestamp, String format) => new(FunctionCall2("unix_timestamp", false, timestamp, format));

	/// <Summary>Column+SimpleType::</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="format">string</param>
	public static SparkColumn UnixTimestamp(SparkColumn timestamp, SparkColumn format) => new(FunctionCall2("unix_timestamp", false, timestamp, format));

	/// <Summary></Summary>
	public static SparkColumn UnixTimestamp() => new(FunctionCall2("unix_timestamp", false));

	///AllColumn <Summary>This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in UTC, and renders that timestamp as a timestamp in the given time zone.</Summary>
	/// <param name="timestamp">'ColumnOrName'</param>
	/// <param name="tz">'ColumnOrName'</param>
	public static SparkColumn FromUtcTimestamp(String timestamp, String tz) => new(FunctionCall2("from_utc_timestamp", false, timestamp, tz));

	///AllColumn <Summary>This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in UTC, and renders that timestamp as a timestamp in the given time zone.</Summary>
	/// <param name="timestamp">'ColumnOrName'</param>
	/// <param name="tz">'ColumnOrName'</param>
	public static SparkColumn FromUtcTimestamp(SparkColumn timestamp, SparkColumn tz) => new(FunctionCall2("from_utc_timestamp", false, timestamp, tz));

	///AllColumn <Summary>This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in the given timezone, and renders that timestamp as a timestamp in UTC.</Summary>
	/// <param name="timestamp">'ColumnOrName'</param>
	/// <param name="tz">'ColumnOrName'</param>
	public static SparkColumn ToUtcTimestamp(String timestamp, String tz) => new(FunctionCall2("to_utc_timestamp", false, timestamp, tz));

	///AllColumn <Summary>This is a common function for databases supporting TIMESTAMP WITHOUT TIMEZONE. This function takes a timestamp which is timezone-agnostic, and interprets it as a timestamp in the given timezone, and renders that timestamp as a timestamp in UTC.</Summary>
	/// <param name="timestamp">'ColumnOrName'</param>
	/// <param name="tz">'ColumnOrName'</param>
	public static SparkColumn ToUtcTimestamp(SparkColumn timestamp, SparkColumn tz) => new(FunctionCall2("to_utc_timestamp", false, timestamp, tz));

	///AllColumn <Summary>Converts the number of seconds from the Unix epoch (1970-01-01T00:00:00Z) to a timestamp.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TimestampSeconds(String col) => new(FunctionCall2("timestamp_seconds", false, col));

	///AllColumn <Summary>Converts the number of seconds from the Unix epoch (1970-01-01T00:00:00Z) to a timestamp.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TimestampSeconds(SparkColumn col) => new(FunctionCall2("timestamp_seconds", false, col));

	///AllColumn <Summary>Creates timestamp from the number of milliseconds since UTC epoch.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TimestampMillis(String col) => new(FunctionCall2("timestamp_millis", false, col));

	///AllColumn <Summary>Creates timestamp from the number of milliseconds since UTC epoch.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TimestampMillis(SparkColumn col) => new(FunctionCall2("timestamp_millis", false, col));

	///AllColumn <Summary>Creates timestamp from the number of microseconds since UTC epoch.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TimestampMicros(String col) => new(FunctionCall2("timestamp_micros", false, col));

	///AllColumn <Summary>Creates timestamp from the number of microseconds since UTC epoch.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn TimestampMicros(SparkColumn col) => new(FunctionCall2("timestamp_micros", false, col));

	///AllColumn <Summary>Computes the event time from a window column. The column window values are produced by window aggregating operators and are of type `STRUCT<start: TIMESTAMP, end: TIMESTAMP>` where start is inclusive and end is exclusive. The event time of records produced by window aggregating operators can be computed as ``window_time(window)`` and are ``window.end - lit(1).alias(QUOTmicrosecondQUOT)`` (as microsecond is the minimal supported event time precision). The window column must be one produced by a window aggregating operator.</Summary>
	/// <param name="windowColumn">'ColumnOrName'</param>
	public static SparkColumn WindowTime(String windowColumn) => new(FunctionCall2("window_time", false, windowColumn));

	///AllColumn <Summary>Computes the event time from a window column. The column window values are produced by window aggregating operators and are of type `STRUCT<start: TIMESTAMP, end: TIMESTAMP>` where start is inclusive and end is exclusive. The event time of records produced by window aggregating operators can be computed as ``window_time(window)`` and are ``window.end - lit(1).alias(QUOTmicrosecondQUOT)`` (as microsecond is the minimal supported event time precision). The window column must be one produced by a window aggregating operator.</Summary>
	/// <param name="windowColumn">'ColumnOrName'</param>
	public static SparkColumn WindowTime(SparkColumn windowColumn) => new(FunctionCall2("window_time", false, windowColumn));

	/// <Summary>Returns the current catalog.</Summary>
	public static SparkColumn CurrentCatalog() => new(FunctionCall2("current_catalog", false));

	/// <Summary>Returns the current database.</Summary>
	public static SparkColumn CurrentDatabase() => new(FunctionCall2("current_database", false));

	/// <Summary>Returns the current database.</Summary>
	public static SparkColumn CurrentSchema() => new(FunctionCall2("current_schema", false));

	/// <Summary>Returns the current database.</Summary>
	public static SparkColumn CurrentUser() => new(FunctionCall2("current_user", false));

	/// <Summary>Returns the current database.</Summary>
	public static SparkColumn User() => new(FunctionCall2("user", false));

	///AllColumn <Summary>Calculates the cyclic redundancy check value  (CRC32) of a binary column and returns the value as a bigint.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Crc32(String col) => new(FunctionCall2("crc32", false, col));

	///AllColumn <Summary>Calculates the cyclic redundancy check value  (CRC32) of a binary column and returns the value as a bigint.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Crc32(SparkColumn col) => new(FunctionCall2("crc32", false, col));

	///AllColumn <Summary>Calculates the MD5 digest and returns the value as a 32 character hex string.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Md5(String col) => new(FunctionCall2("md5", false, col));

	///AllColumn <Summary>Calculates the MD5 digest and returns the value as a 32 character hex string.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Md5(SparkColumn col) => new(FunctionCall2("md5", false, col));

	///AllColumn <Summary>Returns the hex string result of SHA-1.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sha1(String col) => new(FunctionCall2("sha1", false, col));

	///AllColumn <Summary>Returns the hex string result of SHA-1.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sha1(SparkColumn col) => new(FunctionCall2("sha1", false, col));

	/// <Summary>Column+SimpleType::Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512). The numBits indicates the desired bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which is equivalent to 256).</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="numBits">int</param>
	public static SparkColumn Sha2(String col, String numBits) => new(FunctionCall2("sha2", false, col, numBits));

	/// <Summary>Column+SimpleType::Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512). The numBits indicates the desired bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which is equivalent to 256).</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="numBits">int</param>
	public static SparkColumn Sha2(SparkColumn col, SparkColumn numBits) => new(FunctionCall2("sha2", false, col, numBits));

	///AllColumn <Summary>Calculates the hash code of given columns, and returns the result as an int column.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Hash(List<String> cols) => new(FunctionCall2("hash", false, cols));

	///AllColumn <Summary>Calculates the hash code of given columns, and returns the result as an int column.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Hash(List<SparkColumn> cols) => new(FunctionCall2("hash", false, cols));

	///AllColumn <Summary>Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm, and returns the result as a long column. The hash computation uses an initial seed of 42.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Xxhash64(List<String> cols) => new(FunctionCall2("xxhash64", false, cols));

	///AllColumn <Summary>Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm, and returns the result as a long column. The hash computation uses an initial seed of 42.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Xxhash64(List<SparkColumn> cols) => new(FunctionCall2("xxhash64", false, cols));

	///AllColumn <Summary>Converts a string expression to upper case.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Upper(String col) => new(FunctionCall2("upper", false, col));

	///AllColumn <Summary>Converts a string expression to upper case.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Upper(SparkColumn col) => new(FunctionCall2("upper", false, col));

	///AllColumn <Summary>Converts a string expression to lower case.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Lower(String col) => new(FunctionCall2("lower", false, col));

	///AllColumn <Summary>Converts a string expression to lower case.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Lower(SparkColumn col) => new(FunctionCall2("lower", false, col));

	///AllColumn <Summary>Computes the numeric value of the first character of the string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ascii(String col) => new(FunctionCall2("ascii", false, col));

	///AllColumn <Summary>Computes the numeric value of the first character of the string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ascii(SparkColumn col) => new(FunctionCall2("ascii", false, col));

	///AllColumn <Summary>Computes the BASE64 encoding of a binary column and returns it as a string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Base64(String col) => new(FunctionCall2("base64", false, col));

	///AllColumn <Summary>Computes the BASE64 encoding of a binary column and returns it as a string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Base64(SparkColumn col) => new(FunctionCall2("base64", false, col));

	///AllColumn <Summary>Decodes a BASE64 encoded string column and returns it as a binary column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Unbase64(String col) => new(FunctionCall2("unbase64", false, col));

	///AllColumn <Summary>Decodes a BASE64 encoded string column and returns it as a binary column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Unbase64(SparkColumn col) => new(FunctionCall2("unbase64", false, col));

	///AllColumn <Summary>Trim the spaces from left end for the specified string value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ltrim(String col) => new(FunctionCall2("ltrim", false, col));

	///AllColumn <Summary>Trim the spaces from left end for the specified string value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Ltrim(SparkColumn col) => new(FunctionCall2("ltrim", false, col));

	///AllColumn <Summary>Trim the spaces from right end for the specified string value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Rtrim(String col) => new(FunctionCall2("rtrim", false, col));

	///AllColumn <Summary>Trim the spaces from right end for the specified string value.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Rtrim(SparkColumn col) => new(FunctionCall2("rtrim", false, col));

	///AllColumn <Summary>Trim the spaces from both ends for the specified string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Trim(String col) => new(FunctionCall2("trim", false, col));

	///AllColumn <Summary>Trim the spaces from both ends for the specified string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Trim(SparkColumn col) => new(FunctionCall2("trim", false, col));

	/// <Summary>Column+SimpleType::Computes the first argument into a string from a binary using the provided character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="charset">string</param>
	public static SparkColumn Decode(String col, String charset) => new(FunctionCall2("decode", false, col, charset));

	/// <Summary>Column+SimpleType::Computes the first argument into a string from a binary using the provided character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="charset">string</param>
	public static SparkColumn Decode(SparkColumn col, SparkColumn charset) => new(FunctionCall2("decode", false, col, charset));

	/// <Summary>Column+SimpleType::Computes the first argument into a binary from a string using the provided character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="charset">string</param>
	public static SparkColumn Encode(String col, String charset) => new(FunctionCall2("encode", false, col, charset));

	/// <Summary>Column+SimpleType::Computes the first argument into a binary from a string using the provided character set (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="charset">string</param>
	public static SparkColumn Encode(SparkColumn col, SparkColumn charset) => new(FunctionCall2("encode", false, col, charset));

	/// <Summary>Column+SimpleType::Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places with HALF_EVEN round mode, and returns the result as a string.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="d">int</param>
	public static SparkColumn FormatNumber(String col, String d) => new(FunctionCall2("format_number", false, col, d));

	/// <Summary>Column+SimpleType::Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places with HALF_EVEN round mode, and returns the result as a string.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="d">int</param>
	public static SparkColumn FormatNumber(SparkColumn col, SparkColumn d) => new(FunctionCall2("format_number", false, col, d));

	/// <Summary>Column+SimpleType::Locate the position of the first occurrence of substr column in the given string. Returns null if either of the arguments are null.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="substr">string</param>
	public static SparkColumn Instr(String str, String substr) => new(FunctionCall2("instr", false, str, substr));

	/// <Summary>Column+SimpleType::Locate the position of the first occurrence of substr column in the given string. Returns null if either of the arguments are null.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="substr">string</param>
	public static SparkColumn Instr(SparkColumn str, SparkColumn substr) => new(FunctionCall2("instr", false, str, substr));

	/// <Summary>Column+SimpleType::Repeats a string column n times, and returns it as a new string column.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="n">int</param>
	public static SparkColumn Repeat(String col, String n) => new(FunctionCall2("repeat", false, col, n));

	/// <Summary>Column+SimpleType::Repeats a string column n times, and returns it as a new string column.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="n">int</param>
	public static SparkColumn Repeat(SparkColumn col, SparkColumn n) => new(FunctionCall2("repeat", false, col, n));

	///AllColumn <Summary>Returns true if `str` matches the Java regex `regexp`, or false otherwise.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn Rlike(String str, String regexp) => new(FunctionCall2("rlike", false, str, regexp));

	///AllColumn <Summary>Returns true if `str` matches the Java regex `regexp`, or false otherwise.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn Rlike(SparkColumn str, SparkColumn regexp) => new(FunctionCall2("rlike", false, str, regexp));

	///AllColumn <Summary>Returns true if `str` matches the Java regex `regexp`, or false otherwise.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn Regexp(String str, String regexp) => new(FunctionCall2("regexp", false, str, regexp));

	///AllColumn <Summary>Returns true if `str` matches the Java regex `regexp`, or false otherwise.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn Regexp(SparkColumn str, SparkColumn regexp) => new(FunctionCall2("regexp", false, str, regexp));

	///AllColumn <Summary>Returns true if `str` matches the Java regex `regexp`, or false otherwise.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn RegexpLike(String str, String regexp) => new(FunctionCall2("regexp_like", false, str, regexp));

	///AllColumn <Summary>Returns true if `str` matches the Java regex `regexp`, or false otherwise.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn RegexpLike(SparkColumn str, SparkColumn regexp) => new(FunctionCall2("regexp_like", false, str, regexp));

	///AllColumn <Summary>Returns a count of the number of times that the Java regex pattern `regexp` is matched in the string `str`.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn RegexpCount(String str, String regexp) => new(FunctionCall2("regexp_count", false, str, regexp));

	///AllColumn <Summary>Returns a count of the number of times that the Java regex pattern `regexp` is matched in the string `str`.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn RegexpCount(SparkColumn str, SparkColumn regexp) => new(FunctionCall2("regexp_count", false, str, regexp));

	///AllColumn <Summary>Returns the substring that matches the Java regex `regexp` within the string `str`. If the regular expression is not found, the result is null.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn RegexpSubstr(String str, String regexp) => new(FunctionCall2("regexp_substr", false, str, regexp));

	///AllColumn <Summary>Returns the substring that matches the Java regex `regexp` within the string `str`. If the regular expression is not found, the result is null.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="regexp">'ColumnOrName'</param>
	public static SparkColumn RegexpSubstr(SparkColumn str, SparkColumn regexp) => new(FunctionCall2("regexp_substr", false, str, regexp));

	///AllColumn <Summary>Translate the first letter of each word to upper case in the sentence.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Initcap(String col) => new(FunctionCall2("initcap", false, col));

	///AllColumn <Summary>Translate the first letter of each word to upper case in the sentence.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Initcap(SparkColumn col) => new(FunctionCall2("initcap", false, col));

	///AllColumn <Summary>Returns the SoundEx encoding for a string</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Soundex(String col) => new(FunctionCall2("soundex", false, col));

	///AllColumn <Summary>Returns the SoundEx encoding for a string</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Soundex(SparkColumn col) => new(FunctionCall2("soundex", false, col));

	///AllColumn <Summary>Returns the string representation of the binary value of the given column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Bin(String col) => new(FunctionCall2("bin", false, col));

	///AllColumn <Summary>Returns the string representation of the binary value of the given column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Bin(SparkColumn col) => new(FunctionCall2("bin", false, col));

	///AllColumn <Summary>Computes hex value of the given column, which could be :class:`pyspark.sql.types.StringType`, :class:`pyspark.sql.types.BinaryType`, :class:`pyspark.sql.types.IntegerType` or :class:`pyspark.sql.types.LongType`.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Hex(String col) => new(FunctionCall2("hex", false, col));

	///AllColumn <Summary>Computes hex value of the given column, which could be :class:`pyspark.sql.types.StringType`, :class:`pyspark.sql.types.BinaryType`, :class:`pyspark.sql.types.IntegerType` or :class:`pyspark.sql.types.LongType`.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Hex(SparkColumn col) => new(FunctionCall2("hex", false, col));

	///AllColumn <Summary>Inverse of hex. Interprets each pair of characters as a hexadecimal number and converts to the byte representation of number.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Unhex(String col) => new(FunctionCall2("unhex", false, col));

	///AllColumn <Summary>Inverse of hex. Interprets each pair of characters as a hexadecimal number and converts to the byte representation of number.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Unhex(SparkColumn col) => new(FunctionCall2("unhex", false, col));

	///AllColumn <Summary>Computes the character length of string data or number of bytes of binary data. The length of character data includes the trailing spaces. The length of binary data includes binary zeros.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Length(String col) => new(FunctionCall2("length", false, col));

	///AllColumn <Summary>Computes the character length of string data or number of bytes of binary data. The length of character data includes the trailing spaces. The length of binary data includes binary zeros.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Length(SparkColumn col) => new(FunctionCall2("length", false, col));

	///AllColumn <Summary>Calculates the byte length for the specified string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn OctetLength(String col) => new(FunctionCall2("octet_length", false, col));

	///AllColumn <Summary>Calculates the byte length for the specified string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn OctetLength(SparkColumn col) => new(FunctionCall2("octet_length", false, col));

	///AllColumn <Summary>Calculates the bit length for the specified string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitLength(String col) => new(FunctionCall2("bit_length", false, col));

	///AllColumn <Summary>Calculates the bit length for the specified string column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitLength(SparkColumn col) => new(FunctionCall2("bit_length", false, col));

	///AllColumn <Summary>Convert `col` to a string based on the `format`. Throws an exception if the conversion fails. The format can consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the format string matches a sequence of digits in the input value, generating a result string of the same length as the corresponding sequence in the format string. The result string is left-padded with zeros if the 0/9 sequence comprises more digits than the matching part of the decimal value, starts with 0, and is before the decimal point. Otherwise, it is padded with spaces. '.' or 'D': Specifies the position of the decimal point (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There must be a 0 or 9 to the left and right of each grouping separator. '$': Specifies the location of the $ currency sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at the beginning or end of the format string). Note that 'S' prints '+' for positive values but 'MI' prints a space. 'PR': Only allowed at the end of the format string; specifies that the result string will be wrapped by angle brackets if the input value is negative.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="format">'ColumnOrName'</param>
	public static SparkColumn ToChar(String col, String format) => new(FunctionCall2("to_char", false, col, format));

	///AllColumn <Summary>Convert `col` to a string based on the `format`. Throws an exception if the conversion fails. The format can consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the format string matches a sequence of digits in the input value, generating a result string of the same length as the corresponding sequence in the format string. The result string is left-padded with zeros if the 0/9 sequence comprises more digits than the matching part of the decimal value, starts with 0, and is before the decimal point. Otherwise, it is padded with spaces. '.' or 'D': Specifies the position of the decimal point (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There must be a 0 or 9 to the left and right of each grouping separator. '$': Specifies the location of the $ currency sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at the beginning or end of the format string). Note that 'S' prints '+' for positive values but 'MI' prints a space. 'PR': Only allowed at the end of the format string; specifies that the result string will be wrapped by angle brackets if the input value is negative.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="format">'ColumnOrName'</param>
	public static SparkColumn ToChar(SparkColumn col, SparkColumn format) => new(FunctionCall2("to_char", false, col, format));

	///AllColumn <Summary>Convert `col` to a string based on the `format`. Throws an exception if the conversion fails. The format can consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the format string matches a sequence of digits in the input value, generating a result string of the same length as the corresponding sequence in the format string. The result string is left-padded with zeros if the 0/9 sequence comprises more digits than the matching part of the decimal value, starts with 0, and is before the decimal point. Otherwise, it is padded with spaces. '.' or 'D': Specifies the position of the decimal point (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There must be a 0 or 9 to the left and right of each grouping separator. '$': Specifies the location of the $ currency sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at the beginning or end of the format string). Note that 'S' prints '+' for positive values but 'MI' prints a space. 'PR': Only allowed at the end of the format string; specifies that the result string will be wrapped by angle brackets if the input value is negative.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="format">'ColumnOrName'</param>
	public static SparkColumn ToVarchar(String col, String format) => new(FunctionCall2("to_varchar", false, col, format));

	///AllColumn <Summary>Convert `col` to a string based on the `format`. Throws an exception if the conversion fails. The format can consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the format string matches a sequence of digits in the input value, generating a result string of the same length as the corresponding sequence in the format string. The result string is left-padded with zeros if the 0/9 sequence comprises more digits than the matching part of the decimal value, starts with 0, and is before the decimal point. Otherwise, it is padded with spaces. '.' or 'D': Specifies the position of the decimal point (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There must be a 0 or 9 to the left and right of each grouping separator. '$': Specifies the location of the $ currency sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at the beginning or end of the format string). Note that 'S' prints '+' for positive values but 'MI' prints a space. 'PR': Only allowed at the end of the format string; specifies that the result string will be wrapped by angle brackets if the input value is negative.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="format">'ColumnOrName'</param>
	public static SparkColumn ToVarchar(SparkColumn col, SparkColumn format) => new(FunctionCall2("to_varchar", false, col, format));

	///AllColumn <Summary>Convert string 'col' to a number based on the string format 'format'. Throws an exception if the conversion fails. The format can consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the format string matches a sequence of digits in the input string. If the 0/9 sequence starts with 0 and is before the decimal point, it can only match a digit sequence of the same size. Otherwise, if the sequence starts with 9 or is after the decimal point, it can match a digit sequence that has the same or smaller size. '.' or 'D': Specifies the position of the decimal point (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There must be a 0 or 9 to the left and right of each grouping separator. 'col' must match the grouping separator relevant for the size of the number. '$': Specifies the location of the $ currency sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at the beginning or end of the format string). Note that 'S' allows '-' but 'MI' does not. 'PR': Only allowed at the end of the format string; specifies that 'col' indicates a negative number with wrapping angled brackets.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="format">'ColumnOrName'</param>
	public static SparkColumn ToNumber(String col, String format) => new(FunctionCall2("to_number", false, col, format));

	///AllColumn <Summary>Convert string 'col' to a number based on the string format 'format'. Throws an exception if the conversion fails. The format can consist of the following characters, case insensitive: '0' or '9': Specifies an expected digit between 0 and 9. A sequence of 0 or 9 in the format string matches a sequence of digits in the input string. If the 0/9 sequence starts with 0 and is before the decimal point, it can only match a digit sequence of the same size. Otherwise, if the sequence starts with 9 or is after the decimal point, it can match a digit sequence that has the same or smaller size. '.' or 'D': Specifies the position of the decimal point (optional, only allowed once). ',' or 'G': Specifies the position of the grouping (thousands) separator (,). There must be a 0 or 9 to the left and right of each grouping separator. 'col' must match the grouping separator relevant for the size of the number. '$': Specifies the location of the $ currency sign. This character may only be specified once. 'S' or 'MI': Specifies the position of a '-' or '+' sign (optional, only allowed once at the beginning or end of the format string). Note that 'S' allows '-' but 'MI' does not. 'PR': Only allowed at the end of the format string; specifies that 'col' indicates a negative number with wrapping angled brackets.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="format">'ColumnOrName'</param>
	public static SparkColumn ToNumber(SparkColumn col, SparkColumn format) => new(FunctionCall2("to_number", false, col, format));

	///AllColumn <Summary>Splits `str` by delimiter and return requested part of the split (1-based). If any input is null, returns null. if `partNum` is out of range of split parts, returns empty string. If `partNum` is 0, throws an error. If `partNum` is negative, the parts are counted backward from the end of the string. If the `delimiter` is an empty string, the `str` is not split.</Summary>
	/// <param name="src">'ColumnOrName'</param>
	/// <param name="delimiter">'ColumnOrName'</param>
	/// <param name="partNum">'ColumnOrName'</param>
	public static SparkColumn SplitPart(String src, String delimiter, String partNum) => new(FunctionCall2("split_part", false, src, delimiter, partNum));

	///AllColumn <Summary>Splits `str` by delimiter and return requested part of the split (1-based). If any input is null, returns null. if `partNum` is out of range of split parts, returns empty string. If `partNum` is 0, throws an error. If `partNum` is negative, the parts are counted backward from the end of the string. If the `delimiter` is an empty string, the `str` is not split.</Summary>
	/// <param name="src">'ColumnOrName'</param>
	/// <param name="delimiter">'ColumnOrName'</param>
	/// <param name="partNum">'ColumnOrName'</param>
	public static SparkColumn SplitPart(SparkColumn src, SparkColumn delimiter, SparkColumn partNum) => new(FunctionCall2("split_part", false, src, delimiter, partNum));

	///AllColumn <Summary>Decodes a `str` in 'application/x-www-form-urlencoded' format using a specific encoding scheme.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn UrlDecode(String str) => new(FunctionCall2("url_decode", false, str));

	///AllColumn <Summary>Decodes a `str` in 'application/x-www-form-urlencoded' format using a specific encoding scheme.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn UrlDecode(SparkColumn str) => new(FunctionCall2("url_decode", false, str));

	///AllColumn <Summary>Translates a string into 'application/x-www-form-urlencoded' format using a specific encoding scheme.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn UrlEncode(String str) => new(FunctionCall2("url_encode", false, str));

	///AllColumn <Summary>Translates a string into 'application/x-www-form-urlencoded' format using a specific encoding scheme.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn UrlEncode(SparkColumn str) => new(FunctionCall2("url_encode", false, str));

	///AllColumn <Summary>Returns a boolean. The value is True if str ends with suffix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or suffix must be of STRING or BINARY type.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="suffix">'ColumnOrName'</param>
	public static SparkColumn Endswith(String str, String suffix) => new(FunctionCall2("endswith", false, str, suffix));

	///AllColumn <Summary>Returns a boolean. The value is True if str ends with suffix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or suffix must be of STRING or BINARY type.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="suffix">'ColumnOrName'</param>
	public static SparkColumn Endswith(SparkColumn str, SparkColumn suffix) => new(FunctionCall2("endswith", false, str, suffix));

	///AllColumn <Summary>Returns a boolean. The value is True if str starts with prefix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or prefix must be of STRING or BINARY type.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="prefix">'ColumnOrName'</param>
	public static SparkColumn Startswith(String str, String prefix) => new(FunctionCall2("startswith", false, str, prefix));

	///AllColumn <Summary>Returns a boolean. The value is True if str starts with prefix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or prefix must be of STRING or BINARY type.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="prefix">'ColumnOrName'</param>
	public static SparkColumn Startswith(SparkColumn str, SparkColumn prefix) => new(FunctionCall2("startswith", false, str, prefix));

	///AllColumn <Summary>Returns the ASCII character having the binary equivalent to `col`. If col is larger than 256 the result is equivalent to char(col % 256)</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Char(String col) => new(FunctionCall2("char", false, col));

	///AllColumn <Summary>Returns the ASCII character having the binary equivalent to `col`. If col is larger than 256 the result is equivalent to char(col % 256)</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Char(SparkColumn col) => new(FunctionCall2("char", false, col));

	///AllColumn <Summary>Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn CharLength(String str) => new(FunctionCall2("char_length", false, str));

	///AllColumn <Summary>Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn CharLength(SparkColumn str) => new(FunctionCall2("char_length", false, str));

	///AllColumn <Summary>Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn CharacterLength(String str) => new(FunctionCall2("character_length", false, str));

	///AllColumn <Summary>Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn CharacterLength(SparkColumn str) => new(FunctionCall2("character_length", false, str));

	///AllColumn <Summary>Convert string 'col' to a number based on the string format `format`. Returns NULL if the string 'col' does not match the expected format. The format follows the same semantics as the to_number function.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="format">'ColumnOrName'</param>
	public static SparkColumn TryToNumber(String col, String format) => new(FunctionCall2("try_to_number", false, col, format));

	///AllColumn <Summary>Convert string 'col' to a number based on the string format `format`. Returns NULL if the string 'col' does not match the expected format. The format follows the same semantics as the to_number function.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="format">'ColumnOrName'</param>
	public static SparkColumn TryToNumber(SparkColumn col, SparkColumn format) => new(FunctionCall2("try_to_number", false, col, format));

	///AllColumn <Summary>Returns a boolean. The value is True if right is found inside left. Returns NULL if either input expression is NULL. Otherwise, returns False. Both left or right must be of STRING or BINARY type.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn Contains(String left, String right) => new(FunctionCall2("contains", false, left, right));

	///AllColumn <Summary>Returns a boolean. The value is True if right is found inside left. Returns NULL if either input expression is NULL. Otherwise, returns False. Both left or right must be of STRING or BINARY type.</Summary>
	/// <param name="left">'ColumnOrName'</param>
	/// <param name="right">'ColumnOrName'</param>
	public static SparkColumn Contains(SparkColumn left, SparkColumn right) => new(FunctionCall2("contains", false, left, right));

	///AllColumn <Summary>Returns the `n`-th input, e.g., returns `input2` when `n` is 2. The function returns NULL if the index exceeds the length of the array and `spark.sql.ansi.enabled` is set to false. If `spark.sql.ansi.enabled` is set to true, it throws ArrayIndexOutOfBoundsException for invalid indices.</Summary>
	/// <param name="inputs">List&lt;String&gt;</param>
	public static SparkColumn Elt(List<String> inputs) => new(FunctionCall2("elt", false, inputs));

	///AllColumn <Summary>Returns the `n`-th input, e.g., returns `input2` when `n` is 2. The function returns NULL if the index exceeds the length of the array and `spark.sql.ansi.enabled` is set to false. If `spark.sql.ansi.enabled` is set to true, it throws ArrayIndexOutOfBoundsException for invalid indices.</Summary>
	/// <param name="inputs">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Elt(List<SparkColumn> inputs) => new(FunctionCall2("elt", false, inputs));

	///AllColumn <Summary>Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`strArray`). Returns 0, if the string was not found or if the given string (`str`) contains a comma.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="str_array">'ColumnOrName'</param>
	public static SparkColumn FindInSet(String str, String str_array) => new(FunctionCall2("find_in_set", false, str, str_array));

	///AllColumn <Summary>Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`strArray`). Returns 0, if the string was not found or if the given string (`str`) contains a comma.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="str_array">'ColumnOrName'</param>
	public static SparkColumn FindInSet(SparkColumn str, SparkColumn str_array) => new(FunctionCall2("find_in_set", false, str, str_array));

	///AllColumn <Summary>Returns `str` with all characters changed to lowercase.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn Lcase(String str) => new(FunctionCall2("lcase", false, str));

	///AllColumn <Summary>Returns `str` with all characters changed to lowercase.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn Lcase(SparkColumn str) => new(FunctionCall2("lcase", false, str));

	///AllColumn <Summary>Returns `str` with all characters changed to uppercase.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn Ucase(String str) => new(FunctionCall2("ucase", false, str));

	///AllColumn <Summary>Returns `str` with all characters changed to uppercase.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	public static SparkColumn Ucase(SparkColumn str) => new(FunctionCall2("ucase", false, str));

	///AllColumn <Summary>Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="len">'ColumnOrName'</param>
	public static SparkColumn Left(String str, String len) => new(FunctionCall2("left", false, str, len));

	///AllColumn <Summary>Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="len">'ColumnOrName'</param>
	public static SparkColumn Left(SparkColumn str, SparkColumn len) => new(FunctionCall2("left", false, str, len));

	///AllColumn <Summary>Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="len">'ColumnOrName'</param>
	public static SparkColumn Right(String str, String len) => new(FunctionCall2("right", false, str, len));

	///AllColumn <Summary>Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.</Summary>
	/// <param name="str">'ColumnOrName'</param>
	/// <param name="len">'ColumnOrName'</param>
	public static SparkColumn Right(SparkColumn str, SparkColumn len) => new(FunctionCall2("right", false, str, len));

	///AllColumn <Summary></Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn CreateMap(List<String> cols) => new(FunctionCall2("create_map", false, cols));

	///AllColumn <Summary></Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn CreateMap(List<SparkColumn> cols) => new(FunctionCall2("create_map", false, cols));

	/// <Summary>Creates a new map column.</Summary>
	public static SparkColumn CreateMap() => new(FunctionCall2("create_map", false));

	///AllColumn <Summary>Creates a new map from two arrays.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn MapFromArrays(String col1, String col2) => new(FunctionCall2("map_from_arrays", false, col1, col2));

	///AllColumn <Summary>Creates a new map from two arrays.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn MapFromArrays(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("map_from_arrays", false, col1, col2));

	///AllColumn <Summary></Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Array(List<String> cols) => new(FunctionCall2("array", false, cols));

	///AllColumn <Summary></Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Array(List<SparkColumn> cols) => new(FunctionCall2("array", false, cols));

	/// <Summary>Creates a new array column.</Summary>
	public static SparkColumn Array() => new(FunctionCall2("array", false));

	///AllColumn <Summary>Collection function: returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise.</Summary>
	/// <param name="a1">'ColumnOrName'</param>
	/// <param name="a2">'ColumnOrName'</param>
	public static SparkColumn ArraysOverlap(String a1, String a2) => new(FunctionCall2("arrays_overlap", false, a1, a2));

	///AllColumn <Summary>Collection function: returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise.</Summary>
	/// <param name="a1">'ColumnOrName'</param>
	/// <param name="a2">'ColumnOrName'</param>
	public static SparkColumn ArraysOverlap(SparkColumn a1, SparkColumn a2) => new(FunctionCall2("arrays_overlap", false, a1, a2));

	///AllColumn <Summary>Concatenates multiple input columns together into a single column. The function works with strings, numeric, binary and compatible array columns.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Concat(List<String> cols) => new(FunctionCall2("concat", false, cols));

	///AllColumn <Summary>Concatenates multiple input columns together into a single column. The function works with strings, numeric, binary and compatible array columns.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Concat(List<SparkColumn> cols) => new(FunctionCall2("concat", false, cols));

	///AllColumn <Summary>(array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will throw an error. If index < 0, accesses elements from the last to the first. The function always returns NULL if the index exceeds the length of the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="extraction">'ColumnOrName'</param>
	public static SparkColumn TryElementAt(String col, String extraction) => new(FunctionCall2("try_element_at", false, col, extraction));

	///AllColumn <Summary>(array, index) - Returns element of array at given (1-based) index. If Index is 0, Spark will throw an error. If index < 0, accesses elements from the last to the first. The function always returns NULL if the index exceeds the length of the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	/// <param name="extraction">'ColumnOrName'</param>
	public static SparkColumn TryElementAt(SparkColumn col, SparkColumn extraction) => new(FunctionCall2("try_element_at", false, col, extraction));

	///AllColumn <Summary>Collection function: removes duplicate values from the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayDistinct(String col) => new(FunctionCall2("array_distinct", false, col));

	///AllColumn <Summary>Collection function: removes duplicate values from the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayDistinct(SparkColumn col) => new(FunctionCall2("array_distinct", false, col));

	///AllColumn <Summary>Collection function: returns an array of the elements in the intersection of col1 and col2, without duplicates.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn ArrayIntersect(String col1, String col2) => new(FunctionCall2("array_intersect", false, col1, col2));

	///AllColumn <Summary>Collection function: returns an array of the elements in the intersection of col1 and col2, without duplicates.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn ArrayIntersect(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("array_intersect", false, col1, col2));

	///AllColumn <Summary>Collection function: returns an array of the elements in the union of col1 and col2, without duplicates.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn ArrayUnion(String col1, String col2) => new(FunctionCall2("array_union", false, col1, col2));

	///AllColumn <Summary>Collection function: returns an array of the elements in the union of col1 and col2, without duplicates.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn ArrayUnion(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("array_union", false, col1, col2));

	///AllColumn <Summary>Collection function: returns an array of the elements in col1 but not in col2, without duplicates.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn ArrayExcept(String col1, String col2) => new(FunctionCall2("array_except", false, col1, col2));

	///AllColumn <Summary>Collection function: returns an array of the elements in col1 but not in col2, without duplicates.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn ArrayExcept(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("array_except", false, col1, col2));

	///AllColumn <Summary>Collection function: removes null values from the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayCompact(String col) => new(FunctionCall2("array_compact", false, col));

	///AllColumn <Summary>Collection function: removes null values from the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayCompact(SparkColumn col) => new(FunctionCall2("array_compact", false, col));

	///AllColumn <Summary>Returns a new row for each element in the given array or map. Uses the default column name `col` for elements in the array and `key` and `value` for elements in the map unless specified otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Explode(String col) => new(FunctionCall2("explode", false, col));

	///AllColumn <Summary>Returns a new row for each element in the given array or map. Uses the default column name `col` for elements in the array and `key` and `value` for elements in the map unless specified otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Explode(SparkColumn col) => new(FunctionCall2("explode", false, col));

	///AllColumn <Summary>Returns a new row for each element with position in the given array or map. Uses the default column name `pos` for position, and `col` for elements in the array and `key` and `value` for elements in the map unless specified otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Posexplode(String col) => new(FunctionCall2("posexplode", false, col));

	///AllColumn <Summary>Returns a new row for each element with position in the given array or map. Uses the default column name `pos` for position, and `col` for elements in the array and `key` and `value` for elements in the map unless specified otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Posexplode(SparkColumn col) => new(FunctionCall2("posexplode", false, col));

	///AllColumn <Summary>Explodes an array of structs into a table.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Inline(String col) => new(FunctionCall2("inline", false, col));

	///AllColumn <Summary>Explodes an array of structs into a table.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Inline(SparkColumn col) => new(FunctionCall2("inline", false, col));

	///AllColumn <Summary>Returns a new row for each element in the given array or map. Unlike explode, if the array/map is null or empty then null is produced. Uses the default column name `col` for elements in the array and `key` and `value` for elements in the map unless specified otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ExplodeOuter(String col) => new(FunctionCall2("explode_outer", false, col));

	///AllColumn <Summary>Returns a new row for each element in the given array or map. Unlike explode, if the array/map is null or empty then null is produced. Uses the default column name `col` for elements in the array and `key` and `value` for elements in the map unless specified otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ExplodeOuter(SparkColumn col) => new(FunctionCall2("explode_outer", false, col));

	///AllColumn <Summary>Returns a new row for each element with position in the given array or map. Unlike posexplode, if the array/map is null or empty then the row (null, null) is produced. Uses the default column name `pos` for position, and `col` for elements in the array and `key` and `value` for elements in the map unless specified otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn PosexplodeOuter(String col) => new(FunctionCall2("posexplode_outer", false, col));

	///AllColumn <Summary>Returns a new row for each element with position in the given array or map. Unlike posexplode, if the array/map is null or empty then the row (null, null) is produced. Uses the default column name `pos` for position, and `col` for elements in the array and `key` and `value` for elements in the map unless specified otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn PosexplodeOuter(SparkColumn col) => new(FunctionCall2("posexplode_outer", false, col));

	///AllColumn <Summary>Explodes an array of structs into a table. Unlike inline, if the array is null or empty then null is produced for each nested column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn InlineOuter(String col) => new(FunctionCall2("inline_outer", false, col));

	///AllColumn <Summary>Explodes an array of structs into a table. Unlike inline, if the array is null or empty then null is produced for each nested column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn InlineOuter(SparkColumn col) => new(FunctionCall2("inline_outer", false, col));

	/// <Summary>Column+SimpleType::Extracts json object from a json string based on json `path` specified, and returns json string of the extracted json object. It will return null if the input json string is invalid.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="path">string</param>
	public static SparkColumn GetJsonObject(String col, String path) => new(FunctionCall2("get_json_object", false, col, path));

	/// <Summary>Column+SimpleType::Extracts json object from a json string based on json `path` specified, and returns json string of the extracted json object. It will return null if the input json string is invalid.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="path">string</param>
	public static SparkColumn GetJsonObject(SparkColumn col, SparkColumn path) => new(FunctionCall2("get_json_object", false, col, path));

	///AllColumn <Summary>Creates a new row for a json column according to the given field names.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn JsonTuple(String col) => new(FunctionCall2("json_tuple", false, col));

	///AllColumn <Summary>Creates a new row for a json column according to the given field names.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn JsonTuple(SparkColumn col) => new(FunctionCall2("json_tuple", false, col));

	///AllColumn <Summary>Returns the number of elements in the outermost JSON array. `NULL` is returned in case of any other valid JSON string, `NULL` or an invalid JSON.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn JsonArrayLength(String col) => new(FunctionCall2("json_array_length", false, col));

	///AllColumn <Summary>Returns the number of elements in the outermost JSON array. `NULL` is returned in case of any other valid JSON string, `NULL` or an invalid JSON.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn JsonArrayLength(SparkColumn col) => new(FunctionCall2("json_array_length", false, col));

	///AllColumn <Summary>Returns all the keys of the outermost JSON object as an array. If a valid JSON object is given, all the keys of the outermost object will be returned as an array. If it is any other valid JSON string, an invalid JSON string or an empty string, the function returns null.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn JsonObjectKeys(String col) => new(FunctionCall2("json_object_keys", false, col));

	///AllColumn <Summary>Returns all the keys of the outermost JSON object as an array. If a valid JSON object is given, all the keys of the outermost object will be returned as an array. If it is any other valid JSON string, an invalid JSON string or an empty string, the function returns null.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn JsonObjectKeys(SparkColumn col) => new(FunctionCall2("json_object_keys", false, col));

	///AllColumn <Summary>Collection function: returns the length of the array or map stored in the column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Size(String col) => new(FunctionCall2("size", false, col));

	///AllColumn <Summary>Collection function: returns the length of the array or map stored in the column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Size(SparkColumn col) => new(FunctionCall2("size", false, col));

	///AllColumn <Summary>Collection function: returns the minimum value of the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayMin(String col) => new(FunctionCall2("array_min", false, col));

	///AllColumn <Summary>Collection function: returns the minimum value of the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayMin(SparkColumn col) => new(FunctionCall2("array_min", false, col));

	///AllColumn <Summary>Collection function: returns the maximum value of the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayMax(String col) => new(FunctionCall2("array_max", false, col));

	///AllColumn <Summary>Collection function: returns the maximum value of the array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArrayMax(SparkColumn col) => new(FunctionCall2("array_max", false, col));

	///AllColumn <Summary>Returns the total number of elements in the array. The function returns null for null input.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArraySize(String col) => new(FunctionCall2("array_size", false, col));

	///AllColumn <Summary>Returns the total number of elements in the array. The function returns null for null input.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn ArraySize(SparkColumn col) => new(FunctionCall2("array_size", false, col));

	///AllColumn <Summary>Collection function: returns the length of the array or map stored in the column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cardinality(String col) => new(FunctionCall2("cardinality", false, col));

	///AllColumn <Summary>Collection function: returns the length of the array or map stored in the column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Cardinality(SparkColumn col) => new(FunctionCall2("cardinality", false, col));

	/// <Summary>Column+SimpleType::Collection function: sorts the input array in ascending or descending order according to the natural ordering of the array elements. Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order.</Summary>
	/// <param name="col">String, Column Name</param>
	/// <param name="asc">bool</param>
	public static SparkColumn SortArray(String col, String asc) => new(FunctionCall2("sort_array", false, col, asc));

	/// <Summary>Column+SimpleType::Collection function: sorts the input array in ascending or descending order according to the natural ordering of the array elements. Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order.</Summary>
	/// <param name="col">SparkColumn</param>
	/// <param name="asc">bool</param>
	public static SparkColumn SortArray(SparkColumn col, SparkColumn asc) => new(FunctionCall2("sort_array", false, col, asc));

	///AllColumn <Summary>Collection function: Generates a random permutation of the given array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Shuffle(String col) => new(FunctionCall2("shuffle", false, col));

	///AllColumn <Summary>Collection function: Generates a random permutation of the given array.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Shuffle(SparkColumn col) => new(FunctionCall2("shuffle", false, col));

	///AllColumn <Summary>Collection function: returns a reversed string or an array with reverse order of elements.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Reverse(String col) => new(FunctionCall2("reverse", false, col));

	///AllColumn <Summary>Collection function: returns a reversed string or an array with reverse order of elements.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Reverse(SparkColumn col) => new(FunctionCall2("reverse", false, col));

	///AllColumn <Summary>Collection function: creates a single array from an array of arrays. If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Flatten(String col) => new(FunctionCall2("flatten", false, col));

	///AllColumn <Summary>Collection function: creates a single array from an array of arrays. If a structure of nested arrays is deeper than two levels, only one level of nesting is removed.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Flatten(SparkColumn col) => new(FunctionCall2("flatten", false, col));

	///AllColumn <Summary>Collection function: Returns an unordered array containing the keys of the map.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn MapKeys(String col) => new(FunctionCall2("map_keys", false, col));

	///AllColumn <Summary>Collection function: Returns an unordered array containing the keys of the map.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn MapKeys(SparkColumn col) => new(FunctionCall2("map_keys", false, col));

	///AllColumn <Summary>Collection function: Returns an unordered array containing the values of the map.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn MapValues(String col) => new(FunctionCall2("map_values", false, col));

	///AllColumn <Summary>Collection function: Returns an unordered array containing the values of the map.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn MapValues(SparkColumn col) => new(FunctionCall2("map_values", false, col));

	///AllColumn <Summary>Collection function: Returns an unordered array of all entries in the given map.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn MapEntries(String col) => new(FunctionCall2("map_entries", false, col));

	///AllColumn <Summary>Collection function: Returns an unordered array of all entries in the given map.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn MapEntries(SparkColumn col) => new(FunctionCall2("map_entries", false, col));

	///AllColumn <Summary>Collection function: Converts an array of entries (key value struct types) to a map of values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn MapFromEntries(String col) => new(FunctionCall2("map_from_entries", false, col));

	///AllColumn <Summary>Collection function: Converts an array of entries (key value struct types) to a map of values.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn MapFromEntries(SparkColumn col) => new(FunctionCall2("map_from_entries", false, col));

	///AllColumn <Summary>Collection function: Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays. If one of the arrays is shorter than others then resulting struct type value will be a `null` for missing elements.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn ArraysZip(List<String> cols) => new(FunctionCall2("arrays_zip", false, cols));

	///AllColumn <Summary>Collection function: Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays. If one of the arrays is shorter than others then resulting struct type value will be a `null` for missing elements.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn ArraysZip(List<SparkColumn> cols) => new(FunctionCall2("arrays_zip", false, cols));

	///AllColumn <Summary></Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn MapConcat(List<String> cols) => new(FunctionCall2("map_concat", false, cols));

	///AllColumn <Summary></Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn MapConcat(List<SparkColumn> cols) => new(FunctionCall2("map_concat", false, cols));

	/// <Summary>Returns the union of all the given maps.</Summary>
	public static SparkColumn MapConcat() => new(FunctionCall2("map_concat", false));

	///AllColumn <Summary>Partition transform function: A transform for timestamps and dates to partition data into years.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Years(String col) => new(FunctionCall2("years", false, col));

	///AllColumn <Summary>Partition transform function: A transform for timestamps and dates to partition data into years.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Years(SparkColumn col) => new(FunctionCall2("years", false, col));

	///AllColumn <Summary>Partition transform function: A transform for timestamps and dates to partition data into months.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Months(String col) => new(FunctionCall2("months", false, col));

	///AllColumn <Summary>Partition transform function: A transform for timestamps and dates to partition data into months.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Months(SparkColumn col) => new(FunctionCall2("months", false, col));

	///AllColumn <Summary>Partition transform function: A transform for timestamps and dates to partition data into days.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Days(String col) => new(FunctionCall2("days", false, col));

	///AllColumn <Summary>Partition transform function: A transform for timestamps and dates to partition data into days.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Days(SparkColumn col) => new(FunctionCall2("days", false, col));

	///AllColumn <Summary>Partition transform function: A transform for timestamps to partition data into hours.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Hours(String col) => new(FunctionCall2("hours", false, col));

	///AllColumn <Summary>Partition transform function: A transform for timestamps to partition data into hours.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Hours(SparkColumn col) => new(FunctionCall2("hours", false, col));

	///AllColumn <Summary>Create local date-time from years, months, days, hours, mins, secs fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.</Summary>
	/// <param name="years">'ColumnOrName'</param>
	/// <param name="months">'ColumnOrName'</param>
	/// <param name="days">'ColumnOrName'</param>
	/// <param name="hours">'ColumnOrName'</param>
	/// <param name="mins">'ColumnOrName'</param>
	/// <param name="secs">'ColumnOrName'</param>
	public static SparkColumn MakeTimestampNtz(String years, String months, String days, String hours, String mins, String secs) => new(FunctionCall2("make_timestamp_ntz", false, years, months, days, hours, mins, secs));

	///AllColumn <Summary>Create local date-time from years, months, days, hours, mins, secs fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.</Summary>
	/// <param name="years">'ColumnOrName'</param>
	/// <param name="months">'ColumnOrName'</param>
	/// <param name="days">'ColumnOrName'</param>
	/// <param name="hours">'ColumnOrName'</param>
	/// <param name="mins">'ColumnOrName'</param>
	/// <param name="secs">'ColumnOrName'</param>
	public static SparkColumn MakeTimestampNtz(SparkColumn years, SparkColumn months, SparkColumn days, SparkColumn hours, SparkColumn mins, SparkColumn secs) => new(FunctionCall2("make_timestamp_ntz", false, years, months, days, hours, mins, secs));

	///AllColumn <Summary>Unwrap UDT data type column into its underlying type.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnwrapUdt(String col) => new(FunctionCall2("unwrap_udt", false, col));

	///AllColumn <Summary>Unwrap UDT data type column into its underlying type.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn UnwrapUdt(SparkColumn col) => new(FunctionCall2("unwrap_udt", false, col));

	///AllColumn <Summary>Returns the estimated number of unique values given the binary representation of a Datasketches HllSketch.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn HllSketchEstimate(String col) => new(FunctionCall2("hll_sketch_estimate", false, col));

	///AllColumn <Summary>Returns the estimated number of unique values given the binary representation of a Datasketches HllSketch.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn HllSketchEstimate(SparkColumn col) => new(FunctionCall2("hll_sketch_estimate", false, col));

	///AllColumn <Summary>Returns `col2` if `col1` is null, or `col1` otherwise.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Ifnull(String col1, String col2) => new(FunctionCall2("ifnull", false, col1, col2));

	///AllColumn <Summary>Returns `col2` if `col1` is null, or `col1` otherwise.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Ifnull(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("ifnull", false, col1, col2));

	///AllColumn <Summary>Returns true if `col` is not null, or false otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Isnotnull(String col) => new(FunctionCall2("isnotnull", false, col));

	///AllColumn <Summary>Returns true if `col` is not null, or false otherwise.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Isnotnull(SparkColumn col) => new(FunctionCall2("isnotnull", false, col));

	///AllColumn <Summary>Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn EqualNull(String col1, String col2) => new(FunctionCall2("equal_null", false, col1, col2));

	///AllColumn <Summary>Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn EqualNull(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("equal_null", false, col1, col2));

	///AllColumn <Summary>Returns null if `col1` equals to `col2`, or `col1` otherwise.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Nullif(String col1, String col2) => new(FunctionCall2("nullif", false, col1, col2));

	///AllColumn <Summary>Returns null if `col1` equals to `col2`, or `col1` otherwise.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Nullif(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("nullif", false, col1, col2));

	///AllColumn <Summary>Returns `col2` if `col1` is null, or `col1` otherwise.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Nvl(String col1, String col2) => new(FunctionCall2("nvl", false, col1, col2));

	///AllColumn <Summary>Returns `col2` if `col1` is null, or `col1` otherwise.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	public static SparkColumn Nvl(SparkColumn col1, SparkColumn col2) => new(FunctionCall2("nvl", false, col1, col2));

	///AllColumn <Summary>Returns `col2` if `col1` is not null, or `col3` otherwise.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	/// <param name="col3">'ColumnOrName'</param>
	public static SparkColumn Nvl2(String col1, String col2, String col3) => new(FunctionCall2("nvl2", false, col1, col2, col3));

	///AllColumn <Summary>Returns `col2` if `col1` is not null, or `col3` otherwise.</Summary>
	/// <param name="col1">'ColumnOrName'</param>
	/// <param name="col2">'ColumnOrName'</param>
	/// <param name="col3">'ColumnOrName'</param>
	public static SparkColumn Nvl2(SparkColumn col1, SparkColumn col2, SparkColumn col3) => new(FunctionCall2("nvl2", false, col1, col2, col3));

	///AllColumn <Summary>Returns a sha1 hash value as a hex string of the `col`.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sha(String col) => new(FunctionCall2("sha", false, col));

	///AllColumn <Summary>Returns a sha1 hash value as a hex string of the `col`.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Sha(SparkColumn col) => new(FunctionCall2("sha", false, col));

	/// <Summary>Returns the length of the block being read, or -1 if not available.</Summary>
	public static SparkColumn InputFileBlockLength() => new(FunctionCall2("input_file_block_length", false));

	/// <Summary>Returns the start offset of the block being read, or -1 if not available.</Summary>
	public static SparkColumn InputFileBlockStart() => new(FunctionCall2("input_file_block_start", false));

	///AllColumn <Summary>Calls a method with reflection.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Reflect(List<String> cols) => new(FunctionCall2("reflect", false, cols));

	///AllColumn <Summary>Calls a method with reflection.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Reflect(List<SparkColumn> cols) => new(FunctionCall2("reflect", false, cols));

	/// <Summary>Returns the Spark version. The string contains 2 fields, the first being a release version and the second being a git revision.</Summary>
	public static SparkColumn Version() => new(FunctionCall2("version", false));

	///AllColumn <Summary>Return DDL-formatted type string for the data type of the input.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Typeof(String col) => new(FunctionCall2("typeof", false, col));

	///AllColumn <Summary>Return DDL-formatted type string for the data type of the input.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn Typeof(SparkColumn col) => new(FunctionCall2("typeof", false, col));

	///AllColumn <Summary>Separates `col1`, ..., `colk` into `n` rows. Uses column names col0, col1, etc. by default unless specified otherwise.</Summary>
	/// <param name="cols">List&lt;String&gt;</param>
	public static SparkColumn Stack(List<String> cols) => new(FunctionCall2("stack", false, cols));

	///AllColumn <Summary>Separates `col1`, ..., `colk` into `n` rows. Uses column names col0, col1, etc. by default unless specified otherwise.</Summary>
	/// <param name="cols">List&lt;SparkColumn&gt;</param>
	public static SparkColumn Stack(List<SparkColumn> cols) => new(FunctionCall2("stack", false, cols));

	///AllColumn <Summary>Returns the bit position for the given input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapBitPosition(String col) => new(FunctionCall2("bitmap_bit_position", false, col));

	///AllColumn <Summary>Returns the bit position for the given input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapBitPosition(SparkColumn col) => new(FunctionCall2("bitmap_bit_position", false, col));

	///AllColumn <Summary>Returns the bucket number for the given input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapBucketNumber(String col) => new(FunctionCall2("bitmap_bucket_number", false, col));

	///AllColumn <Summary>Returns the bucket number for the given input column.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapBucketNumber(SparkColumn col) => new(FunctionCall2("bitmap_bucket_number", false, col));

	///AllColumn <Summary>Returns a bitmap with the positions of the bits set from all the values from the input column. The input column will most likely be bitmap_bit_position().</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapConstructAgg(String col) => new(FunctionCall2("bitmap_construct_agg", false, col));

	///AllColumn <Summary>Returns a bitmap with the positions of the bits set from all the values from the input column. The input column will most likely be bitmap_bit_position().</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapConstructAgg(SparkColumn col) => new(FunctionCall2("bitmap_construct_agg", false, col));

	///AllColumn <Summary>Returns the number of set bits in the input bitmap.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapCount(String col) => new(FunctionCall2("bitmap_count", false, col));

	///AllColumn <Summary>Returns the number of set bits in the input bitmap.</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapCount(SparkColumn col) => new(FunctionCall2("bitmap_count", false, col));

	///AllColumn <Summary>Returns a bitmap that is the bitwise OR of all of the bitmaps from the input column. The input column should be bitmaps created from bitmap_construct_agg().</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapOrAgg(String col) => new(FunctionCall2("bitmap_or_agg", false, col));

	///AllColumn <Summary>Returns a bitmap that is the bitwise OR of all of the bitmaps from the input column. The input column should be bitmaps created from bitmap_construct_agg().</Summary>
	/// <param name="col">'ColumnOrName'</param>
	public static SparkColumn BitmapOrAgg(SparkColumn col) => new(FunctionCall2("bitmap_or_agg", false, col));

}
