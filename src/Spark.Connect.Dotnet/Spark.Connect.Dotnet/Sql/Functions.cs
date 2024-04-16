
namespace Spark.Connect.Dotnet.Sql;

public partial class Functions : FunctionsWrapper
{


    /// <Summary>Asc</Summary>
    public static SparkColumn Asc(string col) => new(FunctionWrappedCall("asc", false, col));

    /// <Summary>Asc</Summary>
	public static SparkColumn Asc(SparkColumn col) => new(FunctionWrappedCall("asc", false, col));

    /// <Summary>Asc</Summary>
	public static SparkColumn Asc(Expression col) => new(FunctionWrappedCall("asc", false, col));

    /// <Summary>Desc</Summary>
    public static SparkColumn Desc(string col) => new(FunctionWrappedCall("desc", false, col));

    /// <Summary>Desc</Summary>
	public static SparkColumn Desc(SparkColumn col) => new(FunctionWrappedCall("desc", false, col));

    /// <Summary>Desc</Summary>
	public static SparkColumn Desc(Expression col) => new(FunctionWrappedCall("desc", false, col));

    /// <Summary>Sqrt</Summary>
    public static SparkColumn Sqrt(string col) => new(FunctionWrappedCall("sqrt", false, col));

    /// <Summary>Sqrt</Summary>
	public static SparkColumn Sqrt(SparkColumn col) => new(FunctionWrappedCall("sqrt", false, col));

    /// <Summary>Sqrt</Summary>
	public static SparkColumn Sqrt(Expression col) => new(FunctionWrappedCall("sqrt", false, col));

    /// <Summary>
    /// TryAdd
    /// Returns the sum of `left`and `right` and the result is null on overflow. The acceptable input types are the same with the `+` operator.
    /// </Summary>
    public static SparkColumn TryAdd(string left,string right) => new(FunctionWrappedCall("try_add", false, left,right));

    /// <Summary>
    /// TryAdd
    /// Returns the sum of `left`and `right` and the result is null on overflow. The acceptable input types are the same with the `+` operator.
    /// </Summary>
    
	public static SparkColumn TryAdd(SparkColumn left,SparkColumn right) => new(FunctionWrappedCall("try_add", false, left,right));

    /// <Summary>
    /// TryAdd
    /// Returns the sum of `left`and `right` and the result is null on overflow. The acceptable input types are the same with the `+` operator.
    /// </Summary>
    
	public static SparkColumn TryAdd(Expression left,Expression right) => new(FunctionWrappedCall("try_add", false, left,right));

    /// <Summary>TryAvg</Summary>
    public static SparkColumn TryAvg(string col) => new(FunctionWrappedCall("try_avg", false, col));

    /// <Summary>TryAvg</Summary>
	public static SparkColumn TryAvg(SparkColumn col) => new(FunctionWrappedCall("try_avg", false, col));

    /// <Summary>TryAvg</Summary>
	public static SparkColumn TryAvg(Expression col) => new(FunctionWrappedCall("try_avg", false, col));

    /// <Summary>
    /// TryDivide
    /// Returns `dividend`/`divisor`. It always performs floating point division. Its result is always null if `divisor` is 0.
    /// </Summary>
    public static SparkColumn TryDivide(string left,string right) => new(FunctionWrappedCall("try_divide", false, left,right));

    /// <Summary>
    /// TryDivide
    /// Returns `dividend`/`divisor`. It always performs floating point division. Its result is always null if `divisor` is 0.
    /// </Summary>
    
	public static SparkColumn TryDivide(SparkColumn left,SparkColumn right) => new(FunctionWrappedCall("try_divide", false, left,right));

    /// <Summary>
    /// TryDivide
    /// Returns `dividend`/`divisor`. It always performs floating point division. Its result is always null if `divisor` is 0.
    /// </Summary>
    
	public static SparkColumn TryDivide(Expression left,Expression right) => new(FunctionWrappedCall("try_divide", false, left,right));

    /// <Summary>
    /// TryMultiply
    /// Returns `left`*`right` and the result is null on overflow. The acceptable input types are the same with the `*` operator.
    /// </Summary>
    public static SparkColumn TryMultiply(string left,string right) => new(FunctionWrappedCall("try_multiply", false, left,right));

    /// <Summary>
    /// TryMultiply
    /// Returns `left`*`right` and the result is null on overflow. The acceptable input types are the same with the `*` operator.
    /// </Summary>
    
	public static SparkColumn TryMultiply(SparkColumn left,SparkColumn right) => new(FunctionWrappedCall("try_multiply", false, left,right));

    /// <Summary>
    /// TryMultiply
    /// Returns `left`*`right` and the result is null on overflow. The acceptable input types are the same with the `*` operator.
    /// </Summary>
    
	public static SparkColumn TryMultiply(Expression left,Expression right) => new(FunctionWrappedCall("try_multiply", false, left,right));

    /// <Summary>
    /// TrySubtract
    /// Returns `left`-`right` and the result is null on overflow. The acceptable input types are the same with the `-` operator.
    /// </Summary>
    public static SparkColumn TrySubtract(string left,string right) => new(FunctionWrappedCall("try_subtract", false, left,right));

    /// <Summary>
    /// TrySubtract
    /// Returns `left`-`right` and the result is null on overflow. The acceptable input types are the same with the `-` operator.
    /// </Summary>
    
	public static SparkColumn TrySubtract(SparkColumn left,SparkColumn right) => new(FunctionWrappedCall("try_subtract", false, left,right));

    /// <Summary>
    /// TrySubtract
    /// Returns `left`-`right` and the result is null on overflow. The acceptable input types are the same with the `-` operator.
    /// </Summary>
    
	public static SparkColumn TrySubtract(Expression left,Expression right) => new(FunctionWrappedCall("try_subtract", false, left,right));

    /// <Summary>TrySum</Summary>
    public static SparkColumn TrySum(string col) => new(FunctionWrappedCall("try_sum", false, col));

    /// <Summary>TrySum</Summary>
	public static SparkColumn TrySum(SparkColumn col) => new(FunctionWrappedCall("try_sum", false, col));

    /// <Summary>TrySum</Summary>
	public static SparkColumn TrySum(Expression col) => new(FunctionWrappedCall("try_sum", false, col));

    /// <Summary>Abs</Summary>
    public static SparkColumn Abs(string col) => new(FunctionWrappedCall("abs", false, col));

    /// <Summary>Abs</Summary>
	public static SparkColumn Abs(SparkColumn col) => new(FunctionWrappedCall("abs", false, col));

    /// <Summary>Abs</Summary>
	public static SparkColumn Abs(Expression col) => new(FunctionWrappedCall("abs", false, col));

    /// <Summary>Mode</Summary>
    public static SparkColumn Mode(string col) => new(FunctionWrappedCall("mode", false, col));

    /// <Summary>Mode</Summary>
	public static SparkColumn Mode(SparkColumn col) => new(FunctionWrappedCall("mode", false, col));

    /// <Summary>Mode</Summary>
	public static SparkColumn Mode(Expression col) => new(FunctionWrappedCall("mode", false, col));

    /// <Summary>Max</Summary>
    public static SparkColumn Max(string col) => new(FunctionWrappedCall("max", false, col));

    /// <Summary>Max</Summary>
	public static SparkColumn Max(SparkColumn col) => new(FunctionWrappedCall("max", false, col));

    /// <Summary>Max</Summary>
	public static SparkColumn Max(Expression col) => new(FunctionWrappedCall("max", false, col));

    /// <Summary>Min</Summary>
    public static SparkColumn Min(string col) => new(FunctionWrappedCall("min", false, col));

    /// <Summary>Min</Summary>
	public static SparkColumn Min(SparkColumn col) => new(FunctionWrappedCall("min", false, col));

    /// <Summary>Min</Summary>
	public static SparkColumn Min(Expression col) => new(FunctionWrappedCall("min", false, col));

    /// <Summary>
    /// MaxBy
    /// Returns the value associated with the maximum value of ord.
    /// </Summary>
    public static SparkColumn MaxBy(string col,string ord) => new(FunctionWrappedCall("max_by", false, col,ord));

    /// <Summary>
    /// MaxBy
    /// Returns the value associated with the maximum value of ord.
    /// </Summary>
    
	public static SparkColumn MaxBy(SparkColumn col,SparkColumn ord) => new(FunctionWrappedCall("max_by", false, col,ord));

    /// <Summary>
    /// MaxBy
    /// Returns the value associated with the maximum value of ord.
    /// </Summary>
    
	public static SparkColumn MaxBy(Expression col,Expression ord) => new(FunctionWrappedCall("max_by", false, col,ord));

    /// <Summary>
    /// MinBy
    /// Returns the value associated with the minimum value of ord.
    /// </Summary>
    public static SparkColumn MinBy(string col,string ord) => new(FunctionWrappedCall("min_by", false, col,ord));

    /// <Summary>
    /// MinBy
    /// Returns the value associated with the minimum value of ord.
    /// </Summary>
    
	public static SparkColumn MinBy(SparkColumn col,SparkColumn ord) => new(FunctionWrappedCall("min_by", false, col,ord));

    /// <Summary>
    /// MinBy
    /// Returns the value associated with the minimum value of ord.
    /// </Summary>
    
	public static SparkColumn MinBy(Expression col,Expression ord) => new(FunctionWrappedCall("min_by", false, col,ord));

    /// <Summary>Count</Summary>
    public static SparkColumn Count(string col) => new(FunctionWrappedCall("count", false, col));

    /// <Summary>Count</Summary>
	public static SparkColumn Count(SparkColumn col) => new(FunctionWrappedCall("count", false, col));

    /// <Summary>Count</Summary>
	public static SparkColumn Count(Expression col) => new(FunctionWrappedCall("count", false, col));

    /// <Summary>Sum</Summary>
    public static SparkColumn Sum(string col) => new(FunctionWrappedCall("sum", false, col));

    /// <Summary>Sum</Summary>
	public static SparkColumn Sum(SparkColumn col) => new(FunctionWrappedCall("sum", false, col));

    /// <Summary>Sum</Summary>
	public static SparkColumn Sum(Expression col) => new(FunctionWrappedCall("sum", false, col));

    /// <Summary>Avg</Summary>
    public static SparkColumn Avg(string col) => new(FunctionWrappedCall("avg", false, col));

    /// <Summary>Avg</Summary>
	public static SparkColumn Avg(SparkColumn col) => new(FunctionWrappedCall("avg", false, col));

    /// <Summary>Avg</Summary>
	public static SparkColumn Avg(Expression col) => new(FunctionWrappedCall("avg", false, col));

    /// <Summary>Mean</Summary>
    public static SparkColumn Mean(string col) => new(FunctionWrappedCall("mean", false, col));

    /// <Summary>Mean</Summary>
	public static SparkColumn Mean(SparkColumn col) => new(FunctionWrappedCall("mean", false, col));

    /// <Summary>Mean</Summary>
	public static SparkColumn Mean(Expression col) => new(FunctionWrappedCall("mean", false, col));

    /// <Summary>Median</Summary>
    public static SparkColumn Median(string col) => new(FunctionWrappedCall("median", false, col));

    /// <Summary>Median</Summary>
	public static SparkColumn Median(SparkColumn col) => new(FunctionWrappedCall("median", false, col));

    /// <Summary>Median</Summary>
	public static SparkColumn Median(Expression col) => new(FunctionWrappedCall("median", false, col));

    /// <Summary>Product</Summary>
    public static SparkColumn Product(string col) => new(FunctionWrappedCall("product", false, col));

    /// <Summary>Product</Summary>
	public static SparkColumn Product(SparkColumn col) => new(FunctionWrappedCall("product", false, col));

    /// <Summary>Product</Summary>
	public static SparkColumn Product(Expression col) => new(FunctionWrappedCall("product", false, col));

    /// <Summary>Acos</Summary>
    public static SparkColumn Acos(string col) => new(FunctionWrappedCall("acos", false, col));

    /// <Summary>Acos</Summary>
	public static SparkColumn Acos(SparkColumn col) => new(FunctionWrappedCall("acos", false, col));

    /// <Summary>Acos</Summary>
	public static SparkColumn Acos(Expression col) => new(FunctionWrappedCall("acos", false, col));

    /// <Summary>Acosh</Summary>
    public static SparkColumn Acosh(string col) => new(FunctionWrappedCall("acosh", false, col));

    /// <Summary>Acosh</Summary>
	public static SparkColumn Acosh(SparkColumn col) => new(FunctionWrappedCall("acosh", false, col));

    /// <Summary>Acosh</Summary>
	public static SparkColumn Acosh(Expression col) => new(FunctionWrappedCall("acosh", false, col));

    /// <Summary>Asin</Summary>
    public static SparkColumn Asin(string col) => new(FunctionWrappedCall("asin", false, col));

    /// <Summary>Asin</Summary>
	public static SparkColumn Asin(SparkColumn col) => new(FunctionWrappedCall("asin", false, col));

    /// <Summary>Asin</Summary>
	public static SparkColumn Asin(Expression col) => new(FunctionWrappedCall("asin", false, col));

    /// <Summary>Asinh</Summary>
    public static SparkColumn Asinh(string col) => new(FunctionWrappedCall("asinh", false, col));

    /// <Summary>Asinh</Summary>
	public static SparkColumn Asinh(SparkColumn col) => new(FunctionWrappedCall("asinh", false, col));

    /// <Summary>Asinh</Summary>
	public static SparkColumn Asinh(Expression col) => new(FunctionWrappedCall("asinh", false, col));

    /// <Summary>Atan</Summary>
    public static SparkColumn Atan(string col) => new(FunctionWrappedCall("atan", false, col));

    /// <Summary>Atan</Summary>
	public static SparkColumn Atan(SparkColumn col) => new(FunctionWrappedCall("atan", false, col));

    /// <Summary>Atan</Summary>
	public static SparkColumn Atan(Expression col) => new(FunctionWrappedCall("atan", false, col));

    /// <Summary>Atanh</Summary>
    public static SparkColumn Atanh(string col) => new(FunctionWrappedCall("atanh", false, col));

    /// <Summary>Atanh</Summary>
	public static SparkColumn Atanh(SparkColumn col) => new(FunctionWrappedCall("atanh", false, col));

    /// <Summary>Atanh</Summary>
	public static SparkColumn Atanh(Expression col) => new(FunctionWrappedCall("atanh", false, col));

    /// <Summary>Cbrt</Summary>
    public static SparkColumn Cbrt(string col) => new(FunctionWrappedCall("cbrt", false, col));

    /// <Summary>Cbrt</Summary>
	public static SparkColumn Cbrt(SparkColumn col) => new(FunctionWrappedCall("cbrt", false, col));

    /// <Summary>Cbrt</Summary>
	public static SparkColumn Cbrt(Expression col) => new(FunctionWrappedCall("cbrt", false, col));

    /// <Summary>Ceil</Summary>
    public static SparkColumn Ceil(string col) => new(FunctionWrappedCall("ceil", false, col));

    /// <Summary>Ceil</Summary>
	public static SparkColumn Ceil(SparkColumn col) => new(FunctionWrappedCall("ceil", false, col));

    /// <Summary>Ceil</Summary>
	public static SparkColumn Ceil(Expression col) => new(FunctionWrappedCall("ceil", false, col));

    /// <Summary>Ceiling</Summary>
    public static SparkColumn Ceiling(string col) => new(FunctionWrappedCall("ceiling", false, col));

    /// <Summary>Ceiling</Summary>
	public static SparkColumn Ceiling(SparkColumn col) => new(FunctionWrappedCall("ceiling", false, col));

    /// <Summary>Ceiling</Summary>
	public static SparkColumn Ceiling(Expression col) => new(FunctionWrappedCall("ceiling", false, col));

    /// <Summary>Cos</Summary>
    public static SparkColumn Cos(string col) => new(FunctionWrappedCall("cos", false, col));

    /// <Summary>Cos</Summary>
	public static SparkColumn Cos(SparkColumn col) => new(FunctionWrappedCall("cos", false, col));

    /// <Summary>Cos</Summary>
	public static SparkColumn Cos(Expression col) => new(FunctionWrappedCall("cos", false, col));

    /// <Summary>Cosh</Summary>
    public static SparkColumn Cosh(string col) => new(FunctionWrappedCall("cosh", false, col));

    /// <Summary>Cosh</Summary>
	public static SparkColumn Cosh(SparkColumn col) => new(FunctionWrappedCall("cosh", false, col));

    /// <Summary>Cosh</Summary>
	public static SparkColumn Cosh(Expression col) => new(FunctionWrappedCall("cosh", false, col));

    /// <Summary>Cot</Summary>
    public static SparkColumn Cot(string col) => new(FunctionWrappedCall("cot", false, col));

    /// <Summary>Cot</Summary>
	public static SparkColumn Cot(SparkColumn col) => new(FunctionWrappedCall("cot", false, col));

    /// <Summary>Cot</Summary>
	public static SparkColumn Cot(Expression col) => new(FunctionWrappedCall("cot", false, col));

    /// <Summary>Csc</Summary>
    public static SparkColumn Csc(string col) => new(FunctionWrappedCall("csc", false, col));

    /// <Summary>Csc</Summary>
	public static SparkColumn Csc(SparkColumn col) => new(FunctionWrappedCall("csc", false, col));

    /// <Summary>Csc</Summary>
	public static SparkColumn Csc(Expression col) => new(FunctionWrappedCall("csc", false, col));
	/// <Summary>E</Summary>
	public static SparkColumn E() => new(FunctionWrappedCall("e", false));
    


    /// <Summary>Exp</Summary>
    public static SparkColumn Exp(string col) => new(FunctionWrappedCall("exp", false, col));

    /// <Summary>Exp</Summary>
	public static SparkColumn Exp(SparkColumn col) => new(FunctionWrappedCall("exp", false, col));

    /// <Summary>Exp</Summary>
	public static SparkColumn Exp(Expression col) => new(FunctionWrappedCall("exp", false, col));

    /// <Summary>Expm1</Summary>
    public static SparkColumn Expm1(string col) => new(FunctionWrappedCall("expm1", false, col));

    /// <Summary>Expm1</Summary>
	public static SparkColumn Expm1(SparkColumn col) => new(FunctionWrappedCall("expm1", false, col));

    /// <Summary>Expm1</Summary>
	public static SparkColumn Expm1(Expression col) => new(FunctionWrappedCall("expm1", false, col));

    /// <Summary>Floor</Summary>
    public static SparkColumn Floor(string col) => new(FunctionWrappedCall("floor", false, col));

    /// <Summary>Floor</Summary>
	public static SparkColumn Floor(SparkColumn col) => new(FunctionWrappedCall("floor", false, col));

    /// <Summary>Floor</Summary>
	public static SparkColumn Floor(Expression col) => new(FunctionWrappedCall("floor", false, col));

    /// <Summary>Log</Summary>
    public static SparkColumn Log(string col) => new(FunctionWrappedCall("log", false, col));

    /// <Summary>Log</Summary>
	public static SparkColumn Log(SparkColumn col) => new(FunctionWrappedCall("log", false, col));

    /// <Summary>Log</Summary>
	public static SparkColumn Log(Expression col) => new(FunctionWrappedCall("log", false, col));

    /// <Summary>Log10</Summary>
    public static SparkColumn Log10(string col) => new(FunctionWrappedCall("log10", false, col));

    /// <Summary>Log10</Summary>
	public static SparkColumn Log10(SparkColumn col) => new(FunctionWrappedCall("log10", false, col));

    /// <Summary>Log10</Summary>
	public static SparkColumn Log10(Expression col) => new(FunctionWrappedCall("log10", false, col));

    /// <Summary>Log1p</Summary>
    public static SparkColumn Log1p(string col) => new(FunctionWrappedCall("log1p", false, col));

    /// <Summary>Log1p</Summary>
	public static SparkColumn Log1p(SparkColumn col) => new(FunctionWrappedCall("log1p", false, col));

    /// <Summary>Log1p</Summary>
	public static SparkColumn Log1p(Expression col) => new(FunctionWrappedCall("log1p", false, col));

    /// <Summary>Negative</Summary>
    public static SparkColumn Negative(string col) => new(FunctionWrappedCall("negative", false, col));

    /// <Summary>Negative</Summary>
	public static SparkColumn Negative(SparkColumn col) => new(FunctionWrappedCall("negative", false, col));

    /// <Summary>Negative</Summary>
	public static SparkColumn Negative(Expression col) => new(FunctionWrappedCall("negative", false, col));
	/// <Summary>Pi</Summary>
	public static SparkColumn Pi() => new(FunctionWrappedCall("pi", false));
    


    /// <Summary>Positive</Summary>
    public static SparkColumn Positive(string col) => new(FunctionWrappedCall("positive", false, col));

    /// <Summary>Positive</Summary>
	public static SparkColumn Positive(SparkColumn col) => new(FunctionWrappedCall("positive", false, col));

    /// <Summary>Positive</Summary>
	public static SparkColumn Positive(Expression col) => new(FunctionWrappedCall("positive", false, col));

    /// <Summary>Rint</Summary>
    public static SparkColumn Rint(string col) => new(FunctionWrappedCall("rint", false, col));

    /// <Summary>Rint</Summary>
	public static SparkColumn Rint(SparkColumn col) => new(FunctionWrappedCall("rint", false, col));

    /// <Summary>Rint</Summary>
	public static SparkColumn Rint(Expression col) => new(FunctionWrappedCall("rint", false, col));

    /// <Summary>Sec</Summary>
    public static SparkColumn Sec(string col) => new(FunctionWrappedCall("sec", false, col));

    /// <Summary>Sec</Summary>
	public static SparkColumn Sec(SparkColumn col) => new(FunctionWrappedCall("sec", false, col));

    /// <Summary>Sec</Summary>
	public static SparkColumn Sec(Expression col) => new(FunctionWrappedCall("sec", false, col));

    /// <Summary>Signum</Summary>
    public static SparkColumn Signum(string col) => new(FunctionWrappedCall("signum", false, col));

    /// <Summary>Signum</Summary>
	public static SparkColumn Signum(SparkColumn col) => new(FunctionWrappedCall("signum", false, col));

    /// <Summary>Signum</Summary>
	public static SparkColumn Signum(Expression col) => new(FunctionWrappedCall("signum", false, col));

    /// <Summary>Sign</Summary>
    public static SparkColumn Sign(string col) => new(FunctionWrappedCall("sign", false, col));

    /// <Summary>Sign</Summary>
	public static SparkColumn Sign(SparkColumn col) => new(FunctionWrappedCall("sign", false, col));

    /// <Summary>Sign</Summary>
	public static SparkColumn Sign(Expression col) => new(FunctionWrappedCall("sign", false, col));

    /// <Summary>Sin</Summary>
    public static SparkColumn Sin(string col) => new(FunctionWrappedCall("sin", false, col));

    /// <Summary>Sin</Summary>
	public static SparkColumn Sin(SparkColumn col) => new(FunctionWrappedCall("sin", false, col));

    /// <Summary>Sin</Summary>
	public static SparkColumn Sin(Expression col) => new(FunctionWrappedCall("sin", false, col));

    /// <Summary>Sinh</Summary>
    public static SparkColumn Sinh(string col) => new(FunctionWrappedCall("sinh", false, col));

    /// <Summary>Sinh</Summary>
	public static SparkColumn Sinh(SparkColumn col) => new(FunctionWrappedCall("sinh", false, col));

    /// <Summary>Sinh</Summary>
	public static SparkColumn Sinh(Expression col) => new(FunctionWrappedCall("sinh", false, col));

    /// <Summary>Tan</Summary>
    public static SparkColumn Tan(string col) => new(FunctionWrappedCall("tan", false, col));

    /// <Summary>Tan</Summary>
	public static SparkColumn Tan(SparkColumn col) => new(FunctionWrappedCall("tan", false, col));

    /// <Summary>Tan</Summary>
	public static SparkColumn Tan(Expression col) => new(FunctionWrappedCall("tan", false, col));

    /// <Summary>Tanh</Summary>
    public static SparkColumn Tanh(string col) => new(FunctionWrappedCall("tanh", false, col));

    /// <Summary>Tanh</Summary>
	public static SparkColumn Tanh(SparkColumn col) => new(FunctionWrappedCall("tanh", false, col));

    /// <Summary>Tanh</Summary>
	public static SparkColumn Tanh(Expression col) => new(FunctionWrappedCall("tanh", false, col));

    /// <Summary>BitwiseNot</Summary>
    public static SparkColumn BitwiseNot(string col) => new(FunctionWrappedCall("~", false, col));

    /// <Summary>BitwiseNot</Summary>
	public static SparkColumn BitwiseNot(SparkColumn col) => new(FunctionWrappedCall("~", false, col));

    /// <Summary>BitwiseNot</Summary>
	public static SparkColumn BitwiseNot(Expression col) => new(FunctionWrappedCall("~", false, col));

    /// <Summary>BitCount</Summary>
    public static SparkColumn BitCount(string col) => new(FunctionWrappedCall("bit_count", false, col));

    /// <Summary>BitCount</Summary>
	public static SparkColumn BitCount(SparkColumn col) => new(FunctionWrappedCall("bit_count", false, col));

    /// <Summary>BitCount</Summary>
	public static SparkColumn BitCount(Expression col) => new(FunctionWrappedCall("bit_count", false, col));

    /// <Summary>
    /// BitGet
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    public static SparkColumn BitGet(string col ,Expression pos) => new(FunctionWrappedCall("bit_get", false, col,pos));

    /// <Summary>
    /// BitGet
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    
	public static SparkColumn BitGet(SparkColumn col ,Expression pos) => new(FunctionWrappedCall("bit_get", false, col,pos));

    /// <Summary>
    /// BitGet
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    
	public static SparkColumn BitGet(Expression col,Expression pos) => new(FunctionWrappedCall("bit_get", false, col,pos));

    /// <Summary>
    /// Getbit
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    public static SparkColumn Getbit(string col ,Expression pos) => new(FunctionWrappedCall("getbit", false, col,pos));

    /// <Summary>
    /// Getbit
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    
	public static SparkColumn Getbit(SparkColumn col ,Expression pos) => new(FunctionWrappedCall("getbit", false, col,pos));

    /// <Summary>
    /// Getbit
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    
	public static SparkColumn Getbit(Expression col,Expression pos) => new(FunctionWrappedCall("getbit", false, col,pos));

    /// <Summary>AscNullsFirst</Summary>
    public static SparkColumn AscNullsFirst(string col) => new(FunctionWrappedCall("asc_nulls_first", false, col));

    /// <Summary>AscNullsFirst</Summary>
	public static SparkColumn AscNullsFirst(SparkColumn col) => new(FunctionWrappedCall("asc_nulls_first", false, col));

    /// <Summary>AscNullsFirst</Summary>
	public static SparkColumn AscNullsFirst(Expression col) => new(FunctionWrappedCall("asc_nulls_first", false, col));

    /// <Summary>AscNullsLast</Summary>
    public static SparkColumn AscNullsLast(string col) => new(FunctionWrappedCall("asc_nulls_last", false, col));

    /// <Summary>AscNullsLast</Summary>
	public static SparkColumn AscNullsLast(SparkColumn col) => new(FunctionWrappedCall("asc_nulls_last", false, col));

    /// <Summary>AscNullsLast</Summary>
	public static SparkColumn AscNullsLast(Expression col) => new(FunctionWrappedCall("asc_nulls_last", false, col));

    /// <Summary>DescNullsFirst</Summary>
    public static SparkColumn DescNullsFirst(string col) => new(FunctionWrappedCall("desc_nulls_first", false, col));

    /// <Summary>DescNullsFirst</Summary>
	public static SparkColumn DescNullsFirst(SparkColumn col) => new(FunctionWrappedCall("desc_nulls_first", false, col));

    /// <Summary>DescNullsFirst</Summary>
	public static SparkColumn DescNullsFirst(Expression col) => new(FunctionWrappedCall("desc_nulls_first", false, col));

    /// <Summary>DescNullsLast</Summary>
    public static SparkColumn DescNullsLast(string col) => new(FunctionWrappedCall("desc_nulls_last", false, col));

    /// <Summary>DescNullsLast</Summary>
	public static SparkColumn DescNullsLast(SparkColumn col) => new(FunctionWrappedCall("desc_nulls_last", false, col));

    /// <Summary>DescNullsLast</Summary>
	public static SparkColumn DescNullsLast(Expression col) => new(FunctionWrappedCall("desc_nulls_last", false, col));

    /// <Summary>Stddev</Summary>
    public static SparkColumn Stddev(string col) => new(FunctionWrappedCall("stddev", false, col));

    /// <Summary>Stddev</Summary>
	public static SparkColumn Stddev(SparkColumn col) => new(FunctionWrappedCall("stddev", false, col));

    /// <Summary>Stddev</Summary>
	public static SparkColumn Stddev(Expression col) => new(FunctionWrappedCall("stddev", false, col));

    /// <Summary>Std</Summary>
    public static SparkColumn Std(string col) => new(FunctionWrappedCall("std", false, col));

    /// <Summary>Std</Summary>
	public static SparkColumn Std(SparkColumn col) => new(FunctionWrappedCall("std", false, col));

    /// <Summary>Std</Summary>
	public static SparkColumn Std(Expression col) => new(FunctionWrappedCall("std", false, col));

    /// <Summary>StddevSamp</Summary>
    public static SparkColumn StddevSamp(string col) => new(FunctionWrappedCall("stddev_samp", false, col));

    /// <Summary>StddevSamp</Summary>
	public static SparkColumn StddevSamp(SparkColumn col) => new(FunctionWrappedCall("stddev_samp", false, col));

    /// <Summary>StddevSamp</Summary>
	public static SparkColumn StddevSamp(Expression col) => new(FunctionWrappedCall("stddev_samp", false, col));

    /// <Summary>StddevPop</Summary>
    public static SparkColumn StddevPop(string col) => new(FunctionWrappedCall("stddev_pop", false, col));

    /// <Summary>StddevPop</Summary>
	public static SparkColumn StddevPop(SparkColumn col) => new(FunctionWrappedCall("stddev_pop", false, col));

    /// <Summary>StddevPop</Summary>
	public static SparkColumn StddevPop(Expression col) => new(FunctionWrappedCall("stddev_pop", false, col));

    /// <Summary>Variance</Summary>
    public static SparkColumn Variance(string col) => new(FunctionWrappedCall("variance", false, col));

    /// <Summary>Variance</Summary>
	public static SparkColumn Variance(SparkColumn col) => new(FunctionWrappedCall("variance", false, col));

    /// <Summary>Variance</Summary>
	public static SparkColumn Variance(Expression col) => new(FunctionWrappedCall("variance", false, col));

    /// <Summary>VarSamp</Summary>
    public static SparkColumn VarSamp(string col) => new(FunctionWrappedCall("var_samp", false, col));

    /// <Summary>VarSamp</Summary>
	public static SparkColumn VarSamp(SparkColumn col) => new(FunctionWrappedCall("var_samp", false, col));

    /// <Summary>VarSamp</Summary>
	public static SparkColumn VarSamp(Expression col) => new(FunctionWrappedCall("var_samp", false, col));

    /// <Summary>VarPop</Summary>
    public static SparkColumn VarPop(string col) => new(FunctionWrappedCall("var_pop", false, col));

    /// <Summary>VarPop</Summary>
	public static SparkColumn VarPop(SparkColumn col) => new(FunctionWrappedCall("var_pop", false, col));

    /// <Summary>VarPop</Summary>
	public static SparkColumn VarPop(Expression col) => new(FunctionWrappedCall("var_pop", false, col));

    /// <Summary>
    /// RegrAvgx
    /// Aggregate function: returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrAvgx(string y,string x) => new(FunctionWrappedCall("regr_avgx", false, y,x));

    /// <Summary>
    /// RegrAvgx
    /// Aggregate function: returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrAvgx(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_avgx", false, y,x));

    /// <Summary>
    /// RegrAvgx
    /// Aggregate function: returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrAvgx(Expression y,Expression x) => new(FunctionWrappedCall("regr_avgx", false, y,x));

    /// <Summary>
    /// RegrAvgy
    /// Aggregate function: returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrAvgy(string y,string x) => new(FunctionWrappedCall("regr_avgy", false, y,x));

    /// <Summary>
    /// RegrAvgy
    /// Aggregate function: returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrAvgy(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_avgy", false, y,x));

    /// <Summary>
    /// RegrAvgy
    /// Aggregate function: returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrAvgy(Expression y,Expression x) => new(FunctionWrappedCall("regr_avgy", false, y,x));

    /// <Summary>
    /// RegrCount
    /// Aggregate function: returns the number of non-null number pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrCount(string y,string x) => new(FunctionWrappedCall("regr_count", false, y,x));

    /// <Summary>
    /// RegrCount
    /// Aggregate function: returns the number of non-null number pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrCount(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_count", false, y,x));

    /// <Summary>
    /// RegrCount
    /// Aggregate function: returns the number of non-null number pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrCount(Expression y,Expression x) => new(FunctionWrappedCall("regr_count", false, y,x));

    /// <Summary>
    /// RegrIntercept
    /// Aggregate function: returns the intercept of the univariate linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrIntercept(string y,string x) => new(FunctionWrappedCall("regr_intercept", false, y,x));

    /// <Summary>
    /// RegrIntercept
    /// Aggregate function: returns the intercept of the univariate linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrIntercept(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_intercept", false, y,x));

    /// <Summary>
    /// RegrIntercept
    /// Aggregate function: returns the intercept of the univariate linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrIntercept(Expression y,Expression x) => new(FunctionWrappedCall("regr_intercept", false, y,x));

    /// <Summary>
    /// RegrR2
    /// Aggregate function: returns the coefficient of determination for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrR2(string y,string x) => new(FunctionWrappedCall("regr_r2", false, y,x));

    /// <Summary>
    /// RegrR2
    /// Aggregate function: returns the coefficient of determination for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrR2(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_r2", false, y,x));

    /// <Summary>
    /// RegrR2
    /// Aggregate function: returns the coefficient of determination for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrR2(Expression y,Expression x) => new(FunctionWrappedCall("regr_r2", false, y,x));

    /// <Summary>
    /// RegrSlope
    /// Aggregate function: returns the slope of the linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrSlope(string y,string x) => new(FunctionWrappedCall("regr_slope", false, y,x));

    /// <Summary>
    /// RegrSlope
    /// Aggregate function: returns the slope of the linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrSlope(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_slope", false, y,x));

    /// <Summary>
    /// RegrSlope
    /// Aggregate function: returns the slope of the linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrSlope(Expression y,Expression x) => new(FunctionWrappedCall("regr_slope", false, y,x));

    /// <Summary>
    /// RegrSxx
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrSxx(string y,string x) => new(FunctionWrappedCall("regr_sxx", false, y,x));

    /// <Summary>
    /// RegrSxx
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrSxx(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_sxx", false, y,x));

    /// <Summary>
    /// RegrSxx
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrSxx(Expression y,Expression x) => new(FunctionWrappedCall("regr_sxx", false, y,x));

    /// <Summary>
    /// RegrSxy
    /// Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrSxy(string y,string x) => new(FunctionWrappedCall("regr_sxy", false, y,x));

    /// <Summary>
    /// RegrSxy
    /// Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrSxy(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_sxy", false, y,x));

    /// <Summary>
    /// RegrSxy
    /// Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrSxy(Expression y,Expression x) => new(FunctionWrappedCall("regr_sxy", false, y,x));

    /// <Summary>
    /// RegrSyy
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static SparkColumn RegrSyy(string y,string x) => new(FunctionWrappedCall("regr_syy", false, y,x));

    /// <Summary>
    /// RegrSyy
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrSyy(SparkColumn y,SparkColumn x) => new(FunctionWrappedCall("regr_syy", false, y,x));

    /// <Summary>
    /// RegrSyy
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    
	public static SparkColumn RegrSyy(Expression y,Expression x) => new(FunctionWrappedCall("regr_syy", false, y,x));

    /// <Summary>Every</Summary>
    public static SparkColumn Every(string col) => new(FunctionWrappedCall("every", false, col));

    /// <Summary>Every</Summary>
	public static SparkColumn Every(SparkColumn col) => new(FunctionWrappedCall("every", false, col));

    /// <Summary>Every</Summary>
	public static SparkColumn Every(Expression col) => new(FunctionWrappedCall("every", false, col));

    /// <Summary>BoolAnd</Summary>
    public static SparkColumn BoolAnd(string col) => new(FunctionWrappedCall("bool_and", false, col));

    /// <Summary>BoolAnd</Summary>
	public static SparkColumn BoolAnd(SparkColumn col) => new(FunctionWrappedCall("bool_and", false, col));

    /// <Summary>BoolAnd</Summary>
	public static SparkColumn BoolAnd(Expression col) => new(FunctionWrappedCall("bool_and", false, col));

    /// <Summary>Some</Summary>
    public static SparkColumn Some(string col) => new(FunctionWrappedCall("some", false, col));

    /// <Summary>Some</Summary>
	public static SparkColumn Some(SparkColumn col) => new(FunctionWrappedCall("some", false, col));

    /// <Summary>Some</Summary>
	public static SparkColumn Some(Expression col) => new(FunctionWrappedCall("some", false, col));

    /// <Summary>BoolOr</Summary>
    public static SparkColumn BoolOr(string col) => new(FunctionWrappedCall("bool_or", false, col));

    /// <Summary>BoolOr</Summary>
	public static SparkColumn BoolOr(SparkColumn col) => new(FunctionWrappedCall("bool_or", false, col));

    /// <Summary>BoolOr</Summary>
	public static SparkColumn BoolOr(Expression col) => new(FunctionWrappedCall("bool_or", false, col));

    /// <Summary>BitAnd</Summary>
    public static SparkColumn BitAnd(string col) => new(FunctionWrappedCall("bit_and", false, col));

    /// <Summary>BitAnd</Summary>
	public static SparkColumn BitAnd(SparkColumn col) => new(FunctionWrappedCall("bit_and", false, col));

    /// <Summary>BitAnd</Summary>
	public static SparkColumn BitAnd(Expression col) => new(FunctionWrappedCall("bit_and", false, col));

    /// <Summary>BitOr</Summary>
    public static SparkColumn BitOr(string col) => new(FunctionWrappedCall("bit_or", false, col));

    /// <Summary>BitOr</Summary>
	public static SparkColumn BitOr(SparkColumn col) => new(FunctionWrappedCall("bit_or", false, col));

    /// <Summary>BitOr</Summary>
	public static SparkColumn BitOr(Expression col) => new(FunctionWrappedCall("bit_or", false, col));

    /// <Summary>BitXor</Summary>
    public static SparkColumn BitXor(string col) => new(FunctionWrappedCall("bit_xor", false, col));

    /// <Summary>BitXor</Summary>
	public static SparkColumn BitXor(SparkColumn col) => new(FunctionWrappedCall("bit_xor", false, col));

    /// <Summary>BitXor</Summary>
	public static SparkColumn BitXor(Expression col) => new(FunctionWrappedCall("bit_xor", false, col));

    /// <Summary>Skewness</Summary>
    public static SparkColumn Skewness(string col) => new(FunctionWrappedCall("skewness", false, col));

    /// <Summary>Skewness</Summary>
	public static SparkColumn Skewness(SparkColumn col) => new(FunctionWrappedCall("skewness", false, col));

    /// <Summary>Skewness</Summary>
	public static SparkColumn Skewness(Expression col) => new(FunctionWrappedCall("skewness", false, col));

    /// <Summary>Kurtosis</Summary>
    public static SparkColumn Kurtosis(string col) => new(FunctionWrappedCall("kurtosis", false, col));

    /// <Summary>Kurtosis</Summary>
	public static SparkColumn Kurtosis(SparkColumn col) => new(FunctionWrappedCall("kurtosis", false, col));

    /// <Summary>Kurtosis</Summary>
	public static SparkColumn Kurtosis(Expression col) => new(FunctionWrappedCall("kurtosis", false, col));

    /// <Summary>CollectList</Summary>
    public static SparkColumn CollectList(string col) => new(FunctionWrappedCall("collect_list", false, col));

    /// <Summary>CollectList</Summary>
	public static SparkColumn CollectList(SparkColumn col) => new(FunctionWrappedCall("collect_list", false, col));

    /// <Summary>CollectList</Summary>
	public static SparkColumn CollectList(Expression col) => new(FunctionWrappedCall("collect_list", false, col));

    /// <Summary>ArrayAgg</Summary>
    public static SparkColumn ArrayAgg(string col) => new(FunctionWrappedCall("array_agg", false, col));

    /// <Summary>ArrayAgg</Summary>
	public static SparkColumn ArrayAgg(SparkColumn col) => new(FunctionWrappedCall("array_agg", false, col));

    /// <Summary>ArrayAgg</Summary>
	public static SparkColumn ArrayAgg(Expression col) => new(FunctionWrappedCall("array_agg", false, col));

    /// <Summary>CollectSet</Summary>
    public static SparkColumn CollectSet(string col) => new(FunctionWrappedCall("collect_set", false, col));

    /// <Summary>CollectSet</Summary>
	public static SparkColumn CollectSet(SparkColumn col) => new(FunctionWrappedCall("collect_set", false, col));

    /// <Summary>CollectSet</Summary>
	public static SparkColumn CollectSet(Expression col) => new(FunctionWrappedCall("collect_set", false, col));

    /// <Summary>Degrees</Summary>
    public static SparkColumn Degrees(string col) => new(FunctionWrappedCall("degrees", false, col));

    /// <Summary>Degrees</Summary>
	public static SparkColumn Degrees(SparkColumn col) => new(FunctionWrappedCall("degrees", false, col));

    /// <Summary>Degrees</Summary>
	public static SparkColumn Degrees(Expression col) => new(FunctionWrappedCall("degrees", false, col));

    /// <Summary>Radians</Summary>
    public static SparkColumn Radians(string col) => new(FunctionWrappedCall("radians", false, col));

    /// <Summary>Radians</Summary>
	public static SparkColumn Radians(SparkColumn col) => new(FunctionWrappedCall("radians", false, col));

    /// <Summary>Radians</Summary>
	public static SparkColumn Radians(Expression col) => new(FunctionWrappedCall("radians", false, col));
	/// <Summary>RowNumber</Summary>
	public static SparkColumn RowNumber() => new(FunctionWrappedCall("row_number", false));
    

	/// <Summary>DenseRank</Summary>
	public static SparkColumn DenseRank() => new(FunctionWrappedCall("dense_rank", false));
    

	/// <Summary>Rank</Summary>
	public static SparkColumn Rank() => new(FunctionWrappedCall("rank", false));
    

	/// <Summary>CumeDist</Summary>
	public static SparkColumn CumeDist() => new(FunctionWrappedCall("cume_dist", false));
    

	/// <Summary>PercentRank</Summary>
	public static SparkColumn PercentRank() => new(FunctionWrappedCall("percent_rank", false));
    


    /// <Summary>
    /// Corr
    /// Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for ``col1`` and ``col2``.
    /// </Summary>
    public static SparkColumn Corr(string col1,string col2) => new(FunctionWrappedCall("corr", false, col1,col2));

    /// <Summary>
    /// Corr
    /// Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for ``col1`` and ``col2``.
    /// </Summary>
    
	public static SparkColumn Corr(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("corr", false, col1,col2));

    /// <Summary>
    /// Corr
    /// Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for ``col1`` and ``col2``.
    /// </Summary>
    
	public static SparkColumn Corr(Expression col1,Expression col2) => new(FunctionWrappedCall("corr", false, col1,col2));

    /// <Summary>
    /// CovarPop
    /// Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and ``col2``.
    /// </Summary>
    public static SparkColumn CovarPop(string col1,string col2) => new(FunctionWrappedCall("covar_pop", false, col1,col2));

    /// <Summary>
    /// CovarPop
    /// Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and ``col2``.
    /// </Summary>
    
	public static SparkColumn CovarPop(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("covar_pop", false, col1,col2));

    /// <Summary>
    /// CovarPop
    /// Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and ``col2``.
    /// </Summary>
    
	public static SparkColumn CovarPop(Expression col1,Expression col2) => new(FunctionWrappedCall("covar_pop", false, col1,col2));

    /// <Summary>
    /// CovarSamp
    /// Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and ``col2``.
    /// </Summary>
    public static SparkColumn CovarSamp(string col1,string col2) => new(FunctionWrappedCall("covar_samp", false, col1,col2));

    /// <Summary>
    /// CovarSamp
    /// Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and ``col2``.
    /// </Summary>
    
	public static SparkColumn CovarSamp(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("covar_samp", false, col1,col2));

    /// <Summary>
    /// CovarSamp
    /// Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and ``col2``.
    /// </Summary>
    
	public static SparkColumn CovarSamp(Expression col1,Expression col2) => new(FunctionWrappedCall("covar_samp", false, col1,col2));
	/// <Summary>InputFileName</Summary>
	public static SparkColumn InputFileName() => new(FunctionWrappedCall("input_file_name", false));
    


    /// <Summary>Isnan</Summary>
    public static SparkColumn Isnan(string col) => new(FunctionWrappedCall("isnan", false, col));

    /// <Summary>Isnan</Summary>
	public static SparkColumn Isnan(SparkColumn col) => new(FunctionWrappedCall("isnan", false, col));

    /// <Summary>Isnan</Summary>
	public static SparkColumn Isnan(Expression col) => new(FunctionWrappedCall("isnan", false, col));

    /// <Summary>Isnull</Summary>
    public static SparkColumn Isnull(string col) => new(FunctionWrappedCall("isnull", false, col));

    /// <Summary>Isnull</Summary>
	public static SparkColumn Isnull(SparkColumn col) => new(FunctionWrappedCall("isnull", false, col));

    /// <Summary>Isnull</Summary>
	public static SparkColumn Isnull(Expression col) => new(FunctionWrappedCall("isnull", false, col));
	/// <Summary>MonotonicallyIncreasingId</Summary>
	public static SparkColumn MonotonicallyIncreasingId() => new(FunctionWrappedCall("monotonically_increasing_id", false));
    


    /// <Summary>
    /// Nanvl
    /// Returns col1 if it is not NaN, or col2 if col1 is NaN.
    /// </Summary>
    public static SparkColumn Nanvl(string col1,string col2) => new(FunctionWrappedCall("nanvl", false, col1,col2));

    /// <Summary>
    /// Nanvl
    /// Returns col1 if it is not NaN, or col2 if col1 is NaN.
    /// </Summary>
    
	public static SparkColumn Nanvl(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("nanvl", false, col1,col2));

    /// <Summary>
    /// Nanvl
    /// Returns col1 if it is not NaN, or col2 if col1 is NaN.
    /// </Summary>
    
	public static SparkColumn Nanvl(Expression col1,Expression col2) => new(FunctionWrappedCall("nanvl", false, col1,col2));

    /// <Summary>
    /// Round
    /// Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    public static SparkColumn Round(string col ,Expression scale) => new(FunctionWrappedCall("round", false, col,scale));

    /// <Summary>
    /// Round
    /// Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    
	public static SparkColumn Round(SparkColumn col ,Expression scale) => new(FunctionWrappedCall("round", false, col,scale));

    /// <Summary>
    /// Round
    /// Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    
	public static SparkColumn Round(Expression col,Expression scale) => new(FunctionWrappedCall("round", false, col,scale));

    /// <Summary>
    /// Bround
    /// Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    public static SparkColumn Bround(string col ,Expression scale) => new(FunctionWrappedCall("bround", false, col,scale));

    /// <Summary>
    /// Bround
    /// Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    
	public static SparkColumn Bround(SparkColumn col ,Expression scale) => new(FunctionWrappedCall("bround", false, col,scale));

    /// <Summary>
    /// Bround
    /// Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    
	public static SparkColumn Bround(Expression col,Expression scale) => new(FunctionWrappedCall("bround", false, col,scale));

    /// <Summary>
    /// ShiftLeft
    /// Shift the given value numBits left.
    /// </Summary>
    public static SparkColumn ShiftLeft(string col ,Expression numBits) => new(FunctionWrappedCall("shiftLeft", false, col,numBits));

    /// <Summary>
    /// ShiftLeft
    /// Shift the given value numBits left.
    /// </Summary>
    
	public static SparkColumn ShiftLeft(SparkColumn col ,Expression numBits) => new(FunctionWrappedCall("shiftLeft", false, col,numBits));

    /// <Summary>
    /// ShiftLeft
    /// Shift the given value numBits left.
    /// </Summary>
    
	public static SparkColumn ShiftLeft(Expression col,Expression numBits) => new(FunctionWrappedCall("shiftLeft", false, col,numBits));

    /// <Summary>
    /// Shiftleft
    /// Shift the given value numBits left.
    /// </Summary>
    public static SparkColumn Shiftleft(string col ,Expression numBits) => new(FunctionWrappedCall("shiftleft", false, col,numBits));

    /// <Summary>
    /// Shiftleft
    /// Shift the given value numBits left.
    /// </Summary>
    
	public static SparkColumn Shiftleft(SparkColumn col ,Expression numBits) => new(FunctionWrappedCall("shiftleft", false, col,numBits));

    /// <Summary>
    /// Shiftleft
    /// Shift the given value numBits left.
    /// </Summary>
    
	public static SparkColumn Shiftleft(Expression col,Expression numBits) => new(FunctionWrappedCall("shiftleft", false, col,numBits));

    /// <Summary>
    /// ShiftRight
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    public static SparkColumn ShiftRight(string col ,Expression numBits) => new(FunctionWrappedCall("shiftRight", false, col,numBits));

    /// <Summary>
    /// ShiftRight
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    
	public static SparkColumn ShiftRight(SparkColumn col ,Expression numBits) => new(FunctionWrappedCall("shiftRight", false, col,numBits));

    /// <Summary>
    /// ShiftRight
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    
	public static SparkColumn ShiftRight(Expression col,Expression numBits) => new(FunctionWrappedCall("shiftRight", false, col,numBits));

    /// <Summary>
    /// Shiftright
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    public static SparkColumn Shiftright(string col ,Expression numBits) => new(FunctionWrappedCall("shiftright", false, col,numBits));

    /// <Summary>
    /// Shiftright
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    
	public static SparkColumn Shiftright(SparkColumn col ,Expression numBits) => new(FunctionWrappedCall("shiftright", false, col,numBits));

    /// <Summary>
    /// Shiftright
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    
	public static SparkColumn Shiftright(Expression col,Expression numBits) => new(FunctionWrappedCall("shiftright", false, col,numBits));

    /// <Summary>
    /// ShiftRightUnsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    public static SparkColumn ShiftRightUnsigned(string col ,Expression numBits) => new(FunctionWrappedCall("shiftRightUnsigned", false, col,numBits));

    /// <Summary>
    /// ShiftRightUnsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    
	public static SparkColumn ShiftRightUnsigned(SparkColumn col ,Expression numBits) => new(FunctionWrappedCall("shiftRightUnsigned", false, col,numBits));

    /// <Summary>
    /// ShiftRightUnsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    
	public static SparkColumn ShiftRightUnsigned(Expression col,Expression numBits) => new(FunctionWrappedCall("shiftRightUnsigned", false, col,numBits));

    /// <Summary>
    /// Shiftrightunsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    public static SparkColumn Shiftrightunsigned(string col ,Expression numBits) => new(FunctionWrappedCall("shiftrightunsigned", false, col,numBits));

    /// <Summary>
    /// Shiftrightunsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    
	public static SparkColumn Shiftrightunsigned(SparkColumn col ,Expression numBits) => new(FunctionWrappedCall("shiftrightunsigned", false, col,numBits));

    /// <Summary>
    /// Shiftrightunsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    
	public static SparkColumn Shiftrightunsigned(Expression col,Expression numBits) => new(FunctionWrappedCall("shiftrightunsigned", false, col,numBits));
	/// <Summary>SparkPartitionId</Summary>
	public static SparkColumn SparkPartitionId() => new(FunctionWrappedCall("spark_partition_id", false));
    


    /// <Summary>Ln</Summary>
    public static SparkColumn Ln(string col) => new(FunctionWrappedCall("ln", false, col));

    /// <Summary>Ln</Summary>
	public static SparkColumn Ln(SparkColumn col) => new(FunctionWrappedCall("ln", false, col));

    /// <Summary>Ln</Summary>
	public static SparkColumn Ln(Expression col) => new(FunctionWrappedCall("ln", false, col));

    /// <Summary>Log2</Summary>
    public static SparkColumn Log2(string col) => new(FunctionWrappedCall("log2", false, col));

    /// <Summary>Log2</Summary>
	public static SparkColumn Log2(SparkColumn col) => new(FunctionWrappedCall("log2", false, col));

    /// <Summary>Log2</Summary>
	public static SparkColumn Log2(Expression col) => new(FunctionWrappedCall("log2", false, col));

    /// <Summary>Factorial</Summary>
    public static SparkColumn Factorial(string col) => new(FunctionWrappedCall("factorial", false, col));

    /// <Summary>Factorial</Summary>
	public static SparkColumn Factorial(SparkColumn col) => new(FunctionWrappedCall("factorial", false, col));

    /// <Summary>Factorial</Summary>
	public static SparkColumn Factorial(Expression col) => new(FunctionWrappedCall("factorial", false, col));

    /// <Summary>CountIf</Summary>
    public static SparkColumn CountIf(string col) => new(FunctionWrappedCall("count_if", false, col));

    /// <Summary>CountIf</Summary>
	public static SparkColumn CountIf(SparkColumn col) => new(FunctionWrappedCall("count_if", false, col));

    /// <Summary>CountIf</Summary>
	public static SparkColumn CountIf(Expression col) => new(FunctionWrappedCall("count_if", false, col));
	/// <Summary>Curdate</Summary>
	public static SparkColumn Curdate() => new(FunctionWrappedCall("curdate", false));
    

	/// <Summary>CurrentDate</Summary>
	public static SparkColumn CurrentDate() => new(FunctionWrappedCall("current_date", false));
    

	/// <Summary>CurrentTimezone</Summary>
	public static SparkColumn CurrentTimezone() => new(FunctionWrappedCall("current_timezone", false));
    

	/// <Summary>CurrentTimestamp</Summary>
	public static SparkColumn CurrentTimestamp() => new(FunctionWrappedCall("current_timestamp", false));
    

	/// <Summary>Now</Summary>
	public static SparkColumn Now() => new(FunctionWrappedCall("now", false));
    

	/// <Summary>Localtimestamp</Summary>
	public static SparkColumn Localtimestamp() => new(FunctionWrappedCall("localtimestamp", false));
    


    /// <Summary>Year</Summary>
    public static SparkColumn Year(string col) => new(FunctionWrappedCall("year", false, col));

    /// <Summary>Year</Summary>
	public static SparkColumn Year(SparkColumn col) => new(FunctionWrappedCall("year", false, col));

    /// <Summary>Year</Summary>
	public static SparkColumn Year(Expression col) => new(FunctionWrappedCall("year", false, col));

    /// <Summary>Quarter</Summary>
    public static SparkColumn Quarter(string col) => new(FunctionWrappedCall("quarter", false, col));

    /// <Summary>Quarter</Summary>
	public static SparkColumn Quarter(SparkColumn col) => new(FunctionWrappedCall("quarter", false, col));

    /// <Summary>Quarter</Summary>
	public static SparkColumn Quarter(Expression col) => new(FunctionWrappedCall("quarter", false, col));

    /// <Summary>Month</Summary>
    public static SparkColumn Month(string col) => new(FunctionWrappedCall("month", false, col));

    /// <Summary>Month</Summary>
	public static SparkColumn Month(SparkColumn col) => new(FunctionWrappedCall("month", false, col));

    /// <Summary>Month</Summary>
	public static SparkColumn Month(Expression col) => new(FunctionWrappedCall("month", false, col));

    /// <Summary>Dayofweek</Summary>
    public static SparkColumn Dayofweek(string col) => new(FunctionWrappedCall("dayofweek", false, col));

    /// <Summary>Dayofweek</Summary>
	public static SparkColumn Dayofweek(SparkColumn col) => new(FunctionWrappedCall("dayofweek", false, col));

    /// <Summary>Dayofweek</Summary>
	public static SparkColumn Dayofweek(Expression col) => new(FunctionWrappedCall("dayofweek", false, col));

    /// <Summary>Dayofmonth</Summary>
    public static SparkColumn Dayofmonth(string col) => new(FunctionWrappedCall("dayofmonth", false, col));

    /// <Summary>Dayofmonth</Summary>
	public static SparkColumn Dayofmonth(SparkColumn col) => new(FunctionWrappedCall("dayofmonth", false, col));

    /// <Summary>Dayofmonth</Summary>
	public static SparkColumn Dayofmonth(Expression col) => new(FunctionWrappedCall("dayofmonth", false, col));

    /// <Summary>Day</Summary>
    public static SparkColumn Day(string col) => new(FunctionWrappedCall("day", false, col));

    /// <Summary>Day</Summary>
	public static SparkColumn Day(SparkColumn col) => new(FunctionWrappedCall("day", false, col));

    /// <Summary>Day</Summary>
	public static SparkColumn Day(Expression col) => new(FunctionWrappedCall("day", false, col));

    /// <Summary>Dayofyear</Summary>
    public static SparkColumn Dayofyear(string col) => new(FunctionWrappedCall("dayofyear", false, col));

    /// <Summary>Dayofyear</Summary>
	public static SparkColumn Dayofyear(SparkColumn col) => new(FunctionWrappedCall("dayofyear", false, col));

    /// <Summary>Dayofyear</Summary>
	public static SparkColumn Dayofyear(Expression col) => new(FunctionWrappedCall("dayofyear", false, col));

    /// <Summary>Hour</Summary>
    public static SparkColumn Hour(string col) => new(FunctionWrappedCall("hour", false, col));

    /// <Summary>Hour</Summary>
	public static SparkColumn Hour(SparkColumn col) => new(FunctionWrappedCall("hour", false, col));

    /// <Summary>Hour</Summary>
	public static SparkColumn Hour(Expression col) => new(FunctionWrappedCall("hour", false, col));

    /// <Summary>Minute</Summary>
    public static SparkColumn Minute(string col) => new(FunctionWrappedCall("minute", false, col));

    /// <Summary>Minute</Summary>
	public static SparkColumn Minute(SparkColumn col) => new(FunctionWrappedCall("minute", false, col));

    /// <Summary>Minute</Summary>
	public static SparkColumn Minute(Expression col) => new(FunctionWrappedCall("minute", false, col));

    /// <Summary>Second</Summary>
    public static SparkColumn Second(string col) => new(FunctionWrappedCall("second", false, col));

    /// <Summary>Second</Summary>
	public static SparkColumn Second(SparkColumn col) => new(FunctionWrappedCall("second", false, col));

    /// <Summary>Second</Summary>
	public static SparkColumn Second(Expression col) => new(FunctionWrappedCall("second", false, col));

    /// <Summary>Weekofyear</Summary>
    public static SparkColumn Weekofyear(string col) => new(FunctionWrappedCall("weekofyear", false, col));

    /// <Summary>Weekofyear</Summary>
	public static SparkColumn Weekofyear(SparkColumn col) => new(FunctionWrappedCall("weekofyear", false, col));

    /// <Summary>Weekofyear</Summary>
	public static SparkColumn Weekofyear(Expression col) => new(FunctionWrappedCall("weekofyear", false, col));

    /// <Summary>Weekday</Summary>
    public static SparkColumn Weekday(string col) => new(FunctionWrappedCall("weekday", false, col));

    /// <Summary>Weekday</Summary>
	public static SparkColumn Weekday(SparkColumn col) => new(FunctionWrappedCall("weekday", false, col));

    /// <Summary>Weekday</Summary>
	public static SparkColumn Weekday(Expression col) => new(FunctionWrappedCall("weekday", false, col));

    /// <Summary>
    /// MakeDate
    /// Returns a column with a date built from the year, month and day columns.
    /// </Summary>
    public static SparkColumn MakeDate(string year,string month,string day) => new(FunctionWrappedCall("make_date", false, year,month,day));

    /// <Summary>
    /// MakeDate
    /// Returns a column with a date built from the year, month and day columns.
    /// </Summary>
    
	public static SparkColumn MakeDate(SparkColumn year,SparkColumn month,SparkColumn day) => new(FunctionWrappedCall("make_date", false, year,month,day));

    /// <Summary>
    /// MakeDate
    /// Returns a column with a date built from the year, month and day columns.
    /// </Summary>
    
	public static SparkColumn MakeDate(Expression year,Expression month,Expression day) => new(FunctionWrappedCall("make_date", false, year,month,day));

    /// <Summary>
    /// Datediff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>
    public static SparkColumn Datediff(string end,string start) => new(FunctionWrappedCall("datediff", false, end,start));

    /// <Summary>
    /// Datediff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>
    
	public static SparkColumn Datediff(SparkColumn end,SparkColumn start) => new(FunctionWrappedCall("datediff", false, end,start));

    /// <Summary>
    /// Datediff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>
    
	public static SparkColumn Datediff(Expression end,Expression start) => new(FunctionWrappedCall("datediff", false, end,start));

    /// <Summary>
    /// DateDiff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>
    public static SparkColumn DateDiff(string end,string start) => new(FunctionWrappedCall("date_diff", false, end,start));

    /// <Summary>
    /// DateDiff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>
    
	public static SparkColumn DateDiff(SparkColumn end,SparkColumn start) => new(FunctionWrappedCall("date_diff", false, end,start));

    /// <Summary>
    /// DateDiff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>
    
	public static SparkColumn DateDiff(Expression end,Expression start) => new(FunctionWrappedCall("date_diff", false, end,start));

    /// <Summary>DateFromUnixDate</Summary>
    public static SparkColumn DateFromUnixDate(string col) => new(FunctionWrappedCall("date_from_unix_date", false, col));

    /// <Summary>DateFromUnixDate</Summary>
	public static SparkColumn DateFromUnixDate(SparkColumn col) => new(FunctionWrappedCall("date_from_unix_date", false, col));

    /// <Summary>DateFromUnixDate</Summary>
	public static SparkColumn DateFromUnixDate(Expression col) => new(FunctionWrappedCall("date_from_unix_date", false, col));

    /// <Summary>UnixDate</Summary>
    public static SparkColumn UnixDate(string col) => new(FunctionWrappedCall("unix_date", false, col));

    /// <Summary>UnixDate</Summary>
	public static SparkColumn UnixDate(SparkColumn col) => new(FunctionWrappedCall("unix_date", false, col));

    /// <Summary>UnixDate</Summary>
	public static SparkColumn UnixDate(Expression col) => new(FunctionWrappedCall("unix_date", false, col));

    /// <Summary>UnixMicros</Summary>
    public static SparkColumn UnixMicros(string col) => new(FunctionWrappedCall("unix_micros", false, col));

    /// <Summary>UnixMicros</Summary>
	public static SparkColumn UnixMicros(SparkColumn col) => new(FunctionWrappedCall("unix_micros", false, col));

    /// <Summary>UnixMicros</Summary>
	public static SparkColumn UnixMicros(Expression col) => new(FunctionWrappedCall("unix_micros", false, col));

    /// <Summary>UnixMillis</Summary>
    public static SparkColumn UnixMillis(string col) => new(FunctionWrappedCall("unix_millis", false, col));

    /// <Summary>UnixMillis</Summary>
	public static SparkColumn UnixMillis(SparkColumn col) => new(FunctionWrappedCall("unix_millis", false, col));

    /// <Summary>UnixMillis</Summary>
	public static SparkColumn UnixMillis(Expression col) => new(FunctionWrappedCall("unix_millis", false, col));

    /// <Summary>UnixSeconds</Summary>
    public static SparkColumn UnixSeconds(string col) => new(FunctionWrappedCall("unix_seconds", false, col));

    /// <Summary>UnixSeconds</Summary>
	public static SparkColumn UnixSeconds(SparkColumn col) => new(FunctionWrappedCall("unix_seconds", false, col));

    /// <Summary>UnixSeconds</Summary>
	public static SparkColumn UnixSeconds(Expression col) => new(FunctionWrappedCall("unix_seconds", false, col));

    /// <Summary>ToTimestamp</Summary>
    public static SparkColumn ToTimestamp(string col) => new(FunctionWrappedCall("to_timestamp", false, col));

    /// <Summary>ToTimestamp</Summary>
	public static SparkColumn ToTimestamp(SparkColumn col) => new(FunctionWrappedCall("to_timestamp", false, col));

    /// <Summary>ToTimestamp</Summary>
	public static SparkColumn ToTimestamp(Expression col) => new(FunctionWrappedCall("to_timestamp", false, col));

    /// <Summary>LastDay</Summary>
    public static SparkColumn LastDay(string col) => new(FunctionWrappedCall("last_day", false, col));

    /// <Summary>LastDay</Summary>
	public static SparkColumn LastDay(SparkColumn col) => new(FunctionWrappedCall("last_day", false, col));

    /// <Summary>LastDay</Summary>
	public static SparkColumn LastDay(Expression col) => new(FunctionWrappedCall("last_day", false, col));
	/// <Summary>UnixTimestamp</Summary>
	public static SparkColumn UnixTimestamp() => new(FunctionWrappedCall("unix_timestamp", false));
    


    /// <Summary>TimestampSeconds</Summary>
    public static SparkColumn TimestampSeconds(string col) => new(FunctionWrappedCall("timestamp_seconds", false, col));

    /// <Summary>TimestampSeconds</Summary>
	public static SparkColumn TimestampSeconds(SparkColumn col) => new(FunctionWrappedCall("timestamp_seconds", false, col));

    /// <Summary>TimestampSeconds</Summary>
	public static SparkColumn TimestampSeconds(Expression col) => new(FunctionWrappedCall("timestamp_seconds", false, col));

    /// <Summary>TimestampMillis</Summary>
    public static SparkColumn TimestampMillis(string col) => new(FunctionWrappedCall("timestamp_millis", false, col));

    /// <Summary>TimestampMillis</Summary>
	public static SparkColumn TimestampMillis(SparkColumn col) => new(FunctionWrappedCall("timestamp_millis", false, col));

    /// <Summary>TimestampMillis</Summary>
	public static SparkColumn TimestampMillis(Expression col) => new(FunctionWrappedCall("timestamp_millis", false, col));

    /// <Summary>TimestampMicros</Summary>
    public static SparkColumn TimestampMicros(string col) => new(FunctionWrappedCall("timestamp_micros", false, col));

    /// <Summary>TimestampMicros</Summary>
	public static SparkColumn TimestampMicros(SparkColumn col) => new(FunctionWrappedCall("timestamp_micros", false, col));

    /// <Summary>TimestampMicros</Summary>
	public static SparkColumn TimestampMicros(Expression col) => new(FunctionWrappedCall("timestamp_micros", false, col));
	/// <Summary>CurrentCatalog</Summary>
	public static SparkColumn CurrentCatalog() => new(FunctionWrappedCall("current_catalog", false));
    

	/// <Summary>CurrentDatabase</Summary>
	public static SparkColumn CurrentDatabase() => new(FunctionWrappedCall("current_database", false));
    

	/// <Summary>CurrentSchema</Summary>
	public static SparkColumn CurrentSchema() => new(FunctionWrappedCall("current_schema", false));
    

	/// <Summary>CurrentUser</Summary>
	public static SparkColumn CurrentUser() => new(FunctionWrappedCall("current_user", false));
    

	/// <Summary>User</Summary>
	public static SparkColumn User() => new(FunctionWrappedCall("user", false));
    


    /// <Summary>Crc32</Summary>
    public static SparkColumn Crc32(string col) => new(FunctionWrappedCall("crc32", false, col));

    /// <Summary>Crc32</Summary>
	public static SparkColumn Crc32(SparkColumn col) => new(FunctionWrappedCall("crc32", false, col));

    /// <Summary>Crc32</Summary>
	public static SparkColumn Crc32(Expression col) => new(FunctionWrappedCall("crc32", false, col));

    /// <Summary>Md5</Summary>
    public static SparkColumn Md5(string col) => new(FunctionWrappedCall("md5", false, col));

    /// <Summary>Md5</Summary>
	public static SparkColumn Md5(SparkColumn col) => new(FunctionWrappedCall("md5", false, col));

    /// <Summary>Md5</Summary>
	public static SparkColumn Md5(Expression col) => new(FunctionWrappedCall("md5", false, col));

    /// <Summary>Sha1</Summary>
    public static SparkColumn Sha1(string col) => new(FunctionWrappedCall("sha1", false, col));

    /// <Summary>Sha1</Summary>
	public static SparkColumn Sha1(SparkColumn col) => new(FunctionWrappedCall("sha1", false, col));

    /// <Summary>Sha1</Summary>
	public static SparkColumn Sha1(Expression col) => new(FunctionWrappedCall("sha1", false, col));

    /// <Summary>Upper</Summary>
    public static SparkColumn Upper(string col) => new(FunctionWrappedCall("upper", false, col));

    /// <Summary>Upper</Summary>
	public static SparkColumn Upper(SparkColumn col) => new(FunctionWrappedCall("upper", false, col));

    /// <Summary>Upper</Summary>
	public static SparkColumn Upper(Expression col) => new(FunctionWrappedCall("upper", false, col));

    /// <Summary>Lower</Summary>
    public static SparkColumn Lower(string col) => new(FunctionWrappedCall("lower", false, col));

    /// <Summary>Lower</Summary>
	public static SparkColumn Lower(SparkColumn col) => new(FunctionWrappedCall("lower", false, col));

    /// <Summary>Lower</Summary>
	public static SparkColumn Lower(Expression col) => new(FunctionWrappedCall("lower", false, col));

    /// <Summary>Ascii</Summary>
    public static SparkColumn Ascii(string col) => new(FunctionWrappedCall("ascii", false, col));

    /// <Summary>Ascii</Summary>
	public static SparkColumn Ascii(SparkColumn col) => new(FunctionWrappedCall("ascii", false, col));

    /// <Summary>Ascii</Summary>
	public static SparkColumn Ascii(Expression col) => new(FunctionWrappedCall("ascii", false, col));

    /// <Summary>Base64</Summary>
    public static SparkColumn Base64(string col) => new(FunctionWrappedCall("base64", false, col));

    /// <Summary>Base64</Summary>
	public static SparkColumn Base64(SparkColumn col) => new(FunctionWrappedCall("base64", false, col));

    /// <Summary>Base64</Summary>
	public static SparkColumn Base64(Expression col) => new(FunctionWrappedCall("base64", false, col));

    /// <Summary>Unbase64</Summary>
    public static SparkColumn Unbase64(string col) => new(FunctionWrappedCall("unbase64", false, col));

    /// <Summary>Unbase64</Summary>
	public static SparkColumn Unbase64(SparkColumn col) => new(FunctionWrappedCall("unbase64", false, col));

    /// <Summary>Unbase64</Summary>
	public static SparkColumn Unbase64(Expression col) => new(FunctionWrappedCall("unbase64", false, col));

    /// <Summary>Ltrim</Summary>
    public static SparkColumn Ltrim(string col) => new(FunctionWrappedCall("ltrim", false, col));

    /// <Summary>Ltrim</Summary>
	public static SparkColumn Ltrim(SparkColumn col) => new(FunctionWrappedCall("ltrim", false, col));

    /// <Summary>Ltrim</Summary>
	public static SparkColumn Ltrim(Expression col) => new(FunctionWrappedCall("ltrim", false, col));

    /// <Summary>Rtrim</Summary>
    public static SparkColumn Rtrim(string col) => new(FunctionWrappedCall("rtrim", false, col));

    /// <Summary>Rtrim</Summary>
	public static SparkColumn Rtrim(SparkColumn col) => new(FunctionWrappedCall("rtrim", false, col));

    /// <Summary>Rtrim</Summary>
	public static SparkColumn Rtrim(Expression col) => new(FunctionWrappedCall("rtrim", false, col));

    /// <Summary>Trim</Summary>
    public static SparkColumn Trim(string col) => new(FunctionWrappedCall("trim", false, col));

    /// <Summary>Trim</Summary>
	public static SparkColumn Trim(SparkColumn col) => new(FunctionWrappedCall("trim", false, col));

    /// <Summary>Trim</Summary>
	public static SparkColumn Trim(Expression col) => new(FunctionWrappedCall("trim", false, col));

    /// <Summary>
    /// FormatNumber
    /// Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places with HALF_EVEN round mode, and returns the result as a string.
    /// </Summary>
    public static SparkColumn FormatNumber(string col ,Expression d) => new(FunctionWrappedCall("format_number", false, col,d));

    /// <Summary>
    /// FormatNumber
    /// Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places with HALF_EVEN round mode, and returns the result as a string.
    /// </Summary>
    
	public static SparkColumn FormatNumber(SparkColumn col ,Expression d) => new(FunctionWrappedCall("format_number", false, col,d));

    /// <Summary>
    /// FormatNumber
    /// Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places with HALF_EVEN round mode, and returns the result as a string.
    /// </Summary>
    
	public static SparkColumn FormatNumber(Expression col,Expression d) => new(FunctionWrappedCall("format_number", false, col,d));

    /// <Summary>
    /// Repeat
    /// Repeats a string column n times, and returns it as a new string column.
    /// </Summary>
    public static SparkColumn Repeat(string col ,Expression n) => new(FunctionWrappedCall("repeat", false, col,n));

    /// <Summary>
    /// Repeat
    /// Repeats a string column n times, and returns it as a new string column.
    /// </Summary>
    
	public static SparkColumn Repeat(SparkColumn col ,Expression n) => new(FunctionWrappedCall("repeat", false, col,n));

    /// <Summary>
    /// Repeat
    /// Repeats a string column n times, and returns it as a new string column.
    /// </Summary>
    
	public static SparkColumn Repeat(Expression col,Expression n) => new(FunctionWrappedCall("repeat", false, col,n));

    /// <Summary>
    /// Rlike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    public static SparkColumn Rlike(string str,string regexp) => new(FunctionWrappedCall("rlike", false, str,regexp));

    /// <Summary>
    /// Rlike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    
	public static SparkColumn Rlike(SparkColumn str,SparkColumn regexp) => new(FunctionWrappedCall("rlike", false, str,regexp));

    /// <Summary>
    /// Rlike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    
	public static SparkColumn Rlike(Expression str,Expression regexp) => new(FunctionWrappedCall("rlike", false, str,regexp));

    /// <Summary>
    /// Regexp
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    public static SparkColumn Regexp(string str,string regexp) => new(FunctionWrappedCall("regexp", false, str,regexp));

    /// <Summary>
    /// Regexp
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    
	public static SparkColumn Regexp(SparkColumn str,SparkColumn regexp) => new(FunctionWrappedCall("regexp", false, str,regexp));

    /// <Summary>
    /// Regexp
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    
	public static SparkColumn Regexp(Expression str,Expression regexp) => new(FunctionWrappedCall("regexp", false, str,regexp));

    /// <Summary>
    /// RegexpLike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    public static SparkColumn RegexpLike(string str,string regexp) => new(FunctionWrappedCall("regexp_like", false, str,regexp));

    /// <Summary>
    /// RegexpLike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    
	public static SparkColumn RegexpLike(SparkColumn str,SparkColumn regexp) => new(FunctionWrappedCall("regexp_like", false, str,regexp));

    /// <Summary>
    /// RegexpLike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    
	public static SparkColumn RegexpLike(Expression str,Expression regexp) => new(FunctionWrappedCall("regexp_like", false, str,regexp));

    /// <Summary>
    /// RegexpCount
    /// Returns a count of the number of times that the Java regex pattern `regexp` is matched in the string `str`.
    /// </Summary>
    public static SparkColumn RegexpCount(string str,string regexp) => new(FunctionWrappedCall("regexp_count", false, str,regexp));

    /// <Summary>
    /// RegexpCount
    /// Returns a count of the number of times that the Java regex pattern `regexp` is matched in the string `str`.
    /// </Summary>
    
	public static SparkColumn RegexpCount(SparkColumn str,SparkColumn regexp) => new(FunctionWrappedCall("regexp_count", false, str,regexp));

    /// <Summary>
    /// RegexpCount
    /// Returns a count of the number of times that the Java regex pattern `regexp` is matched in the string `str`.
    /// </Summary>
    
	public static SparkColumn RegexpCount(Expression str,Expression regexp) => new(FunctionWrappedCall("regexp_count", false, str,regexp));

    /// <Summary>
    /// RegexpSubstr
    /// Returns the substring that matches the Java regex `regexp` within the string `str`. If the regular expression is not found, the result is null.
    /// </Summary>
    public static SparkColumn RegexpSubstr(string str,string regexp) => new(FunctionWrappedCall("regexp_substr", false, str,regexp));

    /// <Summary>
    /// RegexpSubstr
    /// Returns the substring that matches the Java regex `regexp` within the string `str`. If the regular expression is not found, the result is null.
    /// </Summary>
    
	public static SparkColumn RegexpSubstr(SparkColumn str,SparkColumn regexp) => new(FunctionWrappedCall("regexp_substr", false, str,regexp));

    /// <Summary>
    /// RegexpSubstr
    /// Returns the substring that matches the Java regex `regexp` within the string `str`. If the regular expression is not found, the result is null.
    /// </Summary>
    
	public static SparkColumn RegexpSubstr(Expression str,Expression regexp) => new(FunctionWrappedCall("regexp_substr", false, str,regexp));

    /// <Summary>Initcap</Summary>
    public static SparkColumn Initcap(string col) => new(FunctionWrappedCall("initcap", false, col));

    /// <Summary>Initcap</Summary>
	public static SparkColumn Initcap(SparkColumn col) => new(FunctionWrappedCall("initcap", false, col));

    /// <Summary>Initcap</Summary>
	public static SparkColumn Initcap(Expression col) => new(FunctionWrappedCall("initcap", false, col));

    /// <Summary>Soundex</Summary>
    public static SparkColumn Soundex(string col) => new(FunctionWrappedCall("soundex", false, col));

    /// <Summary>Soundex</Summary>
	public static SparkColumn Soundex(SparkColumn col) => new(FunctionWrappedCall("soundex", false, col));

    /// <Summary>Soundex</Summary>
	public static SparkColumn Soundex(Expression col) => new(FunctionWrappedCall("soundex", false, col));

    /// <Summary>Bin</Summary>
    public static SparkColumn Bin(string col) => new(FunctionWrappedCall("bin", false, col));

    /// <Summary>Bin</Summary>
	public static SparkColumn Bin(SparkColumn col) => new(FunctionWrappedCall("bin", false, col));

    /// <Summary>Bin</Summary>
	public static SparkColumn Bin(Expression col) => new(FunctionWrappedCall("bin", false, col));

    /// <Summary>Hex</Summary>
    public static SparkColumn Hex(string col) => new(FunctionWrappedCall("hex", false, col));

    /// <Summary>Hex</Summary>
	public static SparkColumn Hex(SparkColumn col) => new(FunctionWrappedCall("hex", false, col));

    /// <Summary>Hex</Summary>
	public static SparkColumn Hex(Expression col) => new(FunctionWrappedCall("hex", false, col));

    /// <Summary>Unhex</Summary>
    public static SparkColumn Unhex(string col) => new(FunctionWrappedCall("unhex", false, col));

    /// <Summary>Unhex</Summary>
	public static SparkColumn Unhex(SparkColumn col) => new(FunctionWrappedCall("unhex", false, col));

    /// <Summary>Unhex</Summary>
	public static SparkColumn Unhex(Expression col) => new(FunctionWrappedCall("unhex", false, col));

    /// <Summary>Length</Summary>
    public static SparkColumn Length(string col) => new(FunctionWrappedCall("length", false, col));

    /// <Summary>Length</Summary>
	public static SparkColumn Length(SparkColumn col) => new(FunctionWrappedCall("length", false, col));

    /// <Summary>Length</Summary>
	public static SparkColumn Length(Expression col) => new(FunctionWrappedCall("length", false, col));

    /// <Summary>OctetLength</Summary>
    public static SparkColumn OctetLength(string col) => new(FunctionWrappedCall("octet_length", false, col));

    /// <Summary>OctetLength</Summary>
	public static SparkColumn OctetLength(SparkColumn col) => new(FunctionWrappedCall("octet_length", false, col));

    /// <Summary>OctetLength</Summary>
	public static SparkColumn OctetLength(Expression col) => new(FunctionWrappedCall("octet_length", false, col));

    /// <Summary>BitLength</Summary>
    public static SparkColumn BitLength(string col) => new(FunctionWrappedCall("bit_length", false, col));

    /// <Summary>BitLength</Summary>
	public static SparkColumn BitLength(SparkColumn col) => new(FunctionWrappedCall("bit_length", false, col));

    /// <Summary>BitLength</Summary>
	public static SparkColumn BitLength(Expression col) => new(FunctionWrappedCall("bit_length", false, col));

    /// <Summary>UrlDecode</Summary>
    public static SparkColumn UrlDecode(string col) => new(FunctionWrappedCall("url_decode", false, col));

    /// <Summary>UrlDecode</Summary>
	public static SparkColumn UrlDecode(SparkColumn col) => new(FunctionWrappedCall("url_decode", false, col));

    /// <Summary>UrlDecode</Summary>
	public static SparkColumn UrlDecode(Expression col) => new(FunctionWrappedCall("url_decode", false, col));

    /// <Summary>UrlEncode</Summary>
    public static SparkColumn UrlEncode(string col) => new(FunctionWrappedCall("url_encode", false, col));

    /// <Summary>UrlEncode</Summary>
	public static SparkColumn UrlEncode(SparkColumn col) => new(FunctionWrappedCall("url_encode", false, col));

    /// <Summary>UrlEncode</Summary>
	public static SparkColumn UrlEncode(Expression col) => new(FunctionWrappedCall("url_encode", false, col));

    /// <Summary>
    /// Endswith
    /// Returns a boolean. The value is True if str ends with suffix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or suffix must be of STRING or BINARY type.
    /// </Summary>
    public static SparkColumn Endswith(string str,string suffix) => new(FunctionWrappedCall("endswith", false, str,suffix));

    /// <Summary>
    /// Endswith
    /// Returns a boolean. The value is True if str ends with suffix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or suffix must be of STRING or BINARY type.
    /// </Summary>
    
	public static SparkColumn Endswith(SparkColumn str,SparkColumn suffix) => new(FunctionWrappedCall("endswith", false, str,suffix));

    /// <Summary>
    /// Endswith
    /// Returns a boolean. The value is True if str ends with suffix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or suffix must be of STRING or BINARY type.
    /// </Summary>
    
	public static SparkColumn Endswith(Expression str,Expression suffix) => new(FunctionWrappedCall("endswith", false, str,suffix));

    /// <Summary>
    /// Startswith
    /// Returns a boolean. The value is True if str starts with prefix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or prefix must be of STRING or BINARY type.
    /// </Summary>
    public static SparkColumn Startswith(string str,string prefix) => new(FunctionWrappedCall("startswith", false, str,prefix));

    /// <Summary>
    /// Startswith
    /// Returns a boolean. The value is True if str starts with prefix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or prefix must be of STRING or BINARY type.
    /// </Summary>
    
	public static SparkColumn Startswith(SparkColumn str,SparkColumn prefix) => new(FunctionWrappedCall("startswith", false, str,prefix));

    /// <Summary>
    /// Startswith
    /// Returns a boolean. The value is True if str starts with prefix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or prefix must be of STRING or BINARY type.
    /// </Summary>
    
	public static SparkColumn Startswith(Expression str,Expression prefix) => new(FunctionWrappedCall("startswith", false, str,prefix));

    /// <Summary>Char</Summary>
    public static SparkColumn Char(string col) => new(FunctionWrappedCall("char", false, col));

    /// <Summary>Char</Summary>
	public static SparkColumn Char(SparkColumn col) => new(FunctionWrappedCall("char", false, col));

    /// <Summary>Char</Summary>
	public static SparkColumn Char(Expression col) => new(FunctionWrappedCall("char", false, col));

    /// <Summary>CharLength</Summary>
    public static SparkColumn CharLength(string col) => new(FunctionWrappedCall("char_length", false, col));

    /// <Summary>CharLength</Summary>
	public static SparkColumn CharLength(SparkColumn col) => new(FunctionWrappedCall("char_length", false, col));

    /// <Summary>CharLength</Summary>
	public static SparkColumn CharLength(Expression col) => new(FunctionWrappedCall("char_length", false, col));

    /// <Summary>CharacterLength</Summary>
    public static SparkColumn CharacterLength(string col) => new(FunctionWrappedCall("character_length", false, col));

    /// <Summary>CharacterLength</Summary>
	public static SparkColumn CharacterLength(SparkColumn col) => new(FunctionWrappedCall("character_length", false, col));

    /// <Summary>CharacterLength</Summary>
	public static SparkColumn CharacterLength(Expression col) => new(FunctionWrappedCall("character_length", false, col));

    /// <Summary>
    /// Contains
    /// Returns a boolean. The value is True if right is found inside left. Returns NULL if either input expression is NULL. Otherwise, returns False. Both left or right must be of STRING or BINARY type.
    /// </Summary>
    public static SparkColumn Contains(string left,string right) => new(FunctionWrappedCall("contains", false, left,right));

    /// <Summary>
    /// Contains
    /// Returns a boolean. The value is True if right is found inside left. Returns NULL if either input expression is NULL. Otherwise, returns False. Both left or right must be of STRING or BINARY type.
    /// </Summary>
    
	public static SparkColumn Contains(SparkColumn left,SparkColumn right) => new(FunctionWrappedCall("contains", false, left,right));

    /// <Summary>
    /// Contains
    /// Returns a boolean. The value is True if right is found inside left. Returns NULL if either input expression is NULL. Otherwise, returns False. Both left or right must be of STRING or BINARY type.
    /// </Summary>
    
	public static SparkColumn Contains(Expression left,Expression right) => new(FunctionWrappedCall("contains", false, left,right));

    /// <Summary>
    /// FindInSet
    /// Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`strArray`). Returns 0, if the string was not found or if the given string (`str`) contains a comma.
    /// </Summary>
    public static SparkColumn FindInSet(string str,string str_array) => new(FunctionWrappedCall("find_in_set", false, str,str_array));

    /// <Summary>
    /// FindInSet
    /// Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`strArray`). Returns 0, if the string was not found or if the given string (`str`) contains a comma.
    /// </Summary>
    
	public static SparkColumn FindInSet(SparkColumn str,SparkColumn str_array) => new(FunctionWrappedCall("find_in_set", false, str,str_array));

    /// <Summary>
    /// FindInSet
    /// Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`strArray`). Returns 0, if the string was not found or if the given string (`str`) contains a comma.
    /// </Summary>
    
	public static SparkColumn FindInSet(Expression str,Expression str_array) => new(FunctionWrappedCall("find_in_set", false, str,str_array));

    /// <Summary>Lcase</Summary>
    public static SparkColumn Lcase(string col) => new(FunctionWrappedCall("lcase", false, col));

    /// <Summary>Lcase</Summary>
	public static SparkColumn Lcase(SparkColumn col) => new(FunctionWrappedCall("lcase", false, col));

    /// <Summary>Lcase</Summary>
	public static SparkColumn Lcase(Expression col) => new(FunctionWrappedCall("lcase", false, col));

    /// <Summary>Ucase</Summary>
    public static SparkColumn Ucase(string col) => new(FunctionWrappedCall("ucase", false, col));

    /// <Summary>Ucase</Summary>
	public static SparkColumn Ucase(SparkColumn col) => new(FunctionWrappedCall("ucase", false, col));

    /// <Summary>Ucase</Summary>
	public static SparkColumn Ucase(Expression col) => new(FunctionWrappedCall("ucase", false, col));

    /// <Summary>
    /// Left
    /// Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>
    public static SparkColumn Left(string str,string len) => new(FunctionWrappedCall("left", false, str,len));

    /// <Summary>
    /// Left
    /// Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>
    
	public static SparkColumn Left(SparkColumn str,SparkColumn len) => new(FunctionWrappedCall("left", false, str,len));

    /// <Summary>
    /// Left
    /// Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>
    
	public static SparkColumn Left(Expression str,Expression len) => new(FunctionWrappedCall("left", false, str,len));

    /// <Summary>
    /// Right
    /// Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>
    public static SparkColumn Right(string str,string len) => new(FunctionWrappedCall("right", false, str,len));

    /// <Summary>
    /// Right
    /// Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>
    
	public static SparkColumn Right(SparkColumn str,SparkColumn len) => new(FunctionWrappedCall("right", false, str,len));

    /// <Summary>
    /// Right
    /// Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>
    
	public static SparkColumn Right(Expression str,Expression len) => new(FunctionWrappedCall("right", false, str,len));

    /// <Summary>
    /// MapFromArrays
    /// Creates a new map from two arrays.
    /// </Summary>
    public static SparkColumn MapFromArrays(string col1,string col2) => new(FunctionWrappedCall("map_from_arrays", false, col1,col2));

    /// <Summary>
    /// MapFromArrays
    /// Creates a new map from two arrays.
    /// </Summary>
    
	public static SparkColumn MapFromArrays(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("map_from_arrays", false, col1,col2));

    /// <Summary>
    /// MapFromArrays
    /// Creates a new map from two arrays.
    /// </Summary>
    
	public static SparkColumn MapFromArrays(Expression col1,Expression col2) => new(FunctionWrappedCall("map_from_arrays", false, col1,col2));
	/// <Summary>Array</Summary>
	public static SparkColumn Array() => new(FunctionWrappedCall("array", false));
    


    /// <Summary>
    /// ArraysOverlap
    /// Collection function: returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise.
    /// </Summary>
    public static SparkColumn ArraysOverlap(string a1,string a2) => new(FunctionWrappedCall("arrays_overlap", false, a1,a2));

    /// <Summary>
    /// ArraysOverlap
    /// Collection function: returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise.
    /// </Summary>
    
	public static SparkColumn ArraysOverlap(SparkColumn a1,SparkColumn a2) => new(FunctionWrappedCall("arrays_overlap", false, a1,a2));

    /// <Summary>
    /// ArraysOverlap
    /// Collection function: returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise.
    /// </Summary>
    
	public static SparkColumn ArraysOverlap(Expression a1,Expression a2) => new(FunctionWrappedCall("arrays_overlap", false, a1,a2));

    /// <Summary>ArrayDistinct</Summary>
    public static SparkColumn ArrayDistinct(string col) => new(FunctionWrappedCall("array_distinct", false, col));

    /// <Summary>ArrayDistinct</Summary>
	public static SparkColumn ArrayDistinct(SparkColumn col) => new(FunctionWrappedCall("array_distinct", false, col));

    /// <Summary>ArrayDistinct</Summary>
	public static SparkColumn ArrayDistinct(Expression col) => new(FunctionWrappedCall("array_distinct", false, col));

    /// <Summary>
    /// ArrayIntersect
    /// Collection function: returns an array of the elements in the intersection of col1 and col2, without duplicates.
    /// </Summary>
    public static SparkColumn ArrayIntersect(string col1,string col2) => new(FunctionWrappedCall("array_intersect", false, col1,col2));

    /// <Summary>
    /// ArrayIntersect
    /// Collection function: returns an array of the elements in the intersection of col1 and col2, without duplicates.
    /// </Summary>
    
	public static SparkColumn ArrayIntersect(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("array_intersect", false, col1,col2));

    /// <Summary>
    /// ArrayIntersect
    /// Collection function: returns an array of the elements in the intersection of col1 and col2, without duplicates.
    /// </Summary>
    
	public static SparkColumn ArrayIntersect(Expression col1,Expression col2) => new(FunctionWrappedCall("array_intersect", false, col1,col2));

    /// <Summary>
    /// ArrayUnion
    /// Collection function: returns an array of the elements in the union of col1 and col2, without duplicates.
    /// </Summary>
    public static SparkColumn ArrayUnion(string col1,string col2) => new(FunctionWrappedCall("array_union", false, col1,col2));

    /// <Summary>
    /// ArrayUnion
    /// Collection function: returns an array of the elements in the union of col1 and col2, without duplicates.
    /// </Summary>
    
	public static SparkColumn ArrayUnion(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("array_union", false, col1,col2));

    /// <Summary>
    /// ArrayUnion
    /// Collection function: returns an array of the elements in the union of col1 and col2, without duplicates.
    /// </Summary>
    
	public static SparkColumn ArrayUnion(Expression col1,Expression col2) => new(FunctionWrappedCall("array_union", false, col1,col2));

    /// <Summary>
    /// ArrayExcept
    /// Collection function: returns an array of the elements in col1 but not in col2, without duplicates.
    /// </Summary>
    public static SparkColumn ArrayExcept(string col1,string col2) => new(FunctionWrappedCall("array_except", false, col1,col2));

    /// <Summary>
    /// ArrayExcept
    /// Collection function: returns an array of the elements in col1 but not in col2, without duplicates.
    /// </Summary>
    
	public static SparkColumn ArrayExcept(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("array_except", false, col1,col2));

    /// <Summary>
    /// ArrayExcept
    /// Collection function: returns an array of the elements in col1 but not in col2, without duplicates.
    /// </Summary>
    
	public static SparkColumn ArrayExcept(Expression col1,Expression col2) => new(FunctionWrappedCall("array_except", false, col1,col2));

    /// <Summary>ArrayCompact</Summary>
    public static SparkColumn ArrayCompact(string col) => new(FunctionWrappedCall("array_compact", false, col));

    /// <Summary>ArrayCompact</Summary>
	public static SparkColumn ArrayCompact(SparkColumn col) => new(FunctionWrappedCall("array_compact", false, col));

    /// <Summary>ArrayCompact</Summary>
	public static SparkColumn ArrayCompact(Expression col) => new(FunctionWrappedCall("array_compact", false, col));

    /// <Summary>Explode</Summary>
    public static SparkColumn Explode(string col) => new(FunctionWrappedCall("explode", false, col));

    /// <Summary>Explode</Summary>
	public static SparkColumn Explode(SparkColumn col) => new(FunctionWrappedCall("explode", false, col));

    /// <Summary>Explode</Summary>
	public static SparkColumn Explode(Expression col) => new(FunctionWrappedCall("explode", false, col));

    /// <Summary>Posexplode</Summary>
    public static SparkColumn Posexplode(string col) => new(FunctionWrappedCall("posexplode", false, col));

    /// <Summary>Posexplode</Summary>
	public static SparkColumn Posexplode(SparkColumn col) => new(FunctionWrappedCall("posexplode", false, col));

    /// <Summary>Posexplode</Summary>
	public static SparkColumn Posexplode(Expression col) => new(FunctionWrappedCall("posexplode", false, col));

    /// <Summary>Inline</Summary>
    public static SparkColumn Inline(string col) => new(FunctionWrappedCall("inline", false, col));

    /// <Summary>Inline</Summary>
	public static SparkColumn Inline(SparkColumn col) => new(FunctionWrappedCall("inline", false, col));

    /// <Summary>Inline</Summary>
	public static SparkColumn Inline(Expression col) => new(FunctionWrappedCall("inline", false, col));

    /// <Summary>ExplodeOuter</Summary>
    public static SparkColumn ExplodeOuter(string col) => new(FunctionWrappedCall("explode_outer", false, col));

    /// <Summary>ExplodeOuter</Summary>
	public static SparkColumn ExplodeOuter(SparkColumn col) => new(FunctionWrappedCall("explode_outer", false, col));

    /// <Summary>ExplodeOuter</Summary>
	public static SparkColumn ExplodeOuter(Expression col) => new(FunctionWrappedCall("explode_outer", false, col));

    /// <Summary>PosexplodeOuter</Summary>
    public static SparkColumn PosexplodeOuter(string col) => new(FunctionWrappedCall("posexplode_outer", false, col));

    /// <Summary>PosexplodeOuter</Summary>
	public static SparkColumn PosexplodeOuter(SparkColumn col) => new(FunctionWrappedCall("posexplode_outer", false, col));

    /// <Summary>PosexplodeOuter</Summary>
	public static SparkColumn PosexplodeOuter(Expression col) => new(FunctionWrappedCall("posexplode_outer", false, col));

    /// <Summary>InlineOuter</Summary>
    public static SparkColumn InlineOuter(string col) => new(FunctionWrappedCall("inline_outer", false, col));

    /// <Summary>InlineOuter</Summary>
	public static SparkColumn InlineOuter(SparkColumn col) => new(FunctionWrappedCall("inline_outer", false, col));

    /// <Summary>InlineOuter</Summary>
	public static SparkColumn InlineOuter(Expression col) => new(FunctionWrappedCall("inline_outer", false, col));

    /// <Summary>JsonArrayLength</Summary>
    public static SparkColumn JsonArrayLength(string col) => new(FunctionWrappedCall("json_array_length", false, col));

    /// <Summary>JsonArrayLength</Summary>
	public static SparkColumn JsonArrayLength(SparkColumn col) => new(FunctionWrappedCall("json_array_length", false, col));

    /// <Summary>JsonArrayLength</Summary>
	public static SparkColumn JsonArrayLength(Expression col) => new(FunctionWrappedCall("json_array_length", false, col));

    /// <Summary>JsonObjectKeys</Summary>
    public static SparkColumn JsonObjectKeys(string col) => new(FunctionWrappedCall("json_object_keys", false, col));

    /// <Summary>JsonObjectKeys</Summary>
	public static SparkColumn JsonObjectKeys(SparkColumn col) => new(FunctionWrappedCall("json_object_keys", false, col));

    /// <Summary>JsonObjectKeys</Summary>
	public static SparkColumn JsonObjectKeys(Expression col) => new(FunctionWrappedCall("json_object_keys", false, col));

    /// <Summary>Size</Summary>
    public static SparkColumn Size(string col) => new(FunctionWrappedCall("size", false, col));

    /// <Summary>Size</Summary>
	public static SparkColumn Size(SparkColumn col) => new(FunctionWrappedCall("size", false, col));

    /// <Summary>Size</Summary>
	public static SparkColumn Size(Expression col) => new(FunctionWrappedCall("size", false, col));

    /// <Summary>ArrayMin</Summary>
    public static SparkColumn ArrayMin(string col) => new(FunctionWrappedCall("array_min", false, col));

    /// <Summary>ArrayMin</Summary>
	public static SparkColumn ArrayMin(SparkColumn col) => new(FunctionWrappedCall("array_min", false, col));

    /// <Summary>ArrayMin</Summary>
	public static SparkColumn ArrayMin(Expression col) => new(FunctionWrappedCall("array_min", false, col));

    /// <Summary>ArrayMax</Summary>
    public static SparkColumn ArrayMax(string col) => new(FunctionWrappedCall("array_max", false, col));

    /// <Summary>ArrayMax</Summary>
	public static SparkColumn ArrayMax(SparkColumn col) => new(FunctionWrappedCall("array_max", false, col));

    /// <Summary>ArrayMax</Summary>
	public static SparkColumn ArrayMax(Expression col) => new(FunctionWrappedCall("array_max", false, col));

    /// <Summary>ArraySize</Summary>
    public static SparkColumn ArraySize(string col) => new(FunctionWrappedCall("array_size", false, col));

    /// <Summary>ArraySize</Summary>
	public static SparkColumn ArraySize(SparkColumn col) => new(FunctionWrappedCall("array_size", false, col));

    /// <Summary>ArraySize</Summary>
	public static SparkColumn ArraySize(Expression col) => new(FunctionWrappedCall("array_size", false, col));

    /// <Summary>Cardinality</Summary>
    public static SparkColumn Cardinality(string col) => new(FunctionWrappedCall("cardinality", false, col));

    /// <Summary>Cardinality</Summary>
	public static SparkColumn Cardinality(SparkColumn col) => new(FunctionWrappedCall("cardinality", false, col));

    /// <Summary>Cardinality</Summary>
	public static SparkColumn Cardinality(Expression col) => new(FunctionWrappedCall("cardinality", false, col));

    /// <Summary>Shuffle</Summary>
    public static SparkColumn Shuffle(string col) => new(FunctionWrappedCall("shuffle", false, col));

    /// <Summary>Shuffle</Summary>
	public static SparkColumn Shuffle(SparkColumn col) => new(FunctionWrappedCall("shuffle", false, col));

    /// <Summary>Shuffle</Summary>
	public static SparkColumn Shuffle(Expression col) => new(FunctionWrappedCall("shuffle", false, col));

    /// <Summary>Reverse</Summary>
    public static SparkColumn Reverse(string col) => new(FunctionWrappedCall("reverse", false, col));

    /// <Summary>Reverse</Summary>
	public static SparkColumn Reverse(SparkColumn col) => new(FunctionWrappedCall("reverse", false, col));

    /// <Summary>Reverse</Summary>
	public static SparkColumn Reverse(Expression col) => new(FunctionWrappedCall("reverse", false, col));

    /// <Summary>Flatten</Summary>
    public static SparkColumn Flatten(string col) => new(FunctionWrappedCall("flatten", false, col));

    /// <Summary>Flatten</Summary>
	public static SparkColumn Flatten(SparkColumn col) => new(FunctionWrappedCall("flatten", false, col));

    /// <Summary>Flatten</Summary>
	public static SparkColumn Flatten(Expression col) => new(FunctionWrappedCall("flatten", false, col));

    /// <Summary>MapKeys</Summary>
    public static SparkColumn MapKeys(string col) => new(FunctionWrappedCall("map_keys", false, col));

    /// <Summary>MapKeys</Summary>
	public static SparkColumn MapKeys(SparkColumn col) => new(FunctionWrappedCall("map_keys", false, col));

    /// <Summary>MapKeys</Summary>
	public static SparkColumn MapKeys(Expression col) => new(FunctionWrappedCall("map_keys", false, col));

    /// <Summary>MapValues</Summary>
    public static SparkColumn MapValues(string col) => new(FunctionWrappedCall("map_values", false, col));

    /// <Summary>MapValues</Summary>
	public static SparkColumn MapValues(SparkColumn col) => new(FunctionWrappedCall("map_values", false, col));

    /// <Summary>MapValues</Summary>
	public static SparkColumn MapValues(Expression col) => new(FunctionWrappedCall("map_values", false, col));

    /// <Summary>MapEntries</Summary>
    public static SparkColumn MapEntries(string col) => new(FunctionWrappedCall("map_entries", false, col));

    /// <Summary>MapEntries</Summary>
	public static SparkColumn MapEntries(SparkColumn col) => new(FunctionWrappedCall("map_entries", false, col));

    /// <Summary>MapEntries</Summary>
	public static SparkColumn MapEntries(Expression col) => new(FunctionWrappedCall("map_entries", false, col));

    /// <Summary>MapFromEntries</Summary>
    public static SparkColumn MapFromEntries(string col) => new(FunctionWrappedCall("map_from_entries", false, col));

    /// <Summary>MapFromEntries</Summary>
	public static SparkColumn MapFromEntries(SparkColumn col) => new(FunctionWrappedCall("map_from_entries", false, col));

    /// <Summary>MapFromEntries</Summary>
	public static SparkColumn MapFromEntries(Expression col) => new(FunctionWrappedCall("map_from_entries", false, col));
	/// <Summary>MapConcat</Summary>
	public static SparkColumn MapConcat() => new(FunctionWrappedCall("map_concat", false));
    


    /// <Summary>Years, NOTE: This is untested</Summary>
    public static SparkColumn Years(string col) => new(FunctionWrappedCall("years", false, col));

    /// <Summary>Years, NOTE: This is untested</Summary>
	public static SparkColumn Years(SparkColumn col) => new(FunctionWrappedCall("years", false, col));

    /// <Summary>Years, NOTE: This is untested</Summary>
	public static SparkColumn Years(Expression col) => new(FunctionWrappedCall("years", false, col));

    /// <Summary>Months, NOTE: This is untested</Summary>
    public static SparkColumn Months(string col) => new(FunctionWrappedCall("months", false, col));

    /// <Summary>Months, NOTE: This is untested</Summary>
	public static SparkColumn Months(SparkColumn col) => new(FunctionWrappedCall("months", false, col));

    /// <Summary>Months, NOTE: This is untested</Summary>
	public static SparkColumn Months(Expression col) => new(FunctionWrappedCall("months", false, col));

    /// <Summary>Days, NOTE: This is untested</Summary>
    public static SparkColumn Days(string col) => new(FunctionWrappedCall("days", false, col));

    /// <Summary>Days, NOTE: This is untested</Summary>
	public static SparkColumn Days(SparkColumn col) => new(FunctionWrappedCall("days", false, col));

    /// <Summary>Days, NOTE: This is untested</Summary>
	public static SparkColumn Days(Expression col) => new(FunctionWrappedCall("days", false, col));

    /// <Summary>Hours, NOTE: This is untested</Summary>
    public static SparkColumn Hours(string col) => new(FunctionWrappedCall("hours", false, col));

    /// <Summary>Hours, NOTE: This is untested</Summary>
	public static SparkColumn Hours(SparkColumn col) => new(FunctionWrappedCall("hours", false, col));

    /// <Summary>Hours, NOTE: This is untested</Summary>
	public static SparkColumn Hours(Expression col) => new(FunctionWrappedCall("hours", false, col));

    /// <Summary>
    /// MakeTimestampNtz
    /// Create local date-time from years, months, days, hours, mins, secs fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.
    /// </Summary>
    public static SparkColumn MakeTimestampNtz(string years,string months,string days,string hours,string mins,string secs) => new(FunctionWrappedCall("make_timestamp_ntz", false, years,months,days,hours,mins,secs));

    /// <Summary>
    /// MakeTimestampNtz
    /// Create local date-time from years, months, days, hours, mins, secs fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.
    /// </Summary>
    
	public static SparkColumn MakeTimestampNtz(SparkColumn years,SparkColumn months,SparkColumn days,SparkColumn hours,SparkColumn mins,SparkColumn secs) => new(FunctionWrappedCall("make_timestamp_ntz", false, years,months,days,hours,mins,secs));

    /// <Summary>
    /// MakeTimestampNtz
    /// Create local date-time from years, months, days, hours, mins, secs fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.
    /// </Summary>
    
	public static SparkColumn MakeTimestampNtz(Expression years,Expression months,Expression days,Expression hours,Expression mins,Expression secs) => new(FunctionWrappedCall("make_timestamp_ntz", false, years,months,days,hours,mins,secs));

    /// <Summary>
    /// Ifnull
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>
    public static SparkColumn Ifnull(string col1,string col2) => new(FunctionWrappedCall("ifnull", false, col1,col2));

    /// <Summary>
    /// Ifnull
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>
    
	public static SparkColumn Ifnull(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("ifnull", false, col1,col2));

    /// <Summary>
    /// Ifnull
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>
    
	public static SparkColumn Ifnull(Expression col1,Expression col2) => new(FunctionWrappedCall("ifnull", false, col1,col2));

    /// <Summary>Isnotnull</Summary>
    public static SparkColumn Isnotnull(string col) => new(FunctionWrappedCall("isnotnull", false, col));

    /// <Summary>Isnotnull</Summary>
	public static SparkColumn Isnotnull(SparkColumn col) => new(FunctionWrappedCall("isnotnull", false, col));

    /// <Summary>Isnotnull</Summary>
	public static SparkColumn Isnotnull(Expression col) => new(FunctionWrappedCall("isnotnull", false, col));

    /// <Summary>
    /// EqualNull
    /// Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.
    /// </Summary>
    public static SparkColumn EqualNull(string col1,string col2) => new(FunctionWrappedCall("equal_null", false, col1,col2));

    /// <Summary>
    /// EqualNull
    /// Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.
    /// </Summary>
    
	public static SparkColumn EqualNull(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("equal_null", false, col1,col2));

    /// <Summary>
    /// EqualNull
    /// Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.
    /// </Summary>
    
	public static SparkColumn EqualNull(Expression col1,Expression col2) => new(FunctionWrappedCall("equal_null", false, col1,col2));

    /// <Summary>
    /// Nullif
    /// Returns null if `col1` equals to `col2`, or `col1` otherwise.
    /// </Summary>
    public static SparkColumn Nullif(string col1,string col2) => new(FunctionWrappedCall("nullif", false, col1,col2));

    /// <Summary>
    /// Nullif
    /// Returns null if `col1` equals to `col2`, or `col1` otherwise.
    /// </Summary>
    
	public static SparkColumn Nullif(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("nullif", false, col1,col2));

    /// <Summary>
    /// Nullif
    /// Returns null if `col1` equals to `col2`, or `col1` otherwise.
    /// </Summary>
    
	public static SparkColumn Nullif(Expression col1,Expression col2) => new(FunctionWrappedCall("nullif", false, col1,col2));

    /// <Summary>
    /// Nvl
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>
    public static SparkColumn Nvl(string col1,string col2) => new(FunctionWrappedCall("nvl", false, col1,col2));

    /// <Summary>
    /// Nvl
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>
    
	public static SparkColumn Nvl(SparkColumn col1,SparkColumn col2) => new(FunctionWrappedCall("nvl", false, col1,col2));

    /// <Summary>
    /// Nvl
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>
    
	public static SparkColumn Nvl(Expression col1,Expression col2) => new(FunctionWrappedCall("nvl", false, col1,col2));

    /// <Summary>
    /// Nvl2
    /// Returns `col2` if `col1` is not null, or `col3` otherwise.
    /// </Summary>
    public static SparkColumn Nvl2(string col1,string col2,string col3) => new(FunctionWrappedCall("nvl2", false, col1,col2,col3));

    /// <Summary>
    /// Nvl2
    /// Returns `col2` if `col1` is not null, or `col3` otherwise.
    /// </Summary>
    
	public static SparkColumn Nvl2(SparkColumn col1,SparkColumn col2,SparkColumn col3) => new(FunctionWrappedCall("nvl2", false, col1,col2,col3));

    /// <Summary>
    /// Nvl2
    /// Returns `col2` if `col1` is not null, or `col3` otherwise.
    /// </Summary>
    
	public static SparkColumn Nvl2(Expression col1,Expression col2,Expression col3) => new(FunctionWrappedCall("nvl2", false, col1,col2,col3));

    /// <Summary>Sha</Summary>
    public static SparkColumn Sha(string col) => new(FunctionWrappedCall("sha", false, col));

    /// <Summary>Sha</Summary>
	public static SparkColumn Sha(SparkColumn col) => new(FunctionWrappedCall("sha", false, col));

    /// <Summary>Sha</Summary>
	public static SparkColumn Sha(Expression col) => new(FunctionWrappedCall("sha", false, col));
	/// <Summary>InputFileBlockLength</Summary>
	public static SparkColumn InputFileBlockLength() => new(FunctionWrappedCall("input_file_block_length", false));
    

	/// <Summary>InputFileBlockStart</Summary>
	public static SparkColumn InputFileBlockStart() => new(FunctionWrappedCall("input_file_block_start", false));
    

	/// <Summary>Version</Summary>
	public static SparkColumn Version() => new(FunctionWrappedCall("version", false));
    


    /// <Summary>Typeof</Summary>
    public static SparkColumn Typeof(string col) => new(FunctionWrappedCall("typeof", false, col));

    /// <Summary>Typeof</Summary>
	public static SparkColumn Typeof(SparkColumn col) => new(FunctionWrappedCall("typeof", false, col));

    /// <Summary>Typeof</Summary>
	public static SparkColumn Typeof(Expression col) => new(FunctionWrappedCall("typeof", false, col));

    /// <Summary>BitmapBitPosition</Summary>
    public static SparkColumn BitmapBitPosition(string col) => new(FunctionWrappedCall("bitmap_bit_position", false, col));

    /// <Summary>BitmapBitPosition</Summary>
	public static SparkColumn BitmapBitPosition(SparkColumn col) => new(FunctionWrappedCall("bitmap_bit_position", false, col));

    /// <Summary>BitmapBitPosition</Summary>
	public static SparkColumn BitmapBitPosition(Expression col) => new(FunctionWrappedCall("bitmap_bit_position", false, col));

    /// <Summary>BitmapBucketNumber</Summary>
    public static SparkColumn BitmapBucketNumber(string col) => new(FunctionWrappedCall("bitmap_bucket_number", false, col));

    /// <Summary>BitmapBucketNumber</Summary>
	public static SparkColumn BitmapBucketNumber(SparkColumn col) => new(FunctionWrappedCall("bitmap_bucket_number", false, col));

    /// <Summary>BitmapBucketNumber</Summary>
	public static SparkColumn BitmapBucketNumber(Expression col) => new(FunctionWrappedCall("bitmap_bucket_number", false, col));

    /// <Summary>BitmapConstructAgg</Summary>
    public static SparkColumn BitmapConstructAgg(string col) => new(FunctionWrappedCall("bitmap_construct_agg", false, col));

    /// <Summary>BitmapConstructAgg</Summary>
	public static SparkColumn BitmapConstructAgg(SparkColumn col) => new(FunctionWrappedCall("bitmap_construct_agg", false, col));

    /// <Summary>BitmapConstructAgg</Summary>
	public static SparkColumn BitmapConstructAgg(Expression col) => new(FunctionWrappedCall("bitmap_construct_agg", false, col));

    /// <Summary>BitmapCount</Summary>
    public static SparkColumn BitmapCount(string col) => new(FunctionWrappedCall("bitmap_count", false, col));

    /// <Summary>BitmapCount</Summary>
	public static SparkColumn BitmapCount(SparkColumn col) => new(FunctionWrappedCall("bitmap_count", false, col));

    /// <Summary>BitmapCount</Summary>
	public static SparkColumn BitmapCount(Expression col) => new(FunctionWrappedCall("bitmap_count", false, col));

    /// <Summary>BitmapOrAgg</Summary>
    public static SparkColumn BitmapOrAgg(string col) => new(FunctionWrappedCall("bitmap_or_agg", false, col));

    /// <Summary>BitmapOrAgg</Summary>
	public static SparkColumn BitmapOrAgg(SparkColumn col) => new(FunctionWrappedCall("bitmap_or_agg", false, col));

    /// <Summary>BitmapOrAgg</Summary>
	public static SparkColumn BitmapOrAgg(Expression col) => new(FunctionWrappedCall("bitmap_or_agg", false, col));

}

