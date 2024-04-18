
namespace Spark.Connect.Dotnet.Sql;

public partial class Functions : FunctionsWrapper
{


    /// <Summary>Asc</Summary>
    public static Column Asc(string col) => new(FunctionWrappedCall("asc", false, col));

    /// <Summary>Asc</Summary>
	public static Column Asc(Column col) => new(FunctionWrappedCall("asc", false, col));

    /// <Summary>Desc</Summary>
    public static Column Desc(string col) => new(FunctionWrappedCall("desc", false, col));

    /// <Summary>Desc</Summary>
	public static Column Desc(Column col) => new(FunctionWrappedCall("desc", false, col));

    /// <Summary>Sqrt</Summary>
    public static Column Sqrt(string col) => new(FunctionWrappedCall("sqrt", false, col));

    /// <Summary>Sqrt</Summary>
	public static Column Sqrt(Column col) => new(FunctionWrappedCall("sqrt", false, col));

    /// <Summary>
    /// TryAdd
    /// Returns the sum of `left`and `right` and the result is null on overflow. The acceptable input types are the same with the `+` operator.
    /// </Summary>
    public static Column TryAdd(string left, string right) => new(FunctionWrappedCall("try_add", false, left, right));

    /// <Summary>
    /// TryAdd
    /// Returns the sum of `left`and `right` and the result is null on overflow. The acceptable input types are the same with the `+` operator.
    /// </Summary>    
	public static Column TryAdd(Column left, Column right) => new(FunctionWrappedCall("try_add", false, left, right));


    /// <Summary>TryAvg</Summary>
    public static Column TryAvg(string col) => new(FunctionWrappedCall("try_avg", false, col));

    /// <Summary>TryAvg</Summary>
	public static Column TryAvg(Column col) => new(FunctionWrappedCall("try_avg", false, col));

    /// <Summary>
    /// TryDivide
    /// Returns `dividend`/`divisor`. It always performs floating point division. Its result is always null if `divisor` is 0.
    /// </Summary>
    public static Column TryDivide(string left, string right) => new(FunctionWrappedCall("try_divide", false, left, right));

    /// <Summary>
    /// TryDivide
    /// Returns `dividend`/`divisor`. It always performs floating point division. Its result is always null if `divisor` is 0.
    /// </Summary>    
	public static Column TryDivide(Column left, Column right) => new(FunctionWrappedCall("try_divide", false, left, right));


    /// <Summary>
    /// TryMultiply
    /// Returns `left`*`right` and the result is null on overflow. The acceptable input types are the same with the `*` operator.
    /// </Summary>
    public static Column TryMultiply(string left, string right) => new(FunctionWrappedCall("try_multiply", false, left, right));

    /// <Summary>
    /// TryMultiply
    /// Returns `left`*`right` and the result is null on overflow. The acceptable input types are the same with the `*` operator.
    /// </Summary>    
	public static Column TryMultiply(Column left, Column right) => new(FunctionWrappedCall("try_multiply", false, left, right));


    /// <Summary>
    /// TrySubtract
    /// Returns `left`-`right` and the result is null on overflow. The acceptable input types are the same with the `-` operator.
    /// </Summary>
    public static Column TrySubtract(string left, string right) => new(FunctionWrappedCall("try_subtract", false, left, right));

    /// <Summary>
    /// TrySubtract
    /// Returns `left`-`right` and the result is null on overflow. The acceptable input types are the same with the `-` operator.
    /// </Summary>    
	public static Column TrySubtract(Column left, Column right) => new(FunctionWrappedCall("try_subtract", false, left, right));


    /// <Summary>TrySum</Summary>
    public static Column TrySum(string col) => new(FunctionWrappedCall("try_sum", false, col));

    /// <Summary>TrySum</Summary>
	public static Column TrySum(Column col) => new(FunctionWrappedCall("try_sum", false, col));

    /// <Summary>Abs</Summary>
    public static Column Abs(string col) => new(FunctionWrappedCall("abs", false, col));

    /// <Summary>Abs</Summary>
	public static Column Abs(Column col) => new(FunctionWrappedCall("abs", false, col));

    /// <Summary>Mode</Summary>
    public static Column Mode(string col) => new(FunctionWrappedCall("mode", false, col));

    /// <Summary>Mode</Summary>
	public static Column Mode(Column col) => new(FunctionWrappedCall("mode", false, col));

    /// <Summary>Max</Summary>
    public static Column Max(string col) => new(FunctionWrappedCall("max", false, col));

    /// <Summary>Max</Summary>
	public static Column Max(Column col) => new(FunctionWrappedCall("max", false, col));

    /// <Summary>Min</Summary>
    public static Column Min(string col) => new(FunctionWrappedCall("min", false, col));

    /// <Summary>Min</Summary>
	public static Column Min(Column col) => new(FunctionWrappedCall("min", false, col));

    /// <Summary>
    /// MaxBy
    /// Returns the value associated with the maximum value of ord.
    /// </Summary>
    public static Column MaxBy(string col, string ord) => new(FunctionWrappedCall("max_by", false, col, ord));

    /// <Summary>
    /// MaxBy
    /// Returns the value associated with the maximum value of ord.
    /// </Summary>    
	public static Column MaxBy(Column col, Column ord) => new(FunctionWrappedCall("max_by", false, col, ord));


    /// <Summary>
    /// MinBy
    /// Returns the value associated with the minimum value of ord.
    /// </Summary>
    public static Column MinBy(string col, string ord) => new(FunctionWrappedCall("min_by", false, col, ord));

    /// <Summary>
    /// MinBy
    /// Returns the value associated with the minimum value of ord.
    /// </Summary>    
	public static Column MinBy(Column col, Column ord) => new(FunctionWrappedCall("min_by", false, col, ord));


    /// <Summary>Count</Summary>
    public static Column Count(string col) => new(FunctionWrappedCall("count", false, col));

    /// <Summary>Count</Summary>
	public static Column Count(Column col) => new(FunctionWrappedCall("count", false, col));

    /// <Summary>Sum</Summary>
    public static Column Sum(string col) => new(FunctionWrappedCall("sum", false, col));

    /// <Summary>Sum</Summary>
	public static Column Sum(Column col) => new(FunctionWrappedCall("sum", false, col));

    /// <Summary>Avg</Summary>
    public static Column Avg(string col) => new(FunctionWrappedCall("avg", false, col));

    /// <Summary>Avg</Summary>
	public static Column Avg(Column col) => new(FunctionWrappedCall("avg", false, col));

    /// <Summary>Mean</Summary>
    public static Column Mean(string col) => new(FunctionWrappedCall("mean", false, col));

    /// <Summary>Mean</Summary>
	public static Column Mean(Column col) => new(FunctionWrappedCall("mean", false, col));

    /// <Summary>Median</Summary>
    public static Column Median(string col) => new(FunctionWrappedCall("median", false, col));

    /// <Summary>Median</Summary>
	public static Column Median(Column col) => new(FunctionWrappedCall("median", false, col));

    /// <Summary>Product</Summary>
    public static Column Product(string col) => new(FunctionWrappedCall("product", false, col));

    /// <Summary>Product</Summary>
	public static Column Product(Column col) => new(FunctionWrappedCall("product", false, col));

    /// <Summary>Acos</Summary>
    public static Column Acos(string col) => new(FunctionWrappedCall("acos", false, col));

    /// <Summary>Acos</Summary>
	public static Column Acos(Column col) => new(FunctionWrappedCall("acos", false, col));

    /// <Summary>Acosh</Summary>
    public static Column Acosh(string col) => new(FunctionWrappedCall("acosh", false, col));

    /// <Summary>Acosh</Summary>
	public static Column Acosh(Column col) => new(FunctionWrappedCall("acosh", false, col));

    /// <Summary>Asin</Summary>
    public static Column Asin(string col) => new(FunctionWrappedCall("asin", false, col));

    /// <Summary>Asin</Summary>
	public static Column Asin(Column col) => new(FunctionWrappedCall("asin", false, col));

    /// <Summary>Asinh</Summary>
    public static Column Asinh(string col) => new(FunctionWrappedCall("asinh", false, col));

    /// <Summary>Asinh</Summary>
	public static Column Asinh(Column col) => new(FunctionWrappedCall("asinh", false, col));

    /// <Summary>Atan</Summary>
    public static Column Atan(string col) => new(FunctionWrappedCall("atan", false, col));

    /// <Summary>Atan</Summary>
	public static Column Atan(Column col) => new(FunctionWrappedCall("atan", false, col));

    /// <Summary>Atanh</Summary>
    public static Column Atanh(string col) => new(FunctionWrappedCall("atanh", false, col));

    /// <Summary>Atanh</Summary>
	public static Column Atanh(Column col) => new(FunctionWrappedCall("atanh", false, col));

    /// <Summary>Cbrt</Summary>
    public static Column Cbrt(string col) => new(FunctionWrappedCall("cbrt", false, col));

    /// <Summary>Cbrt</Summary>
	public static Column Cbrt(Column col) => new(FunctionWrappedCall("cbrt", false, col));

    /// <Summary>Ceil</Summary>
    public static Column Ceil(string col) => new(FunctionWrappedCall("ceil", false, col));

    /// <Summary>Ceil</Summary>
	public static Column Ceil(Column col) => new(FunctionWrappedCall("ceil", false, col));

    /// <Summary>Ceiling</Summary>
    public static Column Ceiling(string col) => new(FunctionWrappedCall("ceiling", false, col));

    /// <Summary>Ceiling</Summary>
	public static Column Ceiling(Column col) => new(FunctionWrappedCall("ceiling", false, col));

    /// <Summary>Cos</Summary>
    public static Column Cos(string col) => new(FunctionWrappedCall("cos", false, col));

    /// <Summary>Cos</Summary>
	public static Column Cos(Column col) => new(FunctionWrappedCall("cos", false, col));

    /// <Summary>Cosh</Summary>
    public static Column Cosh(string col) => new(FunctionWrappedCall("cosh", false, col));

    /// <Summary>Cosh</Summary>
	public static Column Cosh(Column col) => new(FunctionWrappedCall("cosh", false, col));

    /// <Summary>Cot</Summary>
    public static Column Cot(string col) => new(FunctionWrappedCall("cot", false, col));

    /// <Summary>Cot</Summary>
	public static Column Cot(Column col) => new(FunctionWrappedCall("cot", false, col));

    /// <Summary>Csc</Summary>
    public static Column Csc(string col) => new(FunctionWrappedCall("csc", false, col));

    /// <Summary>Csc</Summary>
	public static Column Csc(Column col) => new(FunctionWrappedCall("csc", false, col));
	/// <Summary>E</Summary>
	public static Column E() => new(FunctionWrappedCall("e", false));
    


    /// <Summary>Exp</Summary>
    public static Column Exp(string col) => new(FunctionWrappedCall("exp", false, col));

    /// <Summary>Exp</Summary>
	public static Column Exp(Column col) => new(FunctionWrappedCall("exp", false, col));

    /// <Summary>Expm1</Summary>
    public static Column Expm1(string col) => new(FunctionWrappedCall("expm1", false, col));

    /// <Summary>Expm1</Summary>
	public static Column Expm1(Column col) => new(FunctionWrappedCall("expm1", false, col));

    /// <Summary>Floor</Summary>
    public static Column Floor(string col) => new(FunctionWrappedCall("floor", false, col));

    /// <Summary>Floor</Summary>
	public static Column Floor(Column col) => new(FunctionWrappedCall("floor", false, col));

    /// <Summary>Log</Summary>
    public static Column Log(string col) => new(FunctionWrappedCall("log", false, col));

    /// <Summary>Log</Summary>
	public static Column Log(Column col) => new(FunctionWrappedCall("log", false, col));

    /// <Summary>Log10</Summary>
    public static Column Log10(string col) => new(FunctionWrappedCall("log10", false, col));

    /// <Summary>Log10</Summary>
	public static Column Log10(Column col) => new(FunctionWrappedCall("log10", false, col));

    /// <Summary>Log1p</Summary>
    public static Column Log1p(string col) => new(FunctionWrappedCall("log1p", false, col));

    /// <Summary>Log1p</Summary>
	public static Column Log1p(Column col) => new(FunctionWrappedCall("log1p", false, col));

    /// <Summary>Negative</Summary>
    public static Column Negative(string col) => new(FunctionWrappedCall("negative", false, col));

    /// <Summary>Negative</Summary>
	public static Column Negative(Column col) => new(FunctionWrappedCall("negative", false, col));
	/// <Summary>Pi</Summary>
	public static Column Pi() => new(FunctionWrappedCall("pi", false));
    


    /// <Summary>Positive</Summary>
    public static Column Positive(string col) => new(FunctionWrappedCall("positive", false, col));

    /// <Summary>Positive</Summary>
	public static Column Positive(Column col) => new(FunctionWrappedCall("positive", false, col));

    /// <Summary>Rint</Summary>
    public static Column Rint(string col) => new(FunctionWrappedCall("rint", false, col));

    /// <Summary>Rint</Summary>
	public static Column Rint(Column col) => new(FunctionWrappedCall("rint", false, col));

    /// <Summary>Sec</Summary>
    public static Column Sec(string col) => new(FunctionWrappedCall("sec", false, col));

    /// <Summary>Sec</Summary>
	public static Column Sec(Column col) => new(FunctionWrappedCall("sec", false, col));

    /// <Summary>Signum</Summary>
    public static Column Signum(string col) => new(FunctionWrappedCall("signum", false, col));

    /// <Summary>Signum</Summary>
	public static Column Signum(Column col) => new(FunctionWrappedCall("signum", false, col));

    /// <Summary>Sign</Summary>
    public static Column Sign(string col) => new(FunctionWrappedCall("sign", false, col));

    /// <Summary>Sign</Summary>
	public static Column Sign(Column col) => new(FunctionWrappedCall("sign", false, col));

    /// <Summary>Sin</Summary>
    public static Column Sin(string col) => new(FunctionWrappedCall("sin", false, col));

    /// <Summary>Sin</Summary>
	public static Column Sin(Column col) => new(FunctionWrappedCall("sin", false, col));

    /// <Summary>Sinh</Summary>
    public static Column Sinh(string col) => new(FunctionWrappedCall("sinh", false, col));

    /// <Summary>Sinh</Summary>
	public static Column Sinh(Column col) => new(FunctionWrappedCall("sinh", false, col));

    /// <Summary>Tan</Summary>
    public static Column Tan(string col) => new(FunctionWrappedCall("tan", false, col));

    /// <Summary>Tan</Summary>
	public static Column Tan(Column col) => new(FunctionWrappedCall("tan", false, col));

    /// <Summary>Tanh</Summary>
    public static Column Tanh(string col) => new(FunctionWrappedCall("tanh", false, col));

    /// <Summary>Tanh</Summary>
	public static Column Tanh(Column col) => new(FunctionWrappedCall("tanh", false, col));

    /// <Summary>BitwiseNot</Summary>
    public static Column BitwiseNot(string col) => new(FunctionWrappedCall("~", false, col));

    /// <Summary>BitwiseNot</Summary>
	public static Column BitwiseNot(Column col) => new(FunctionWrappedCall("~", false, col));

    /// <Summary>BitCount</Summary>
    public static Column BitCount(string col) => new(FunctionWrappedCall("bit_count", false, col));

    /// <Summary>BitCount</Summary>
	public static Column BitCount(Column col) => new(FunctionWrappedCall("bit_count", false, col));

    /// <Summary>
    /// BitGet
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    public static Column BitGet(string col , Column pos) => new(FunctionWrappedCall("bit_get", false, Col(col), pos));

    /// <Summary>
    /// BitGet
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    public static Column BitGet(Column col, Column pos) => new(FunctionWrappedCall("bit_get", false, col, pos));

    /// <Summary>
    /// Getbit
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    public static Column Getbit(string col , Column pos) => new(FunctionWrappedCall("getbit", false, Col(col), pos));

    /// <Summary>
    /// Getbit
    /// Returns the value of the bit (0 or 1) at the specified position. The positions are numbered from right to left, starting at zero. The position argument cannot be negative.
    /// </Summary>
    public static Column Getbit(Column col, Column pos) => new(FunctionWrappedCall("getbit", false, col, pos));

    /// <Summary>AscNullsFirst</Summary>
    public static Column AscNullsFirst(string col) => new(FunctionWrappedCall("asc_nulls_first", false, col));

    /// <Summary>AscNullsFirst</Summary>
	public static Column AscNullsFirst(Column col) => new(FunctionWrappedCall("asc_nulls_first", false, col));

    /// <Summary>AscNullsLast</Summary>
    public static Column AscNullsLast(string col) => new(FunctionWrappedCall("asc_nulls_last", false, col));

    /// <Summary>AscNullsLast</Summary>
	public static Column AscNullsLast(Column col) => new(FunctionWrappedCall("asc_nulls_last", false, col));

    /// <Summary>DescNullsFirst</Summary>
    public static Column DescNullsFirst(string col) => new(FunctionWrappedCall("desc_nulls_first", false, col));

    /// <Summary>DescNullsFirst</Summary>
	public static Column DescNullsFirst(Column col) => new(FunctionWrappedCall("desc_nulls_first", false, col));

    /// <Summary>DescNullsLast</Summary>
    public static Column DescNullsLast(string col) => new(FunctionWrappedCall("desc_nulls_last", false, col));

    /// <Summary>DescNullsLast</Summary>
	public static Column DescNullsLast(Column col) => new(FunctionWrappedCall("desc_nulls_last", false, col));

    /// <Summary>Stddev</Summary>
    public static Column Stddev(string col) => new(FunctionWrappedCall("stddev", false, col));

    /// <Summary>Stddev</Summary>
	public static Column Stddev(Column col) => new(FunctionWrappedCall("stddev", false, col));

    /// <Summary>Std</Summary>
    public static Column Std(string col) => new(FunctionWrappedCall("std", false, col));

    /// <Summary>Std</Summary>
	public static Column Std(Column col) => new(FunctionWrappedCall("std", false, col));

    /// <Summary>StddevSamp</Summary>
    public static Column StddevSamp(string col) => new(FunctionWrappedCall("stddev_samp", false, col));

    /// <Summary>StddevSamp</Summary>
	public static Column StddevSamp(Column col) => new(FunctionWrappedCall("stddev_samp", false, col));

    /// <Summary>StddevPop</Summary>
    public static Column StddevPop(string col) => new(FunctionWrappedCall("stddev_pop", false, col));

    /// <Summary>StddevPop</Summary>
	public static Column StddevPop(Column col) => new(FunctionWrappedCall("stddev_pop", false, col));

    /// <Summary>Variance</Summary>
    public static Column Variance(string col) => new(FunctionWrappedCall("variance", false, col));

    /// <Summary>Variance</Summary>
	public static Column Variance(Column col) => new(FunctionWrappedCall("variance", false, col));

    /// <Summary>VarSamp</Summary>
    public static Column VarSamp(string col) => new(FunctionWrappedCall("var_samp", false, col));

    /// <Summary>VarSamp</Summary>
	public static Column VarSamp(Column col) => new(FunctionWrappedCall("var_samp", false, col));

    /// <Summary>VarPop</Summary>
    public static Column VarPop(string col) => new(FunctionWrappedCall("var_pop", false, col));

    /// <Summary>VarPop</Summary>
	public static Column VarPop(Column col) => new(FunctionWrappedCall("var_pop", false, col));

    /// <Summary>
    /// RegrAvgx
    /// Aggregate function: returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrAvgx(string y, string x) => new(FunctionWrappedCall("regr_avgx", false, y, x));

    /// <Summary>
    /// RegrAvgx
    /// Aggregate function: returns the average of the independent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrAvgx(Column y, Column x) => new(FunctionWrappedCall("regr_avgx", false, y, x));


    /// <Summary>
    /// RegrAvgy
    /// Aggregate function: returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrAvgy(string y, string x) => new(FunctionWrappedCall("regr_avgy", false, y, x));

    /// <Summary>
    /// RegrAvgy
    /// Aggregate function: returns the average of the dependent variable for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrAvgy(Column y, Column x) => new(FunctionWrappedCall("regr_avgy", false, y, x));


    /// <Summary>
    /// RegrCount
    /// Aggregate function: returns the number of non-null number pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrCount(string y, string x) => new(FunctionWrappedCall("regr_count", false, y, x));

    /// <Summary>
    /// RegrCount
    /// Aggregate function: returns the number of non-null number pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrCount(Column y, Column x) => new(FunctionWrappedCall("regr_count", false, y, x));


    /// <Summary>
    /// RegrIntercept
    /// Aggregate function: returns the intercept of the univariate linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrIntercept(string y, string x) => new(FunctionWrappedCall("regr_intercept", false, y, x));

    /// <Summary>
    /// RegrIntercept
    /// Aggregate function: returns the intercept of the univariate linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrIntercept(Column y, Column x) => new(FunctionWrappedCall("regr_intercept", false, y, x));


    /// <Summary>
    /// RegrR2
    /// Aggregate function: returns the coefficient of determination for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrR2(string y, string x) => new(FunctionWrappedCall("regr_r2", false, y, x));

    /// <Summary>
    /// RegrR2
    /// Aggregate function: returns the coefficient of determination for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrR2(Column y, Column x) => new(FunctionWrappedCall("regr_r2", false, y, x));


    /// <Summary>
    /// RegrSlope
    /// Aggregate function: returns the slope of the linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrSlope(string y, string x) => new(FunctionWrappedCall("regr_slope", false, y, x));

    /// <Summary>
    /// RegrSlope
    /// Aggregate function: returns the slope of the linear regression line for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrSlope(Column y, Column x) => new(FunctionWrappedCall("regr_slope", false, y, x));


    /// <Summary>
    /// RegrSxx
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrSxx(string y, string x) => new(FunctionWrappedCall("regr_sxx", false, y, x));

    /// <Summary>
    /// RegrSxx
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrSxx(Column y, Column x) => new(FunctionWrappedCall("regr_sxx", false, y, x));


    /// <Summary>
    /// RegrSxy
    /// Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrSxy(string y, string x) => new(FunctionWrappedCall("regr_sxy", false, y, x));

    /// <Summary>
    /// RegrSxy
    /// Aggregate function: returns REGR_COUNT(y, x) * COVAR_POP(y, x) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrSxy(Column y, Column x) => new(FunctionWrappedCall("regr_sxy", false, y, x));


    /// <Summary>
    /// RegrSyy
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>
    public static Column RegrSyy(string y, string x) => new(FunctionWrappedCall("regr_syy", false, y, x));

    /// <Summary>
    /// RegrSyy
    /// Aggregate function: returns REGR_COUNT(y, x) * VAR_POP(y) for non-null pairs in a group, where `y` is the dependent variable and `x` is the independent variable.
    /// </Summary>    
	public static Column RegrSyy(Column y, Column x) => new(FunctionWrappedCall("regr_syy", false, y, x));


    /// <Summary>Every</Summary>
    public static Column Every(string col) => new(FunctionWrappedCall("every", false, col));

    /// <Summary>Every</Summary>
	public static Column Every(Column col) => new(FunctionWrappedCall("every", false, col));

    /// <Summary>BoolAnd</Summary>
    public static Column BoolAnd(string col) => new(FunctionWrappedCall("bool_and", false, col));

    /// <Summary>BoolAnd</Summary>
	public static Column BoolAnd(Column col) => new(FunctionWrappedCall("bool_and", false, col));

    /// <Summary>Some</Summary>
    public static Column Some(string col) => new(FunctionWrappedCall("some", false, col));

    /// <Summary>Some</Summary>
	public static Column Some(Column col) => new(FunctionWrappedCall("some", false, col));

    /// <Summary>BoolOr</Summary>
    public static Column BoolOr(string col) => new(FunctionWrappedCall("bool_or", false, col));

    /// <Summary>BoolOr</Summary>
	public static Column BoolOr(Column col) => new(FunctionWrappedCall("bool_or", false, col));

    /// <Summary>BitAnd</Summary>
    public static Column BitAnd(string col) => new(FunctionWrappedCall("bit_and", false, col));

    /// <Summary>BitAnd</Summary>
	public static Column BitAnd(Column col) => new(FunctionWrappedCall("bit_and", false, col));

    /// <Summary>BitOr</Summary>
    public static Column BitOr(string col) => new(FunctionWrappedCall("bit_or", false, col));

    /// <Summary>BitOr</Summary>
	public static Column BitOr(Column col) => new(FunctionWrappedCall("bit_or", false, col));

    /// <Summary>BitXor</Summary>
    public static Column BitXor(string col) => new(FunctionWrappedCall("bit_xor", false, col));

    /// <Summary>BitXor</Summary>
	public static Column BitXor(Column col) => new(FunctionWrappedCall("bit_xor", false, col));

    /// <Summary>Skewness</Summary>
    public static Column Skewness(string col) => new(FunctionWrappedCall("skewness", false, col));

    /// <Summary>Skewness</Summary>
	public static Column Skewness(Column col) => new(FunctionWrappedCall("skewness", false, col));

    /// <Summary>Kurtosis</Summary>
    public static Column Kurtosis(string col) => new(FunctionWrappedCall("kurtosis", false, col));

    /// <Summary>Kurtosis</Summary>
	public static Column Kurtosis(Column col) => new(FunctionWrappedCall("kurtosis", false, col));

    /// <Summary>CollectList</Summary>
    public static Column CollectList(string col) => new(FunctionWrappedCall("collect_list", false, col));

    /// <Summary>CollectList</Summary>
	public static Column CollectList(Column col) => new(FunctionWrappedCall("collect_list", false, col));

    /// <Summary>ArrayAgg</Summary>
    public static Column ArrayAgg(string col) => new(FunctionWrappedCall("array_agg", false, col));

    /// <Summary>ArrayAgg</Summary>
	public static Column ArrayAgg(Column col) => new(FunctionWrappedCall("array_agg", false, col));

    /// <Summary>CollectSet</Summary>
    public static Column CollectSet(string col) => new(FunctionWrappedCall("collect_set", false, col));

    /// <Summary>CollectSet</Summary>
	public static Column CollectSet(Column col) => new(FunctionWrappedCall("collect_set", false, col));

    /// <Summary>Degrees</Summary>
    public static Column Degrees(string col) => new(FunctionWrappedCall("degrees", false, col));

    /// <Summary>Degrees</Summary>
	public static Column Degrees(Column col) => new(FunctionWrappedCall("degrees", false, col));

    /// <Summary>Radians</Summary>
    public static Column Radians(string col) => new(FunctionWrappedCall("radians", false, col));

    /// <Summary>Radians</Summary>
	public static Column Radians(Column col) => new(FunctionWrappedCall("radians", false, col));
	/// <Summary>RowNumber</Summary>
	public static Column RowNumber() => new(FunctionWrappedCall("row_number", false));
    

	/// <Summary>DenseRank</Summary>
	public static Column DenseRank() => new(FunctionWrappedCall("dense_rank", false));
    

	/// <Summary>Rank</Summary>
	public static Column Rank() => new(FunctionWrappedCall("rank", false));
    

	/// <Summary>CumeDist</Summary>
	public static Column CumeDist() => new(FunctionWrappedCall("cume_dist", false));
    

	/// <Summary>PercentRank</Summary>
	public static Column PercentRank() => new(FunctionWrappedCall("percent_rank", false));
    


    /// <Summary>
    /// Coalesce
    /// Returns the first column that is not null.
    /// </Summary>
    public static Column Coalesce(params string[] cols) => new(FunctionWrappedCall("coalesce", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Coalesce
    /// Returns the first column that is not null.
    /// </Summary>
	public static Column Coalesce(params Column[] cols) => new(FunctionWrappedCall("coalesce", false, cols));


    /// <Summary>
    /// Corr
    /// Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for ``col1`` and ``col2``.
    /// </Summary>
    public static Column Corr(string col1, string col2) => new(FunctionWrappedCall("corr", false, col1, col2));

    /// <Summary>
    /// Corr
    /// Returns a new :class:`~pyspark.sql.Column` for the Pearson Correlation Coefficient for ``col1`` and ``col2``.
    /// </Summary>    
	public static Column Corr(Column col1, Column col2) => new(FunctionWrappedCall("corr", false, col1, col2));


    /// <Summary>
    /// CovarPop
    /// Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and ``col2``.
    /// </Summary>
    public static Column CovarPop(string col1, string col2) => new(FunctionWrappedCall("covar_pop", false, col1, col2));

    /// <Summary>
    /// CovarPop
    /// Returns a new :class:`~pyspark.sql.Column` for the population covariance of ``col1`` and ``col2``.
    /// </Summary>    
	public static Column CovarPop(Column col1, Column col2) => new(FunctionWrappedCall("covar_pop", false, col1, col2));


    /// <Summary>
    /// CovarSamp
    /// Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and ``col2``.
    /// </Summary>
    public static Column CovarSamp(string col1, string col2) => new(FunctionWrappedCall("covar_samp", false, col1, col2));

    /// <Summary>
    /// CovarSamp
    /// Returns a new :class:`~pyspark.sql.Column` for the sample covariance of ``col1`` and ``col2``.
    /// </Summary>    
	public static Column CovarSamp(Column col1, Column col2) => new(FunctionWrappedCall("covar_samp", false, col1, col2));


    /// <Summary>
    /// GroupingId
    /// Aggregate function: returns the level of grouping, equals to
    /// </Summary>
    public static Column GroupingId(params string[] cols) => new(FunctionWrappedCall("grouping_id", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// GroupingId
    /// Aggregate function: returns the level of grouping, equals to
    /// </Summary>
	public static Column GroupingId(params Column[] cols) => new(FunctionWrappedCall("grouping_id", false, cols));

	/// <Summary>InputFileName</Summary>
	public static Column InputFileName() => new(FunctionWrappedCall("input_file_name", false));
    


    /// <Summary>Isnan</Summary>
    public static Column Isnan(string col) => new(FunctionWrappedCall("isnan", false, col));

    /// <Summary>Isnan</Summary>
	public static Column Isnan(Column col) => new(FunctionWrappedCall("isnan", false, col));

    /// <Summary>Isnull</Summary>
    public static Column Isnull(string col) => new(FunctionWrappedCall("isnull", false, col));

    /// <Summary>Isnull</Summary>
	public static Column Isnull(Column col) => new(FunctionWrappedCall("isnull", false, col));
	/// <Summary>MonotonicallyIncreasingId</Summary>
	public static Column MonotonicallyIncreasingId() => new(FunctionWrappedCall("monotonically_increasing_id", false));
    


    /// <Summary>
    /// Nanvl
    /// Returns col1 if it is not NaN, or col2 if col1 is NaN.
    /// </Summary>
    public static Column Nanvl(string col1, string col2) => new(FunctionWrappedCall("nanvl", false, col1, col2));

    /// <Summary>
    /// Nanvl
    /// Returns col1 if it is not NaN, or col2 if col1 is NaN.
    /// </Summary>    
	public static Column Nanvl(Column col1, Column col2) => new(FunctionWrappedCall("nanvl", false, col1, col2));


    /// <Summary>
    /// Round
    /// Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    public static Column Round(string col , Column scale) => new(FunctionWrappedCall("round", false, Col(col), scale));

    /// <Summary>
    /// Round
    /// Round the given value to `scale` decimal places using HALF_UP rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    public static Column Round(Column col, Column scale) => new(FunctionWrappedCall("round", false, col, scale));

    /// <Summary>
    /// Bround
    /// Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    public static Column Bround(string col , Column scale) => new(FunctionWrappedCall("bround", false, Col(col), scale));

    /// <Summary>
    /// Bround
    /// Round the given value to `scale` decimal places using HALF_EVEN rounding mode if `scale` >= 0 or at integral part when `scale` < 0.
    /// </Summary>
    public static Column Bround(Column col, Column scale) => new(FunctionWrappedCall("bround", false, col, scale));

    /// <Summary>
    /// ShiftLeft
    /// Shift the given value numBits left.
    /// </Summary>
    public static Column ShiftLeft(string col , Column numBits) => new(FunctionWrappedCall("shiftLeft", false, Col(col), numBits));

    /// <Summary>
    /// ShiftLeft
    /// Shift the given value numBits left.
    /// </Summary>
    public static Column ShiftLeft(Column col, Column numBits) => new(FunctionWrappedCall("shiftLeft", false, col, numBits));

    /// <Summary>
    /// Shiftleft
    /// Shift the given value numBits left.
    /// </Summary>
    public static Column Shiftleft(string col , Column numBits) => new(FunctionWrappedCall("shiftleft", false, Col(col), numBits));

    /// <Summary>
    /// Shiftleft
    /// Shift the given value numBits left.
    /// </Summary>
    public static Column Shiftleft(Column col, Column numBits) => new(FunctionWrappedCall("shiftleft", false, col, numBits));

    /// <Summary>
    /// ShiftRight
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    public static Column ShiftRight(string col , Column numBits) => new(FunctionWrappedCall("shiftRight", false, Col(col), numBits));

    /// <Summary>
    /// ShiftRight
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    public static Column ShiftRight(Column col, Column numBits) => new(FunctionWrappedCall("shiftRight", false, col, numBits));

    /// <Summary>
    /// Shiftright
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    public static Column Shiftright(string col , Column numBits) => new(FunctionWrappedCall("shiftright", false, Col(col), numBits));

    /// <Summary>
    /// Shiftright
    /// (Signed) shift the given value numBits right.
    /// </Summary>
    public static Column Shiftright(Column col, Column numBits) => new(FunctionWrappedCall("shiftright", false, col, numBits));

    /// <Summary>
    /// ShiftRightUnsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    public static Column ShiftRightUnsigned(string col , Column numBits) => new(FunctionWrappedCall("shiftRightUnsigned", false, Col(col), numBits));

    /// <Summary>
    /// ShiftRightUnsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    public static Column ShiftRightUnsigned(Column col, Column numBits) => new(FunctionWrappedCall("shiftRightUnsigned", false, col, numBits));

    /// <Summary>
    /// Shiftrightunsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    public static Column Shiftrightunsigned(string col , Column numBits) => new(FunctionWrappedCall("shiftrightunsigned", false, Col(col), numBits));

    /// <Summary>
    /// Shiftrightunsigned
    /// Unsigned shift the given value numBits right.
    /// </Summary>
    public static Column Shiftrightunsigned(Column col, Column numBits) => new(FunctionWrappedCall("shiftrightunsigned", false, col, numBits));
	/// <Summary>SparkPartitionId</Summary>
	public static Column SparkPartitionId() => new(FunctionWrappedCall("spark_partition_id", false));
    


    /// <Summary>
    /// NamedStruct
    /// Creates a struct with the given field names and values.
    /// </Summary>
    public static Column NamedStruct(params string[] cols) => new(FunctionWrappedCall("named_struct", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// NamedStruct
    /// Creates a struct with the given field names and values.
    /// </Summary>
	public static Column NamedStruct(params Column[] cols) => new(FunctionWrappedCall("named_struct", false, cols));


    /// <Summary>
    /// Greatest
    /// Returns the greatest value of the list of column names, skipping null values. This function takes at least 2 parameters. It will return null if all parameters are null.
    /// </Summary>
    public static Column Greatest(params string[] cols) => new(FunctionWrappedCall("greatest", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Greatest
    /// Returns the greatest value of the list of column names, skipping null values. This function takes at least 2 parameters. It will return null if all parameters are null.
    /// </Summary>
	public static Column Greatest(params Column[] cols) => new(FunctionWrappedCall("greatest", false, cols));


    /// <Summary>
    /// Least
    /// Returns the least value of the list of column names, skipping null values. This function takes at least 2 parameters. It will return null if all parameters are null.
    /// </Summary>
    public static Column Least(params string[] cols) => new(FunctionWrappedCall("least", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Least
    /// Returns the least value of the list of column names, skipping null values. This function takes at least 2 parameters. It will return null if all parameters are null.
    /// </Summary>
	public static Column Least(params Column[] cols) => new(FunctionWrappedCall("least", false, cols));


    /// <Summary>Ln</Summary>
    public static Column Ln(string col) => new(FunctionWrappedCall("ln", false, col));

    /// <Summary>Ln</Summary>
	public static Column Ln(Column col) => new(FunctionWrappedCall("ln", false, col));

    /// <Summary>Log2</Summary>
    public static Column Log2(string col) => new(FunctionWrappedCall("log2", false, col));

    /// <Summary>Log2</Summary>
	public static Column Log2(Column col) => new(FunctionWrappedCall("log2", false, col));

    /// <Summary>Factorial</Summary>
    public static Column Factorial(string col) => new(FunctionWrappedCall("factorial", false, col));

    /// <Summary>Factorial</Summary>
	public static Column Factorial(Column col) => new(FunctionWrappedCall("factorial", false, col));

    /// <Summary>CountIf</Summary>
    public static Column CountIf(string col) => new(FunctionWrappedCall("count_if", false, col));

    /// <Summary>CountIf</Summary>
	public static Column CountIf(Column col) => new(FunctionWrappedCall("count_if", false, col));
	/// <Summary>Curdate</Summary>
	public static Column Curdate() => new(FunctionWrappedCall("curdate", false));
    

	/// <Summary>CurrentDate</Summary>
	public static Column CurrentDate() => new(FunctionWrappedCall("current_date", false));
    

	/// <Summary>CurrentTimezone</Summary>
	public static Column CurrentTimezone() => new(FunctionWrappedCall("current_timezone", false));
    

	/// <Summary>CurrentTimestamp</Summary>
	public static Column CurrentTimestamp() => new(FunctionWrappedCall("current_timestamp", false));
    

	/// <Summary>Now</Summary>
	public static Column Now() => new(FunctionWrappedCall("now", false));
    

	/// <Summary>Localtimestamp</Summary>
	public static Column Localtimestamp() => new(FunctionWrappedCall("localtimestamp", false));
    


    /// <Summary>Year</Summary>
    public static Column Year(string col) => new(FunctionWrappedCall("year", false, col));

    /// <Summary>Year</Summary>
	public static Column Year(Column col) => new(FunctionWrappedCall("year", false, col));

    /// <Summary>Quarter</Summary>
    public static Column Quarter(string col) => new(FunctionWrappedCall("quarter", false, col));

    /// <Summary>Quarter</Summary>
	public static Column Quarter(Column col) => new(FunctionWrappedCall("quarter", false, col));

    /// <Summary>Month</Summary>
    public static Column Month(string col) => new(FunctionWrappedCall("month", false, col));

    /// <Summary>Month</Summary>
	public static Column Month(Column col) => new(FunctionWrappedCall("month", false, col));

    /// <Summary>Dayofweek</Summary>
    public static Column Dayofweek(string col) => new(FunctionWrappedCall("dayofweek", false, col));

    /// <Summary>Dayofweek</Summary>
	public static Column Dayofweek(Column col) => new(FunctionWrappedCall("dayofweek", false, col));

    /// <Summary>Dayofmonth</Summary>
    public static Column Dayofmonth(string col) => new(FunctionWrappedCall("dayofmonth", false, col));

    /// <Summary>Dayofmonth</Summary>
	public static Column Dayofmonth(Column col) => new(FunctionWrappedCall("dayofmonth", false, col));

    /// <Summary>Day</Summary>
    public static Column Day(string col) => new(FunctionWrappedCall("day", false, col));

    /// <Summary>Day</Summary>
	public static Column Day(Column col) => new(FunctionWrappedCall("day", false, col));

    /// <Summary>Dayofyear</Summary>
    public static Column Dayofyear(string col) => new(FunctionWrappedCall("dayofyear", false, col));

    /// <Summary>Dayofyear</Summary>
	public static Column Dayofyear(Column col) => new(FunctionWrappedCall("dayofyear", false, col));

    /// <Summary>Hour</Summary>
    public static Column Hour(string col) => new(FunctionWrappedCall("hour", false, col));

    /// <Summary>Hour</Summary>
	public static Column Hour(Column col) => new(FunctionWrappedCall("hour", false, col));

    /// <Summary>Minute</Summary>
    public static Column Minute(string col) => new(FunctionWrappedCall("minute", false, col));

    /// <Summary>Minute</Summary>
	public static Column Minute(Column col) => new(FunctionWrappedCall("minute", false, col));

    /// <Summary>Second</Summary>
    public static Column Second(string col) => new(FunctionWrappedCall("second", false, col));

    /// <Summary>Second</Summary>
	public static Column Second(Column col) => new(FunctionWrappedCall("second", false, col));

    /// <Summary>Weekofyear</Summary>
    public static Column Weekofyear(string col) => new(FunctionWrappedCall("weekofyear", false, col));

    /// <Summary>Weekofyear</Summary>
	public static Column Weekofyear(Column col) => new(FunctionWrappedCall("weekofyear", false, col));

    /// <Summary>Weekday</Summary>
    public static Column Weekday(string col) => new(FunctionWrappedCall("weekday", false, col));

    /// <Summary>Weekday</Summary>
	public static Column Weekday(Column col) => new(FunctionWrappedCall("weekday", false, col));

    /// <Summary>
    /// MakeDate
    /// Returns a column with a date built from the year, month and day columns.
    /// </Summary>
    public static Column MakeDate(string year, string month, string day) => new(FunctionWrappedCall("make_date", false, year, month, day));

    /// <Summary>
    /// MakeDate
    /// Returns a column with a date built from the year, month and day columns.
    /// </Summary>    
	public static Column MakeDate(Column year, Column month, Column day) => new(FunctionWrappedCall("make_date", false, year, month, day));


    /// <Summary>
    /// Datediff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>
    public static Column Datediff(string end, string start) => new(FunctionWrappedCall("datediff", false, end, start));

    /// <Summary>
    /// Datediff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>    
	public static Column Datediff(Column end, Column start) => new(FunctionWrappedCall("datediff", false, end, start));


    /// <Summary>
    /// DateDiff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>
    public static Column DateDiff(string end, string start) => new(FunctionWrappedCall("date_diff", false, end, start));

    /// <Summary>
    /// DateDiff
    /// Returns the number of days from `start` to `end`.
    /// </Summary>    
	public static Column DateDiff(Column end, Column start) => new(FunctionWrappedCall("date_diff", false, end, start));


    /// <Summary>DateFromUnixDate</Summary>
    public static Column DateFromUnixDate(string col) => new(FunctionWrappedCall("date_from_unix_date", false, col));

    /// <Summary>DateFromUnixDate</Summary>
	public static Column DateFromUnixDate(Column col) => new(FunctionWrappedCall("date_from_unix_date", false, col));

    /// <Summary>UnixDate</Summary>
    public static Column UnixDate(string col) => new(FunctionWrappedCall("unix_date", false, col));

    /// <Summary>UnixDate</Summary>
	public static Column UnixDate(Column col) => new(FunctionWrappedCall("unix_date", false, col));

    /// <Summary>UnixMicros</Summary>
    public static Column UnixMicros(string col) => new(FunctionWrappedCall("unix_micros", false, col));

    /// <Summary>UnixMicros</Summary>
	public static Column UnixMicros(Column col) => new(FunctionWrappedCall("unix_micros", false, col));

    /// <Summary>UnixMillis</Summary>
    public static Column UnixMillis(string col) => new(FunctionWrappedCall("unix_millis", false, col));

    /// <Summary>UnixMillis</Summary>
	public static Column UnixMillis(Column col) => new(FunctionWrappedCall("unix_millis", false, col));

    /// <Summary>UnixSeconds</Summary>
    public static Column UnixSeconds(string col) => new(FunctionWrappedCall("unix_seconds", false, col));

    /// <Summary>UnixSeconds</Summary>
	public static Column UnixSeconds(Column col) => new(FunctionWrappedCall("unix_seconds", false, col));

    /// <Summary>ToTimestamp</Summary>
    public static Column ToTimestamp(string col) => new(FunctionWrappedCall("to_timestamp", false, col));

    /// <Summary>ToTimestamp</Summary>
	public static Column ToTimestamp(Column col) => new(FunctionWrappedCall("to_timestamp", false, col));

    /// <Summary>LastDay</Summary>
    public static Column LastDay(string col) => new(FunctionWrappedCall("last_day", false, col));

    /// <Summary>LastDay</Summary>
	public static Column LastDay(Column col) => new(FunctionWrappedCall("last_day", false, col));
	/// <Summary>UnixTimestamp</Summary>
	public static Column UnixTimestamp() => new(FunctionWrappedCall("unix_timestamp", false));
    


    /// <Summary>TimestampSeconds</Summary>
    public static Column TimestampSeconds(string col) => new(FunctionWrappedCall("timestamp_seconds", false, col));

    /// <Summary>TimestampSeconds</Summary>
	public static Column TimestampSeconds(Column col) => new(FunctionWrappedCall("timestamp_seconds", false, col));

    /// <Summary>TimestampMillis</Summary>
    public static Column TimestampMillis(string col) => new(FunctionWrappedCall("timestamp_millis", false, col));

    /// <Summary>TimestampMillis</Summary>
	public static Column TimestampMillis(Column col) => new(FunctionWrappedCall("timestamp_millis", false, col));

    /// <Summary>TimestampMicros</Summary>
    public static Column TimestampMicros(string col) => new(FunctionWrappedCall("timestamp_micros", false, col));

    /// <Summary>TimestampMicros</Summary>
	public static Column TimestampMicros(Column col) => new(FunctionWrappedCall("timestamp_micros", false, col));
	/// <Summary>CurrentCatalog</Summary>
	public static Column CurrentCatalog() => new(FunctionWrappedCall("current_catalog", false));
    

	/// <Summary>CurrentDatabase</Summary>
	public static Column CurrentDatabase() => new(FunctionWrappedCall("current_database", false));
    

	/// <Summary>CurrentSchema</Summary>
	public static Column CurrentSchema() => new(FunctionWrappedCall("current_schema", false));
    

	/// <Summary>CurrentUser</Summary>
	public static Column CurrentUser() => new(FunctionWrappedCall("current_user", false));
    

	/// <Summary>User</Summary>
	public static Column User() => new(FunctionWrappedCall("user", false));
    


    /// <Summary>Crc32</Summary>
    public static Column Crc32(string col) => new(FunctionWrappedCall("crc32", false, col));

    /// <Summary>Crc32</Summary>
	public static Column Crc32(Column col) => new(FunctionWrappedCall("crc32", false, col));

    /// <Summary>Md5</Summary>
    public static Column Md5(string col) => new(FunctionWrappedCall("md5", false, col));

    /// <Summary>Md5</Summary>
	public static Column Md5(Column col) => new(FunctionWrappedCall("md5", false, col));

    /// <Summary>Sha1</Summary>
    public static Column Sha1(string col) => new(FunctionWrappedCall("sha1", false, col));

    /// <Summary>Sha1</Summary>
	public static Column Sha1(Column col) => new(FunctionWrappedCall("sha1", false, col));

    /// <Summary>
    /// Hash
    /// Calculates the hash code of given columns, and returns the result as an int column.
    /// </Summary>
    public static Column Hash(params string[] cols) => new(FunctionWrappedCall("hash", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Hash
    /// Calculates the hash code of given columns, and returns the result as an int column.
    /// </Summary>
	public static Column Hash(params Column[] cols) => new(FunctionWrappedCall("hash", false, cols));


    /// <Summary>
    /// Xxhash64
    /// Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm, and returns the result as a long column. The hash computation uses an initial seed of 42.
    /// </Summary>
    public static Column Xxhash64(params string[] cols) => new(FunctionWrappedCall("xxhash64", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Xxhash64
    /// Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm, and returns the result as a long column. The hash computation uses an initial seed of 42.
    /// </Summary>
	public static Column Xxhash64(params Column[] cols) => new(FunctionWrappedCall("xxhash64", false, cols));


    /// <Summary>Upper</Summary>
    public static Column Upper(string col) => new(FunctionWrappedCall("upper", false, col));

    /// <Summary>Upper</Summary>
	public static Column Upper(Column col) => new(FunctionWrappedCall("upper", false, col));

    /// <Summary>Lower</Summary>
    public static Column Lower(string col) => new(FunctionWrappedCall("lower", false, col));

    /// <Summary>Lower</Summary>
	public static Column Lower(Column col) => new(FunctionWrappedCall("lower", false, col));

    /// <Summary>Ascii</Summary>
    public static Column Ascii(string col) => new(FunctionWrappedCall("ascii", false, col));

    /// <Summary>Ascii</Summary>
	public static Column Ascii(Column col) => new(FunctionWrappedCall("ascii", false, col));

    /// <Summary>Base64</Summary>
    public static Column Base64(string col) => new(FunctionWrappedCall("base64", false, col));

    /// <Summary>Base64</Summary>
	public static Column Base64(Column col) => new(FunctionWrappedCall("base64", false, col));

    /// <Summary>Unbase64</Summary>
    public static Column Unbase64(string col) => new(FunctionWrappedCall("unbase64", false, col));

    /// <Summary>Unbase64</Summary>
	public static Column Unbase64(Column col) => new(FunctionWrappedCall("unbase64", false, col));

    /// <Summary>Ltrim</Summary>
    public static Column Ltrim(string col) => new(FunctionWrappedCall("ltrim", false, col));

    /// <Summary>Ltrim</Summary>
	public static Column Ltrim(Column col) => new(FunctionWrappedCall("ltrim", false, col));

    /// <Summary>Rtrim</Summary>
    public static Column Rtrim(string col) => new(FunctionWrappedCall("rtrim", false, col));

    /// <Summary>Rtrim</Summary>
	public static Column Rtrim(Column col) => new(FunctionWrappedCall("rtrim", false, col));

    /// <Summary>Trim</Summary>
    public static Column Trim(string col) => new(FunctionWrappedCall("trim", false, col));

    /// <Summary>Trim</Summary>
	public static Column Trim(Column col) => new(FunctionWrappedCall("trim", false, col));

    /// <Summary>
    /// FormatNumber
    /// Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places with HALF_EVEN round mode, and returns the result as a string.
    /// </Summary>
    public static Column FormatNumber(string col , Column d) => new(FunctionWrappedCall("format_number", false, Col(col), d));

    /// <Summary>
    /// FormatNumber
    /// Formats the number X to a format like '#,--#,--#.--', rounded to d decimal places with HALF_EVEN round mode, and returns the result as a string.
    /// </Summary>
    public static Column FormatNumber(Column col, Column d) => new(FunctionWrappedCall("format_number", false, col, d));

    /// <Summary>
    /// Repeat
    /// Repeats a string column n times, and returns it as a new string column.
    /// </Summary>
    public static Column Repeat(string col , Column n) => new(FunctionWrappedCall("repeat", false, Col(col), n));

    /// <Summary>
    /// Repeat
    /// Repeats a string column n times, and returns it as a new string column.
    /// </Summary>
    public static Column Repeat(Column col, Column n) => new(FunctionWrappedCall("repeat", false, col, n));

    /// <Summary>
    /// Rlike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    public static Column Rlike(string str, string regexp) => new(FunctionWrappedCall("rlike", false, str, regexp));

    /// <Summary>
    /// Rlike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>    
	public static Column Rlike(Column str, Column regexp) => new(FunctionWrappedCall("rlike", false, str, regexp));


    /// <Summary>
    /// Regexp
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    public static Column Regexp(string str, string regexp) => new(FunctionWrappedCall("regexp", false, str, regexp));

    /// <Summary>
    /// Regexp
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>    
	public static Column Regexp(Column str, Column regexp) => new(FunctionWrappedCall("regexp", false, str, regexp));


    /// <Summary>
    /// RegexpLike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>
    public static Column RegexpLike(string str, string regexp) => new(FunctionWrappedCall("regexp_like", false, str, regexp));

    /// <Summary>
    /// RegexpLike
    /// Returns true if `str` matches the Java regex `regexp`, or false otherwise.
    /// </Summary>    
	public static Column RegexpLike(Column str, Column regexp) => new(FunctionWrappedCall("regexp_like", false, str, regexp));


    /// <Summary>
    /// RegexpCount
    /// Returns a count of the number of times that the Java regex pattern `regexp` is matched in the string `str`.
    /// </Summary>
    public static Column RegexpCount(string str, string regexp) => new(FunctionWrappedCall("regexp_count", false, str, regexp));

    /// <Summary>
    /// RegexpCount
    /// Returns a count of the number of times that the Java regex pattern `regexp` is matched in the string `str`.
    /// </Summary>    
	public static Column RegexpCount(Column str, Column regexp) => new(FunctionWrappedCall("regexp_count", false, str, regexp));


    /// <Summary>
    /// RegexpSubstr
    /// Returns the substring that matches the Java regex `regexp` within the string `str`. If the regular expression is not found, the result is null.
    /// </Summary>
    public static Column RegexpSubstr(string str, string regexp) => new(FunctionWrappedCall("regexp_substr", false, str, regexp));

    /// <Summary>
    /// RegexpSubstr
    /// Returns the substring that matches the Java regex `regexp` within the string `str`. If the regular expression is not found, the result is null.
    /// </Summary>    
	public static Column RegexpSubstr(Column str, Column regexp) => new(FunctionWrappedCall("regexp_substr", false, str, regexp));


    /// <Summary>Initcap</Summary>
    public static Column Initcap(string col) => new(FunctionWrappedCall("initcap", false, col));

    /// <Summary>Initcap</Summary>
	public static Column Initcap(Column col) => new(FunctionWrappedCall("initcap", false, col));

    /// <Summary>Soundex</Summary>
    public static Column Soundex(string col) => new(FunctionWrappedCall("soundex", false, col));

    /// <Summary>Soundex</Summary>
	public static Column Soundex(Column col) => new(FunctionWrappedCall("soundex", false, col));

    /// <Summary>Bin</Summary>
    public static Column Bin(string col) => new(FunctionWrappedCall("bin", false, col));

    /// <Summary>Bin</Summary>
	public static Column Bin(Column col) => new(FunctionWrappedCall("bin", false, col));

    /// <Summary>Hex</Summary>
    public static Column Hex(string col) => new(FunctionWrappedCall("hex", false, col));

    /// <Summary>Hex</Summary>
	public static Column Hex(Column col) => new(FunctionWrappedCall("hex", false, col));

    /// <Summary>Unhex</Summary>
    public static Column Unhex(string col) => new(FunctionWrappedCall("unhex", false, col));

    /// <Summary>Unhex</Summary>
	public static Column Unhex(Column col) => new(FunctionWrappedCall("unhex", false, col));

    /// <Summary>Length</Summary>
    public static Column Length(string col) => new(FunctionWrappedCall("length", false, col));

    /// <Summary>Length</Summary>
	public static Column Length(Column col) => new(FunctionWrappedCall("length", false, col));

    /// <Summary>OctetLength</Summary>
    public static Column OctetLength(string col) => new(FunctionWrappedCall("octet_length", false, col));

    /// <Summary>OctetLength</Summary>
	public static Column OctetLength(Column col) => new(FunctionWrappedCall("octet_length", false, col));

    /// <Summary>BitLength</Summary>
    public static Column BitLength(string col) => new(FunctionWrappedCall("bit_length", false, col));

    /// <Summary>BitLength</Summary>
	public static Column BitLength(Column col) => new(FunctionWrappedCall("bit_length", false, col));

    /// <Summary>UrlDecode</Summary>
    public static Column UrlDecode(string col) => new(FunctionWrappedCall("url_decode", false, col));

    /// <Summary>UrlDecode</Summary>
	public static Column UrlDecode(Column col) => new(FunctionWrappedCall("url_decode", false, col));

    /// <Summary>UrlEncode</Summary>
    public static Column UrlEncode(string col) => new(FunctionWrappedCall("url_encode", false, col));

    /// <Summary>UrlEncode</Summary>
	public static Column UrlEncode(Column col) => new(FunctionWrappedCall("url_encode", false, col));

    /// <Summary>
    /// Endswith
    /// Returns a boolean. The value is True if str ends with suffix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or suffix must be of STRING or BINARY type.
    /// </Summary>
    public static Column Endswith(string str, string suffix) => new(FunctionWrappedCall("endswith", false, str, suffix));

    /// <Summary>
    /// Endswith
    /// Returns a boolean. The value is True if str ends with suffix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or suffix must be of STRING or BINARY type.
    /// </Summary>    
	public static Column Endswith(Column str, Column suffix) => new(FunctionWrappedCall("endswith", false, str, suffix));


    /// <Summary>
    /// Startswith
    /// Returns a boolean. The value is True if str starts with prefix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or prefix must be of STRING or BINARY type.
    /// </Summary>
    public static Column Startswith(string str, string prefix) => new(FunctionWrappedCall("startswith", false, str, prefix));

    /// <Summary>
    /// Startswith
    /// Returns a boolean. The value is True if str starts with prefix. Returns NULL if either input expression is NULL. Otherwise, returns False. Both str or prefix must be of STRING or BINARY type.
    /// </Summary>    
	public static Column Startswith(Column str, Column prefix) => new(FunctionWrappedCall("startswith", false, str, prefix));


    /// <Summary>Char</Summary>
    public static Column Char(string col) => new(FunctionWrappedCall("char", false, col));

    /// <Summary>Char</Summary>
	public static Column Char(Column col) => new(FunctionWrappedCall("char", false, col));

    /// <Summary>CharLength</Summary>
    public static Column CharLength(string col) => new(FunctionWrappedCall("char_length", false, col));

    /// <Summary>CharLength</Summary>
	public static Column CharLength(Column col) => new(FunctionWrappedCall("char_length", false, col));

    /// <Summary>CharacterLength</Summary>
    public static Column CharacterLength(string col) => new(FunctionWrappedCall("character_length", false, col));

    /// <Summary>CharacterLength</Summary>
	public static Column CharacterLength(Column col) => new(FunctionWrappedCall("character_length", false, col));

    /// <Summary>
    /// Contains
    /// Returns a boolean. The value is True if right is found inside left. Returns NULL if either input expression is NULL. Otherwise, returns False. Both left or right must be of STRING or BINARY type.
    /// </Summary>
    public static Column Contains(string left, string right) => new(FunctionWrappedCall("contains", false, left, right));

    /// <Summary>
    /// Contains
    /// Returns a boolean. The value is True if right is found inside left. Returns NULL if either input expression is NULL. Otherwise, returns False. Both left or right must be of STRING or BINARY type.
    /// </Summary>    
	public static Column Contains(Column left, Column right) => new(FunctionWrappedCall("contains", false, left, right));


    /// <Summary>
    /// Elt
    /// Returns the `n`-th input, e.g., returns `input2` when `n` is 2. The function returns NULL if the index exceeds the length of the array and `spark.sql.ansi.enabled` is set to false. If `spark.sql.ansi.enabled` is set to true, it throws ArrayIndexOutOfBoundsException for invalid indices.
    /// </Summary>
    public static Column Elt(params string[] cols) => new(FunctionWrappedCall("elt", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Elt
    /// Returns the `n`-th input, e.g., returns `input2` when `n` is 2. The function returns NULL if the index exceeds the length of the array and `spark.sql.ansi.enabled` is set to false. If `spark.sql.ansi.enabled` is set to true, it throws ArrayIndexOutOfBoundsException for invalid indices.
    /// </Summary>
	public static Column Elt(params Column[] cols) => new(FunctionWrappedCall("elt", false, cols));


    /// <Summary>
    /// FindInSet
    /// Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`strArray`). Returns 0, if the string was not found or if the given string (`str`) contains a comma.
    /// </Summary>
    public static Column FindInSet(string str, string str_array) => new(FunctionWrappedCall("find_in_set", false, str, str_array));

    /// <Summary>
    /// FindInSet
    /// Returns the index (1-based) of the given string (`str`) in the comma-delimited list (`strArray`). Returns 0, if the string was not found or if the given string (`str`) contains a comma.
    /// </Summary>    
	public static Column FindInSet(Column str, Column str_array) => new(FunctionWrappedCall("find_in_set", false, str, str_array));


    /// <Summary>Lcase</Summary>
    public static Column Lcase(string col) => new(FunctionWrappedCall("lcase", false, col));

    /// <Summary>Lcase</Summary>
	public static Column Lcase(Column col) => new(FunctionWrappedCall("lcase", false, col));

    /// <Summary>Ucase</Summary>
    public static Column Ucase(string col) => new(FunctionWrappedCall("ucase", false, col));

    /// <Summary>Ucase</Summary>
	public static Column Ucase(Column col) => new(FunctionWrappedCall("ucase", false, col));

    /// <Summary>
    /// Left
    /// Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>
    public static Column Left(string str, string len) => new(FunctionWrappedCall("left", false, str, len));

    /// <Summary>
    /// Left
    /// Returns the leftmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>    
	public static Column Left(Column str, Column len) => new(FunctionWrappedCall("left", false, str, len));


    /// <Summary>
    /// Right
    /// Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>
    public static Column Right(string str, string len) => new(FunctionWrappedCall("right", false, str, len));

    /// <Summary>
    /// Right
    /// Returns the rightmost `len`(`len` can be string type) characters from the string `str`, if `len` is less or equal than 0 the result is an empty string.
    /// </Summary>    
	public static Column Right(Column str, Column len) => new(FunctionWrappedCall("right", false, str, len));


    /// <Summary>
    /// MapFromArrays
    /// Creates a new map from two arrays.
    /// </Summary>
    public static Column MapFromArrays(string col1, string col2) => new(FunctionWrappedCall("map_from_arrays", false, col1, col2));

    /// <Summary>
    /// MapFromArrays
    /// Creates a new map from two arrays.
    /// </Summary>    
	public static Column MapFromArrays(Column col1, Column col2) => new(FunctionWrappedCall("map_from_arrays", false, col1, col2));


    /// <Summary>
    /// Array
    /// 
    /// </Summary>
    public static Column Array(params string[] cols) => new(FunctionWrappedCall("array", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Array
    /// 
    /// </Summary>
	public static Column Array(params Column[] cols) => new(FunctionWrappedCall("array", false, cols));

	/// <Summary>Array</Summary>
	public static Column Array() => new(FunctionWrappedCall("array", false));
    


    /// <Summary>
    /// ArrayContains
    /// Collection function: returns null if the array is null, true if the array contains the given value, and false otherwise.
    /// </Summary>
    public static Column ArrayContains(string col , Column value) => new(FunctionWrappedCall("array_contains", false, Col(col), value));

    /// <Summary>
    /// ArrayContains
    /// Collection function: returns null if the array is null, true if the array contains the given value, and false otherwise.
    /// </Summary>
    public static Column ArrayContains(Column col, Column value) => new(FunctionWrappedCall("array_contains", false, col, value));

    /// <Summary>
    /// ArraysOverlap
    /// Collection function: returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise.
    /// </Summary>
    public static Column ArraysOverlap(string a1, string a2) => new(FunctionWrappedCall("arrays_overlap", false, a1, a2));

    /// <Summary>
    /// ArraysOverlap
    /// Collection function: returns true if the arrays contain any common non-null element; if not, returns null if both the arrays are non-empty and any of them contains a null element; returns false otherwise.
    /// </Summary>    
	public static Column ArraysOverlap(Column a1, Column a2) => new(FunctionWrappedCall("arrays_overlap", false, a1, a2));


    /// <Summary>
    /// Concat
    /// Concatenates multiple input columns together into a single column. The function works with strings, numeric, binary and compatible array columns.
    /// </Summary>
    public static Column Concat(params string[] cols) => new(FunctionWrappedCall("concat", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Concat
    /// Concatenates multiple input columns together into a single column. The function works with strings, numeric, binary and compatible array columns.
    /// </Summary>
	public static Column Concat(params Column[] cols) => new(FunctionWrappedCall("concat", false, cols));


    /// <Summary>
    /// ArrayPosition
    /// Collection function: Locates the position of the first occurrence of the given value in the given array. Returns null if either of the arguments are null.
    /// </Summary>
    public static Column ArrayPosition(string col , Column value) => new(FunctionWrappedCall("array_position", false, Col(col), value));

    /// <Summary>
    /// ArrayPosition
    /// Collection function: Locates the position of the first occurrence of the given value in the given array. Returns null if either of the arguments are null.
    /// </Summary>
    public static Column ArrayPosition(Column col, Column value) => new(FunctionWrappedCall("array_position", false, col, value));

    /// <Summary>
    /// ElementAt
    /// Collection function: Returns element of array at given index in `extraction` if col is array. Returns value for the given key in `extraction` if col is map. If position is negative then location of the element will start from end, if number is outside the array boundaries then None will be returned.
    /// </Summary>
    public static Column ElementAt(string col , Column extraction) => new(FunctionWrappedCall("element_at", false, Col(col), extraction));

    /// <Summary>
    /// ElementAt
    /// Collection function: Returns element of array at given index in `extraction` if col is array. Returns value for the given key in `extraction` if col is map. If position is negative then location of the element will start from end, if number is outside the array boundaries then None will be returned.
    /// </Summary>
    public static Column ElementAt(Column col, Column extraction) => new(FunctionWrappedCall("element_at", false, col, extraction));

    /// <Summary>
    /// ArrayPrepend
    /// Collection function: Returns an array containing element as well as all elements from array. The new element is positioned at the beginning of the array.
    /// </Summary>
    public static Column ArrayPrepend(string col , Column value) => new(FunctionWrappedCall("array_prepend", false, Col(col), value));

    /// <Summary>
    /// ArrayPrepend
    /// Collection function: Returns an array containing element as well as all elements from array. The new element is positioned at the beginning of the array.
    /// </Summary>
    public static Column ArrayPrepend(Column col, Column value) => new(FunctionWrappedCall("array_prepend", false, col, value));

    /// <Summary>
    /// ArrayRemove
    /// Collection function: Remove all elements that equal to element from the given array.
    /// </Summary>
    public static Column ArrayRemove(string col , Column element) => new(FunctionWrappedCall("array_remove", false, Col(col), element));

    /// <Summary>
    /// ArrayRemove
    /// Collection function: Remove all elements that equal to element from the given array.
    /// </Summary>
    public static Column ArrayRemove(Column col, Column element) => new(FunctionWrappedCall("array_remove", false, col, element));

    /// <Summary>ArrayDistinct</Summary>
    public static Column ArrayDistinct(string col) => new(FunctionWrappedCall("array_distinct", false, col));

    /// <Summary>ArrayDistinct</Summary>
	public static Column ArrayDistinct(Column col) => new(FunctionWrappedCall("array_distinct", false, col));

    /// <Summary>
    /// ArrayIntersect
    /// Collection function: returns an array of the elements in the intersection of col1 and col2, without duplicates.
    /// </Summary>
    public static Column ArrayIntersect(string col1, string col2) => new(FunctionWrappedCall("array_intersect", false, col1, col2));

    /// <Summary>
    /// ArrayIntersect
    /// Collection function: returns an array of the elements in the intersection of col1 and col2, without duplicates.
    /// </Summary>    
	public static Column ArrayIntersect(Column col1, Column col2) => new(FunctionWrappedCall("array_intersect", false, col1, col2));


    /// <Summary>
    /// ArrayUnion
    /// Collection function: returns an array of the elements in the union of col1 and col2, without duplicates.
    /// </Summary>
    public static Column ArrayUnion(string col1, string col2) => new(FunctionWrappedCall("array_union", false, col1, col2));

    /// <Summary>
    /// ArrayUnion
    /// Collection function: returns an array of the elements in the union of col1 and col2, without duplicates.
    /// </Summary>    
	public static Column ArrayUnion(Column col1, Column col2) => new(FunctionWrappedCall("array_union", false, col1, col2));


    /// <Summary>
    /// ArrayExcept
    /// Collection function: returns an array of the elements in col1 but not in col2, without duplicates.
    /// </Summary>
    public static Column ArrayExcept(string col1, string col2) => new(FunctionWrappedCall("array_except", false, col1, col2));

    /// <Summary>
    /// ArrayExcept
    /// Collection function: returns an array of the elements in col1 but not in col2, without duplicates.
    /// </Summary>    
	public static Column ArrayExcept(Column col1, Column col2) => new(FunctionWrappedCall("array_except", false, col1, col2));


    /// <Summary>ArrayCompact</Summary>
    public static Column ArrayCompact(string col) => new(FunctionWrappedCall("array_compact", false, col));

    /// <Summary>ArrayCompact</Summary>
	public static Column ArrayCompact(Column col) => new(FunctionWrappedCall("array_compact", false, col));

    /// <Summary>
    /// ArrayAppend
    /// Collection function: returns an array of the elements in col1 along with the added element in col2 at the last of the array.
    /// </Summary>
    public static Column ArrayAppend(string col , Column value) => new(FunctionWrappedCall("array_append", false, Col(col), value));

    /// <Summary>
    /// ArrayAppend
    /// Collection function: returns an array of the elements in col1 along with the added element in col2 at the last of the array.
    /// </Summary>
    public static Column ArrayAppend(Column col, Column value) => new(FunctionWrappedCall("array_append", false, col, value));

    /// <Summary>Explode</Summary>
    public static Column Explode(string col) => new(FunctionWrappedCall("explode", false, col));

    /// <Summary>Explode</Summary>
	public static Column Explode(Column col) => new(FunctionWrappedCall("explode", false, col));

    /// <Summary>Posexplode</Summary>
    public static Column Posexplode(string col) => new(FunctionWrappedCall("posexplode", false, col));

    /// <Summary>Posexplode</Summary>
	public static Column Posexplode(Column col) => new(FunctionWrappedCall("posexplode", false, col));

    /// <Summary>Inline</Summary>
    public static Column Inline(string col) => new(FunctionWrappedCall("inline", false, col));

    /// <Summary>Inline</Summary>
	public static Column Inline(Column col) => new(FunctionWrappedCall("inline", false, col));

    /// <Summary>ExplodeOuter</Summary>
    public static Column ExplodeOuter(string col) => new(FunctionWrappedCall("explode_outer", false, col));

    /// <Summary>ExplodeOuter</Summary>
	public static Column ExplodeOuter(Column col) => new(FunctionWrappedCall("explode_outer", false, col));

    /// <Summary>PosexplodeOuter</Summary>
    public static Column PosexplodeOuter(string col) => new(FunctionWrappedCall("posexplode_outer", false, col));

    /// <Summary>PosexplodeOuter</Summary>
	public static Column PosexplodeOuter(Column col) => new(FunctionWrappedCall("posexplode_outer", false, col));

    /// <Summary>InlineOuter</Summary>
    public static Column InlineOuter(string col) => new(FunctionWrappedCall("inline_outer", false, col));

    /// <Summary>InlineOuter</Summary>
	public static Column InlineOuter(Column col) => new(FunctionWrappedCall("inline_outer", false, col));

    /// <Summary>JsonArrayLength</Summary>
    public static Column JsonArrayLength(string col) => new(FunctionWrappedCall("json_array_length", false, col));

    /// <Summary>JsonArrayLength</Summary>
	public static Column JsonArrayLength(Column col) => new(FunctionWrappedCall("json_array_length", false, col));

    /// <Summary>JsonObjectKeys</Summary>
    public static Column JsonObjectKeys(string col) => new(FunctionWrappedCall("json_object_keys", false, col));

    /// <Summary>JsonObjectKeys</Summary>
	public static Column JsonObjectKeys(Column col) => new(FunctionWrappedCall("json_object_keys", false, col));

    /// <Summary>Size</Summary>
    public static Column Size(string col) => new(FunctionWrappedCall("size", false, col));

    /// <Summary>Size</Summary>
	public static Column Size(Column col) => new(FunctionWrappedCall("size", false, col));

    /// <Summary>ArrayMin</Summary>
    public static Column ArrayMin(string col) => new(FunctionWrappedCall("array_min", false, col));

    /// <Summary>ArrayMin</Summary>
	public static Column ArrayMin(Column col) => new(FunctionWrappedCall("array_min", false, col));

    /// <Summary>ArrayMax</Summary>
    public static Column ArrayMax(string col) => new(FunctionWrappedCall("array_max", false, col));

    /// <Summary>ArrayMax</Summary>
	public static Column ArrayMax(Column col) => new(FunctionWrappedCall("array_max", false, col));

    /// <Summary>ArraySize</Summary>
    public static Column ArraySize(string col) => new(FunctionWrappedCall("array_size", false, col));

    /// <Summary>ArraySize</Summary>
	public static Column ArraySize(Column col) => new(FunctionWrappedCall("array_size", false, col));

    /// <Summary>Cardinality</Summary>
    public static Column Cardinality(string col) => new(FunctionWrappedCall("cardinality", false, col));

    /// <Summary>Cardinality</Summary>
	public static Column Cardinality(Column col) => new(FunctionWrappedCall("cardinality", false, col));

    /// <Summary>Shuffle</Summary>
    public static Column Shuffle(string col) => new(FunctionWrappedCall("shuffle", false, col));

    /// <Summary>Shuffle</Summary>
	public static Column Shuffle(Column col) => new(FunctionWrappedCall("shuffle", false, col));

    /// <Summary>Reverse</Summary>
    public static Column Reverse(string col) => new(FunctionWrappedCall("reverse", false, col));

    /// <Summary>Reverse</Summary>
	public static Column Reverse(Column col) => new(FunctionWrappedCall("reverse", false, col));

    /// <Summary>Flatten</Summary>
    public static Column Flatten(string col) => new(FunctionWrappedCall("flatten", false, col));

    /// <Summary>Flatten</Summary>
	public static Column Flatten(Column col) => new(FunctionWrappedCall("flatten", false, col));

    /// <Summary>
    /// MapContainsKey
    /// Returns true if the map contains the key.
    /// </Summary>
    public static Column MapContainsKey(string col , Column value) => new(FunctionWrappedCall("map_contains_key", false, Col(col), value));

    /// <Summary>
    /// MapContainsKey
    /// Returns true if the map contains the key.
    /// </Summary>
    public static Column MapContainsKey(Column col, Column value) => new(FunctionWrappedCall("map_contains_key", false, col, value));

    /// <Summary>MapKeys</Summary>
    public static Column MapKeys(string col) => new(FunctionWrappedCall("map_keys", false, col));

    /// <Summary>MapKeys</Summary>
	public static Column MapKeys(Column col) => new(FunctionWrappedCall("map_keys", false, col));

    /// <Summary>MapValues</Summary>
    public static Column MapValues(string col) => new(FunctionWrappedCall("map_values", false, col));

    /// <Summary>MapValues</Summary>
	public static Column MapValues(Column col) => new(FunctionWrappedCall("map_values", false, col));

    /// <Summary>MapEntries</Summary>
    public static Column MapEntries(string col) => new(FunctionWrappedCall("map_entries", false, col));

    /// <Summary>MapEntries</Summary>
	public static Column MapEntries(Column col) => new(FunctionWrappedCall("map_entries", false, col));

    /// <Summary>MapFromEntries</Summary>
    public static Column MapFromEntries(string col) => new(FunctionWrappedCall("map_from_entries", false, col));

    /// <Summary>MapFromEntries</Summary>
	public static Column MapFromEntries(Column col) => new(FunctionWrappedCall("map_from_entries", false, col));

    /// <Summary>
    /// ArraysZip
    /// Collection function: Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays. If one of the arrays is shorter than others then resulting struct type value will be a `null` for missing elements.
    /// </Summary>
    public static Column ArraysZip(params string[] cols) => new(FunctionWrappedCall("arrays_zip", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// ArraysZip
    /// Collection function: Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays. If one of the arrays is shorter than others then resulting struct type value will be a `null` for missing elements.
    /// </Summary>
	public static Column ArraysZip(params Column[] cols) => new(FunctionWrappedCall("arrays_zip", false, cols));


    /// <Summary>
    /// MapConcat
    /// 
    /// </Summary>
    public static Column MapConcat(params string[] cols) => new(FunctionWrappedCall("map_concat", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// MapConcat
    /// 
    /// </Summary>
	public static Column MapConcat(params Column[] cols) => new(FunctionWrappedCall("map_concat", false, cols));

	/// <Summary>MapConcat</Summary>
	public static Column MapConcat() => new(FunctionWrappedCall("map_concat", false));
    


    /// <Summary>Years, NOTE: This is untested</Summary>
    public static Column Years(string col) => new(FunctionWrappedCall("years", false, col));

    /// <Summary>Years, NOTE: This is untested</Summary>
	public static Column Years(Column col) => new(FunctionWrappedCall("years", false, col));

    /// <Summary>Months, NOTE: This is untested</Summary>
    public static Column Months(string col) => new(FunctionWrappedCall("months", false, col));

    /// <Summary>Months, NOTE: This is untested</Summary>
	public static Column Months(Column col) => new(FunctionWrappedCall("months", false, col));

    /// <Summary>Days, NOTE: This is untested</Summary>
    public static Column Days(string col) => new(FunctionWrappedCall("days", false, col));

    /// <Summary>Days, NOTE: This is untested</Summary>
	public static Column Days(Column col) => new(FunctionWrappedCall("days", false, col));

    /// <Summary>Hours, NOTE: This is untested</Summary>
    public static Column Hours(string col) => new(FunctionWrappedCall("hours", false, col));

    /// <Summary>Hours, NOTE: This is untested</Summary>
	public static Column Hours(Column col) => new(FunctionWrappedCall("hours", false, col));

    /// <Summary>
    /// MakeTimestampNtz
    /// Create local date-time from years, months, days, hours, mins, secs fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.
    /// </Summary>
    public static Column MakeTimestampNtz(string years, string months, string days, string hours, string mins, string secs) => new(FunctionWrappedCall("make_timestamp_ntz", false, years, months, days, hours, mins, secs));

    /// <Summary>
    /// MakeTimestampNtz
    /// Create local date-time from years, months, days, hours, mins, secs fields. If the configuration `spark.sql.ansi.enabled` is false, the function returns NULL on invalid inputs. Otherwise, it will throw an error instead.
    /// </Summary>    
	public static Column MakeTimestampNtz(Column years, Column months, Column days, Column hours, Column mins, Column secs) => new(FunctionWrappedCall("make_timestamp_ntz", false, years, months, days, hours, mins, secs));


    /// <Summary>
    /// Ifnull
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>
    public static Column Ifnull(string col1, string col2) => new(FunctionWrappedCall("ifnull", false, col1, col2));

    /// <Summary>
    /// Ifnull
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>    
	public static Column Ifnull(Column col1, Column col2) => new(FunctionWrappedCall("ifnull", false, col1, col2));


    /// <Summary>Isnotnull</Summary>
    public static Column Isnotnull(string col) => new(FunctionWrappedCall("isnotnull", false, col));

    /// <Summary>Isnotnull</Summary>
	public static Column Isnotnull(Column col) => new(FunctionWrappedCall("isnotnull", false, col));

    /// <Summary>
    /// EqualNull
    /// Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.
    /// </Summary>
    public static Column EqualNull(string col1, string col2) => new(FunctionWrappedCall("equal_null", false, col1, col2));

    /// <Summary>
    /// EqualNull
    /// Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null.
    /// </Summary>    
	public static Column EqualNull(Column col1, Column col2) => new(FunctionWrappedCall("equal_null", false, col1, col2));


    /// <Summary>
    /// Nullif
    /// Returns null if `col1` equals to `col2`, or `col1` otherwise.
    /// </Summary>
    public static Column Nullif(string col1, string col2) => new(FunctionWrappedCall("nullif", false, col1, col2));

    /// <Summary>
    /// Nullif
    /// Returns null if `col1` equals to `col2`, or `col1` otherwise.
    /// </Summary>    
	public static Column Nullif(Column col1, Column col2) => new(FunctionWrappedCall("nullif", false, col1, col2));


    /// <Summary>
    /// Nvl
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>
    public static Column Nvl(string col1, string col2) => new(FunctionWrappedCall("nvl", false, col1, col2));

    /// <Summary>
    /// Nvl
    /// Returns `col2` if `col1` is null, or `col1` otherwise.
    /// </Summary>    
	public static Column Nvl(Column col1, Column col2) => new(FunctionWrappedCall("nvl", false, col1, col2));


    /// <Summary>
    /// Nvl2
    /// Returns `col2` if `col1` is not null, or `col3` otherwise.
    /// </Summary>
    public static Column Nvl2(string col1, string col2, string col3) => new(FunctionWrappedCall("nvl2", false, col1, col2, col3));

    /// <Summary>
    /// Nvl2
    /// Returns `col2` if `col1` is not null, or `col3` otherwise.
    /// </Summary>    
	public static Column Nvl2(Column col1, Column col2, Column col3) => new(FunctionWrappedCall("nvl2", false, col1, col2, col3));


    /// <Summary>Sha</Summary>
    public static Column Sha(string col) => new(FunctionWrappedCall("sha", false, col));

    /// <Summary>Sha</Summary>
	public static Column Sha(Column col) => new(FunctionWrappedCall("sha", false, col));
	/// <Summary>InputFileBlockLength</Summary>
	public static Column InputFileBlockLength() => new(FunctionWrappedCall("input_file_block_length", false));
    

	/// <Summary>InputFileBlockStart</Summary>
	public static Column InputFileBlockStart() => new(FunctionWrappedCall("input_file_block_start", false));
    

	/// <Summary>Version</Summary>
	public static Column Version() => new(FunctionWrappedCall("version", false));
    


    /// <Summary>Typeof</Summary>
    public static Column Typeof(string col) => new(FunctionWrappedCall("typeof", false, col));

    /// <Summary>Typeof</Summary>
	public static Column Typeof(Column col) => new(FunctionWrappedCall("typeof", false, col));

    /// <Summary>
    /// Stack
    /// Separates `col1`, ..., `colk` into `n` rows. Uses column names col0, col1, etc. by default unless specified otherwise.
    /// </Summary>
    public static Column Stack(params string[] cols) => new(FunctionWrappedCall("stack", false, cols.ToList().Select(Col).ToArray()));

    /// <Summary>
    /// Stack
    /// Separates `col1`, ..., `colk` into `n` rows. Uses column names col0, col1, etc. by default unless specified otherwise.
    /// </Summary>
	public static Column Stack(params Column[] cols) => new(FunctionWrappedCall("stack", false, cols));


    /// <Summary>BitmapBitPosition</Summary>
    public static Column BitmapBitPosition(string col) => new(FunctionWrappedCall("bitmap_bit_position", false, col));

    /// <Summary>BitmapBitPosition</Summary>
	public static Column BitmapBitPosition(Column col) => new(FunctionWrappedCall("bitmap_bit_position", false, col));

    /// <Summary>BitmapBucketNumber</Summary>
    public static Column BitmapBucketNumber(string col) => new(FunctionWrappedCall("bitmap_bucket_number", false, col));

    /// <Summary>BitmapBucketNumber</Summary>
	public static Column BitmapBucketNumber(Column col) => new(FunctionWrappedCall("bitmap_bucket_number", false, col));

    /// <Summary>BitmapConstructAgg</Summary>
    public static Column BitmapConstructAgg(string col) => new(FunctionWrappedCall("bitmap_construct_agg", false, col));

    /// <Summary>BitmapConstructAgg</Summary>
	public static Column BitmapConstructAgg(Column col) => new(FunctionWrappedCall("bitmap_construct_agg", false, col));

    /// <Summary>BitmapCount</Summary>
    public static Column BitmapCount(string col) => new(FunctionWrappedCall("bitmap_count", false, col));

    /// <Summary>BitmapCount</Summary>
	public static Column BitmapCount(Column col) => new(FunctionWrappedCall("bitmap_count", false, col));

    /// <Summary>BitmapOrAgg</Summary>
    public static Column BitmapOrAgg(string col) => new(FunctionWrappedCall("bitmap_or_agg", false, col));

    /// <Summary>BitmapOrAgg</Summary>
	public static Column BitmapOrAgg(Column col) => new(FunctionWrappedCall("bitmap_or_agg", false, col));

}

