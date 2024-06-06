using Spark.Connect.Dotnet.Sql;
using Xunit.Abstractions;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.FunctionsTests;

public class GeneratedFunctionsTests : E2ETestBase
{
    
    public GeneratedFunctionsTests(ITestOutputHelper logger) : base(logger)
    {
        Source = Spark.Sql(
            "SELECT array(id, id + 1, id + 2) as idarray, array(array(id, id + 1, id + 2), array(id, id + 1, id + 2)) as idarrayarray, cast(cast(id as string) as binary) as idbinary, cast(id as boolean) as idboolean, cast(id as int) as idint, id, id as id0, id as id1, id as id2, id as id3, id as id4, current_date() as dt, current_timestamp() as ts, 'hello' as str, 'SGVsbG8gRnJpZW5kcw==' as b64, map('k', id) as m, array(struct(1, 'a'), struct(2, 'b')) as data, '[]' as jstr FROM range(100)");
    }

    private readonly Dotnet.Sql.DataFrame Source;

    private static WindowSpec Window =   Dotnet.Sql.Window.OrderBy("id").PartitionBy("id");
    private static WindowSpec OtherWindow = new WindowSpec().OrderBy("id").PartitionBy("id");
    
    /** GeneratedBy::SingleArgColumnOrNameFunction::Sort **/
    [Fact]
    public void Asc_Test()
    {
        Source.OrderBy(Asc("id")).Show();
        Source.OrderBy(Asc(Lit(1))).Show();
        Source.OrderBy(Asc(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Sort **/
    [Fact]
    public void Desc_Test()
    {
        Source.OrderBy(Desc("id")).Show();
        Source.OrderBy(Desc(Lit(1))).Show();
        Source.OrderBy(Desc(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Sqrt_Test()
    {
        Source.Select(Sqrt("id")).Show();
        Source.Select(Sqrt(Lit(180))).Show();
        Source.Select(Sqrt(Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void TryAdd_Test()
    {
        Source.Select(TryAdd("id", "id")).Show();
        Source.Select(TryAdd(Lit(1), Lit(1))).Show();
        Source.Select(TryAdd(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void TryAvg_Test()
    {
        Source.Select(TryAvg("id")).Show();
        Source.Select(TryAvg(Lit(180))).Show();
        Source.Select(TryAvg(Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void TryDivide_Test()
    {
        Source.Select(TryDivide("id", "id")).Show();
        Source.Select(TryDivide(Lit(1), Lit(1))).Show();
        Source.Select(TryDivide(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void TryMultiply_Test()
    {
        Source.Select(TryMultiply("id", "id")).Show();
        Source.Select(TryMultiply(Lit(1), Lit(1))).Show();
        Source.Select(TryMultiply(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void TrySubtract_Test()
    {
        Source.Select(TrySubtract("id", "id")).Show();
        Source.Select(TrySubtract(Lit(1), Lit(1))).Show();
        Source.Select(TrySubtract(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void TrySum_Test()
    {
        Source.Select(TrySum("id")).Show();
        Source.Select(TrySum(Lit(180))).Show();
        Source.Select(TrySum(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Abs_Test()
    {
        Source.Select(Abs("id")).Show();
        Source.Select(Abs(Lit(180))).Show();
        Source.Select(Abs(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Mode_Test()
    {
        Source.Select(Mode("id")).Show();
        Source.Select(Mode(Lit(180))).Show();
        Source.Select(Mode(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Max_Test()
    {
        Source.Select(Max("id")).Show();
        Source.Select(Max(Lit(180))).Show();
        Source.Select(Max(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Min_Test()
    {
        Source.Select(Min("id")).Show();
        Source.Select(Min(Lit(180))).Show();
        Source.Select(Min(Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void MaxBy_Test()
    {
        Source.Select(MaxBy("id", "id")).Show();
        Source.Select(MaxBy(Lit(1), Lit(1))).Show();
        Source.Select(MaxBy(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void MinBy_Test()
    {
        Source.Select(MinBy("id", "id")).Show();
        Source.Select(MinBy(Lit(1), Lit(1))).Show();
        Source.Select(MinBy(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Count_Test()
    {
        Source.Select(Count("id")).Show();
        Source.Select(Count(Lit(180))).Show();
        Source.Select(Count(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Sum_Test()
    {
        Source.Select(Sum("id")).Show();
        Source.Select(Sum(Lit(180))).Show();
        Source.Select(Sum(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Avg_Test()
    {
        Source.Select(Avg("id")).Show();
        Source.Select(Avg(Lit(180))).Show();
        Source.Select(Avg(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Mean_Test()
    {
        Source.Select(Mean("id")).Show();
        Source.Select(Mean(Lit(180))).Show();
        Source.Select(Mean(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Median_Test()
    {
        Source.Select(Median("id")).Show();
        Source.Select(Median(Lit(180))).Show();
        Source.Select(Median(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Product_Test()
    {
        Source.Select(Product("id")).Show();
        Source.Select(Product(Lit(180))).Show();
        Source.Select(Product(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Acos_Test()
    {
        Source.Select(Acos("id")).Show();
        Source.Select(Acos(Lit(180))).Show();
        Source.Select(Acos(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Acosh_Test()
    {
        Source.Select(Acosh("id")).Show();
        Source.Select(Acosh(Lit(180))).Show();
        Source.Select(Acosh(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Asin_Test()
    {
        Source.Select(Asin("id")).Show();
        Source.Select(Asin(Lit(180))).Show();
        Source.Select(Asin(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Asinh_Test()
    {
        Source.Select(Asinh("id")).Show();
        Source.Select(Asinh(Lit(180))).Show();
        Source.Select(Asinh(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Atan_Test()
    {
        Source.Select(Atan("id")).Show();
        Source.Select(Atan(Lit(180))).Show();
        Source.Select(Atan(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Atanh_Test()
    {
        Source.Select(Atanh("id")).Show();
        Source.Select(Atanh(Lit(180))).Show();
        Source.Select(Atanh(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Cbrt_Test()
    {
        Source.Select(Cbrt("id")).Show();
        Source.Select(Cbrt(Lit(180))).Show();
        Source.Select(Cbrt(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Ceil_Test()
    {
        Source.Select(Ceil("id")).Show();
        Source.Select(Ceil(Lit(180))).Show();
        Source.Select(Ceil(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Ceiling_Test()
    {
        Source.Select(Ceiling("id")).Show();
        Source.Select(Ceiling(Lit(180))).Show();
        Source.Select(Ceiling(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Cos_Test()
    {
        Source.Select(Cos("id")).Show();
        Source.Select(Cos(Lit(180))).Show();
        Source.Select(Cos(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Cosh_Test()
    {
        Source.Select(Cosh("id")).Show();
        Source.Select(Cosh(Lit(180))).Show();
        Source.Select(Cosh(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Cot_Test()
    {
        Source.Select(Cot("id")).Show();
        Source.Select(Cot(Lit(180))).Show();
        Source.Select(Cot(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Csc_Test()
    {
        Source.Select(Csc("id")).Show();
        Source.Select(Csc(Lit(180))).Show();
        Source.Select(Csc(Col("id"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void E_Test()
    {
        Source.Select(E()).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Exp_Test()
    {
        Source.Select(Exp("id")).Show();
        Source.Select(Exp(Lit(180))).Show();
        Source.Select(Exp(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Expm1_Test()
    {
        Source.Select(Expm1("id")).Show();
        Source.Select(Expm1(Lit(180))).Show();
        Source.Select(Expm1(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Floor_Test()
    {
        Source.Select(Floor("id")).Show();
        Source.Select(Floor(Lit(180))).Show();
        Source.Select(Floor(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Log_Test()
    {
        Source.Select(Log("id")).Show();
        Source.Select(Log(Lit(180))).Show();
        Source.Select(Log(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Log10_Test()
    {
        Source.Select(Log10("id")).Show();
        Source.Select(Log10(Lit(180))).Show();
        Source.Select(Log10(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Log1p_Test()
    {
        Source.Select(Log1p("id")).Show();
        Source.Select(Log1p(Lit(180))).Show();
        Source.Select(Log1p(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Negative_Test()
    {
        Source.Select(Negative("id")).Show();
        Source.Select(Negative(Lit(180))).Show();
        Source.Select(Negative(Col("id"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void Pi_Test()
    {
        Source.Select(Pi()).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Positive_Test()
    {
        Source.Select(Positive("id")).Show();
        Source.Select(Positive(Lit(180))).Show();
        Source.Select(Positive(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Rint_Test()
    {
        Source.Select(Rint("id")).Show();
        Source.Select(Rint(Lit(180))).Show();
        Source.Select(Rint(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Sec_Test()
    {
        Source.Select(Sec("id")).Show();
        Source.Select(Sec(Lit(180))).Show();
        Source.Select(Sec(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Signum_Test()
    {
        Source.Select(Signum("id")).Show();
        Source.Select(Signum(Lit(180))).Show();
        Source.Select(Signum(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Sign_Test()
    {
        Source.Select(Sign("id")).Show();
        Source.Select(Sign(Lit(180))).Show();
        Source.Select(Sign(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Sin_Test()
    {
        Source.Select(Sin("id")).Show();
        Source.Select(Sin(Lit(180))).Show();
        Source.Select(Sin(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Sinh_Test()
    {
        Source.Select(Sinh("id")).Show();
        Source.Select(Sinh(Lit(180))).Show();
        Source.Select(Sinh(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Tan_Test()
    {
        Source.Select(Tan("id")).Show();
        Source.Select(Tan(Lit(180))).Show();
        Source.Select(Tan(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Tanh_Test()
    {
        Source.Select(Tanh("id")).Show();
        Source.Select(Tanh(Lit(180))).Show();
        Source.Select(Tanh(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitwiseNot_Test()
    {
        Source.Select(BitwiseNot("id")).Show();
        Source.Select(BitwiseNot(Lit(180))).Show();
        Source.Select(BitwiseNot(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitCount_Test()
    {
        Source.Select(BitCount("id")).Show();
        Source.Select(BitCount(Lit(180))).Show();
        Source.Select(BitCount(Col("id"))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void BitGet_Test()
    {
        Source.Select(BitGet("id", Lit(0))).Show();
        Source.Select(BitGet(Lit(10), Lit(0))).Show();
        Source.Select(BitGet(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void Getbit_Test()
    {
        Source.Select(Getbit("id", Lit(0))).Show();
        Source.Select(Getbit(Lit(10), Lit(0))).Show();
        Source.Select(Getbit(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Sort **/
    [Fact]
    public void AscNullsFirst_Test()
    {
        Source.OrderBy(AscNullsFirst("id")).Show();
        Source.OrderBy(AscNullsFirst(Lit(1))).Show();
        Source.OrderBy(AscNullsFirst(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Sort **/
    [Fact]
    public void AscNullsLast_Test()
    {
        Source.OrderBy(AscNullsLast("id")).Show();
        Source.OrderBy(AscNullsLast(Lit(1))).Show();
        Source.OrderBy(AscNullsLast(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Sort **/
    [Fact]
    public void DescNullsFirst_Test()
    {
        Source.OrderBy(DescNullsFirst("id")).Show();
        Source.OrderBy(DescNullsFirst(Lit(1))).Show();
        Source.OrderBy(DescNullsFirst(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Sort **/
    [Fact]
    public void DescNullsLast_Test()
    {
        Source.OrderBy(DescNullsLast("id")).Show();
        Source.OrderBy(DescNullsLast(Lit(1))).Show();
        Source.OrderBy(DescNullsLast(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Stddev_Test()
    {
        Source.Select(Stddev("id")).Show();
        Source.Select(Stddev(Lit(180))).Show();
        Source.Select(Stddev(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Std_Test()
    {
        Source.Select(Std("id")).Show();
        Source.Select(Std(Lit(180))).Show();
        Source.Select(Std(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void StddevSamp_Test()
    {
        Source.Select(StddevSamp("id")).Show();
        Source.Select(StddevSamp(Lit(180))).Show();
        Source.Select(StddevSamp(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void StddevPop_Test()
    {
        Source.Select(StddevPop("id")).Show();
        Source.Select(StddevPop(Lit(180))).Show();
        Source.Select(StddevPop(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Variance_Test()
    {
        Source.Select(Variance("id")).Show();
        Source.Select(Variance(Lit(180))).Show();
        Source.Select(Variance(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void VarSamp_Test()
    {
        Source.Select(VarSamp("id")).Show();
        Source.Select(VarSamp(Lit(180))).Show();
        Source.Select(VarSamp(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void VarPop_Test()
    {
        Source.Select(VarPop("id")).Show();
        Source.Select(VarPop(Lit(180))).Show();
        Source.Select(VarPop(Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrAvgx_Test()
    {
        Source.Select(RegrAvgx("id", "id")).Show();
        Source.Select(RegrAvgx(Lit(1), Lit(1))).Show();
        Source.Select(RegrAvgx(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrAvgy_Test()
    {
        Source.Select(RegrAvgy("id", "id")).Show();
        Source.Select(RegrAvgy(Lit(1), Lit(1))).Show();
        Source.Select(RegrAvgy(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrCount_Test()
    {
        Source.Select(RegrCount("id", "id")).Show();
        Source.Select(RegrCount(Lit(1), Lit(1))).Show();
        Source.Select(RegrCount(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrIntercept_Test()
    {
        Source.Select(RegrIntercept("id", "id")).Show();
        Source.Select(RegrIntercept(Lit(1), Lit(1))).Show();
        Source.Select(RegrIntercept(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrR2_Test()
    {
        Source.Select(RegrR2("id", "id")).Show();
        Source.Select(RegrR2(Lit(1), Lit(1))).Show();
        Source.Select(RegrR2(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrSlope_Test()
    {
        Source.Select(RegrSlope("id", "id")).Show();
        Source.Select(RegrSlope(Lit(1), Lit(1))).Show();
        Source.Select(RegrSlope(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrSxx_Test()
    {
        Source.Select(RegrSxx("id", "id")).Show();
        Source.Select(RegrSxx(Lit(1), Lit(1))).Show();
        Source.Select(RegrSxx(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrSxy_Test()
    {
        Source.Select(RegrSxy("id", "id")).Show();
        Source.Select(RegrSxy(Lit(1), Lit(1))).Show();
        Source.Select(RegrSxy(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegrSyy_Test()
    {
        Source.Select(RegrSyy("id", "id")).Show();
        Source.Select(RegrSyy(Lit(1), Lit(1))).Show();
        Source.Select(RegrSyy(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Boolean **/
    [Fact]
    public void Every_Test()
    {
        Source.Select(Every("idboolean")).Show();
        Source.Select(Every(Lit(false))).Show();
        Source.Select(Every(Col("idboolean"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Boolean **/
    [Fact]
    public void BoolAnd_Test()
    {
        Source.Select(BoolAnd("idboolean")).Show();
        Source.Select(BoolAnd(Lit(false))).Show();
        Source.Select(BoolAnd(Col("idboolean"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Boolean **/
    [Fact]
    public void Some_Test()
    {
        Source.Select(Some("idboolean")).Show();
        Source.Select(Some(Lit(false))).Show();
        Source.Select(Some(Col("idboolean"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Boolean **/
    [Fact]
    public void BoolOr_Test()
    {
        Source.Select(BoolOr("idboolean")).Show();
        Source.Select(BoolOr(Lit(false))).Show();
        Source.Select(BoolOr(Col("idboolean"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitAnd_Test()
    {
        Source.Select(BitAnd("id")).Show();
        Source.Select(BitAnd(Lit(180))).Show();
        Source.Select(BitAnd(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitOr_Test()
    {
        Source.Select(BitOr("id")).Show();
        Source.Select(BitOr(Lit(180))).Show();
        Source.Select(BitOr(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitXor_Test()
    {
        Source.Select(BitXor("id")).Show();
        Source.Select(BitXor(Lit(180))).Show();
        Source.Select(BitXor(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Skewness_Test()
    {
        Source.Select(Skewness("id")).Show();
        Source.Select(Skewness(Lit(180))).Show();
        Source.Select(Skewness(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Kurtosis_Test()
    {
        Source.Select(Kurtosis("id")).Show();
        Source.Select(Kurtosis(Lit(180))).Show();
        Source.Select(Kurtosis(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void CollectList_Test()
    {
        Source.Select(CollectList("id")).Show();
        Source.Select(CollectList(Lit(180))).Show();
        Source.Select(CollectList(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void ArrayAgg_Test()
    {
        Source.Select(ArrayAgg("id")).Show();
        Source.Select(ArrayAgg(Lit(180))).Show();
        Source.Select(ArrayAgg(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void CollectSet_Test()
    {
        Source.Select(CollectSet("id")).Show();
        Source.Select(CollectSet(Lit(180))).Show();
        Source.Select(CollectSet(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Degrees_Test()
    {
        Source.Select(Degrees("id")).Show();
        Source.Select(Degrees(Lit(180))).Show();
        Source.Select(Degrees(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Radians_Test()
    {
        Source.Select(Radians("id")).Show();
        Source.Select(Radians(Lit(180))).Show();
        Source.Select(Radians(Col("id"))).Show();
    }

    /** GeneratedBy::TwoArgColumnOrNameOrAllSimpleFunction **/
    [Fact]
    public void Hypot_Test()
    {
        Source.Select(Hypot("id", "id")).Show();
        Source.Select(Hypot(Lit(1), Lit(2))).Show();
        Source.Select(Hypot(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::TwoArgColumnOrNameOrAllSimpleFunction **/
    [Fact]
    public void Pow_Test()
    {
        Source.Select(Pow("id", "id")).Show();
        Source.Select(Pow(Lit(1), Lit(2))).Show();
        Source.Select(Pow(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::TwoArgColumnOrNameOrAllSimpleFunction **/
    [Fact]
    public void Pmod_Test()
    {
        Spark.Conf.Set("spark.sql.ansi.enabled", "false");
        Source.Select(Pmod("id", "id")).Show();
        Source.Select(Pmod(Lit(1), Lit(2))).Show();
        Source.Select(Pmod(Col("id"), Col("id"))).Show();
        Spark.Conf.Set("spark.sql.ansi.enabled", "true");
        
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void RowNumber_Test()
    {
        Source.Select(RowNumber().Over(Window)).Show();
        Source.Select(RowNumber().Over(Window)).Collect();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void DenseRank_Test()
    {
        Source.Select(DenseRank().Over(Window)).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void Rank_Test()
    {
        Source.Select(Rank().Over(Window)).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void CumeDist_Test()
    {
        Source.Select(CumeDist().Over(Window)).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void PercentRank_Test()
    {
        Source.Select(PercentRank().Over(Window)).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Coalesce_Test()
    {
        Source.Select(Coalesce("id", "id")).Show();
        Source.Select(Coalesce(Lit(180), Lit(180))).Show();
        Source.Select(Coalesce(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Corr_Test()
    {
        Spark.Conf.Set("spark.sql.ansi.enabled", "false");
        Source.Select(Corr("id", "id")).Show();
        Source.Select(Corr(Lit(1), Lit(1))).Show();
        Source.Select(Corr(Col("id"), Col("id"))).Show();
        Spark.Conf.Set("spark.sql.ansi.enabled", "true");
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void CovarPop_Test()
    {
        Source.Select(CovarPop("id", "id")).Show();
        Source.Select(CovarPop(Lit(1), Lit(1))).Show();
        Source.Select(CovarPop(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void CovarSamp_Test()
    {
        Source.Select(CovarSamp("id", "id")).Show();
        Source.Select(CovarSamp(Lit(1), Lit(1))).Show();
        Source.Select(CovarSamp(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact(Skip = "Do Manual with Pivot")]
    public void GroupingId_Test()
    {
        Source.Select(GroupingId("id", "id")).Show();
        Source.Select(GroupingId(Lit(180), Lit(180))).Show();
        Source.Select(GroupingId(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void InputFileName_Test()
    {
        Source.Select(InputFileName()).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Isnan_Test()
    {
        Source.Select(Isnan("id")).Show();
        Source.Select(Isnan(Lit(180))).Show();
        Source.Select(Isnan(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Isnull_Test()
    {
        Source.Select(Isnull("id")).Show();
        Source.Select(Isnull(Lit(180))).Show();
        Source.Select(Isnull(Col("id"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void MonotonicallyIncreasingId_Test()
    {
        Source.Select(MonotonicallyIncreasingId()).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Nanvl_Test()
    {
        Source.Select(Nanvl("id", "id")).Show();
        Source.Select(Nanvl(Lit(1), Lit(1))).Show();
        Source.Select(Nanvl(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleOptionalArgBasicType::EverythingElse **/
    [Fact]
    public void Rand_Test()
    {
        Source.Select(Rand()).Show();
        Source.Select(Rand(1)).Show();
        Source.Select(Rand(Lit(180))).Show();
    }

    /** GeneratedBy::SingleOptionalArgBasicType::EverythingElse **/
    [Fact]
    public void Randn_Test()
    {
        Source.Select(Randn()).Show();
        Source.Select(Randn(1)).Show();
        Source.Select(Randn(Lit(180))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void Round_Test()
    {
        Source.Select(Round("id", Lit(0))).Show();
        Source.Select(Round(Lit(10), Lit(0))).Show();
        Source.Select(Round(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void Bround_Test()
    {
        Source.Select(Bround("id", Lit(0))).Show();
        Source.Select(Bround(Lit(10), Lit(0))).Show();
        Source.Select(Bround(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void ShiftLeft_Test()
    {
        Source.Select(ShiftLeft("id", Lit(0))).Show();
        Source.Select(ShiftLeft(Lit(10), Lit(0))).Show();
        Source.Select(ShiftLeft(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void Shiftleft_Test()
    {
        Source.Select(Shiftleft("id", Lit(0))).Show();
        Source.Select(Shiftleft(Lit(10), Lit(0))).Show();
        Source.Select(Shiftleft(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void ShiftRight_Test()
    {
        Source.Select(ShiftRight("id", Lit(0))).Show();
        Source.Select(ShiftRight(Lit(10), Lit(0))).Show();
        Source.Select(ShiftRight(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void Shiftright_Test()
    {
        Source.Select(Shiftright("id", Lit(0))).Show();
        Source.Select(Shiftright(Lit(10), Lit(0))).Show();
        Source.Select(Shiftright(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void ShiftRightUnsigned_Test()
    {
        Source.Select(ShiftRightUnsigned("id", Lit(0))).Show();
        Source.Select(ShiftRightUnsigned(Lit(10), Lit(0))).Show();
        Source.Select(ShiftRightUnsigned(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void Shiftrightunsigned_Test()
    {
        Source.Select(Shiftrightunsigned("id", Lit(0))).Show();
        Source.Select(Shiftrightunsigned(Lit(10), Lit(0))).Show();
        Source.Select(Shiftrightunsigned(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void SparkPartitionId_Test()
    {
        Source.Select(SparkPartitionId()).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Greatest_Test()
    {
        Source.Select(Greatest("id", "id")).Show();
        Source.Select(Greatest(Lit(180), Lit(180))).Show();
        Source.Select(Greatest(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Least_Test()
    {
        Source.Select(Least("id", "id")).Show();
        Source.Select(Least(Lit(180), Lit(180))).Show();
        Source.Select(Least(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Ln_Test()
    {
        Source.Select(Ln("id")).Show();
        Source.Select(Ln(Lit(180))).Show();
        Source.Select(Ln(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Log2_Test()
    {
        Source.Select(Log2("id")).Show();
        Source.Select(Log2(Lit(180))).Show();
        Source.Select(Log2(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Factorial_Test()
    {
        Source.Select(Factorial("id")).Show();
        Source.Select(Factorial(Lit(180))).Show();
        Source.Select(Factorial(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Boolean **/
    [Fact]
    public void CountIf_Test()
    {
        Source.Select(CountIf("idboolean")).Show();
        Source.Select(CountIf(Lit(false))).Show();
        Source.Select(CountIf(Col("idboolean"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void Curdate_Test()
    {
        Source.Select(Curdate()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void CurrentDate_Test()
    {
        Source.Select(CurrentDate()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void CurrentTimezone_Test()
    {
        Source.Select(CurrentTimezone()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void CurrentTimestamp_Test()
    {
        Source.Select(CurrentTimestamp()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void Now_Test()
    {
        Source.Select(Now()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void Localtimestamp_Test()
    {
        Source.Select(Localtimestamp()).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Year_Test()
    {
        Source.Select(Year("dt")).Show();
        Source.Select(Year(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Year(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Quarter_Test()
    {
        Source.Select(Quarter("dt")).Show();
        Source.Select(Quarter(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Quarter(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Month_Test()
    {
        Source.Select(Month("dt")).Show();
        Source.Select(Month(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Month(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Dayofweek_Test()
    {
        Source.Select(Dayofweek("dt")).Show();
        Source.Select(Dayofweek(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Dayofweek(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Dayofmonth_Test()
    {
        Source.Select(Dayofmonth("dt")).Show();
        Source.Select(Dayofmonth(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Dayofmonth(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Day_Test()
    {
        Source.Select(Day("dt")).Show();
        Source.Select(Day(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Day(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Dayofyear_Test()
    {
        Source.Select(Dayofyear("dt")).Show();
        Source.Select(Dayofyear(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Dayofyear(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Timestamp **/
    [Fact]
    public void Hour_Test()
    {
        Source.Select(Hour("ts")).Show();
        Source.Select(Hour(Lit(DateTime.Parse("1980-04-01 01:23:21")))).Show();
        Source.Select(Hour(Col("ts"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Timestamp **/
    [Fact]
    public void Minute_Test()
    {
        Source.Select(Minute("ts")).Show();
        Source.Select(Minute(Lit(DateTime.Parse("1980-04-01 01:23:21")))).Show();
        Source.Select(Minute(Col("ts"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Timestamp **/
    [Fact]
    public void Second_Test()
    {
        Source.Select(Second("ts")).Show();
        Source.Select(Second(Lit(DateTime.Parse("1980-04-01 01:23:21")))).Show();
        Source.Select(Second(Col("ts"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Weekofyear_Test()
    {
        Source.Select(Weekofyear("dt")).Show();
        Source.Select(Weekofyear(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Weekofyear(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void Weekday_Test()
    {
        Source.Select(Weekday("dt")).Show();
        Source.Select(Weekday(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Weekday(Col("dt"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void MakeDate_Test()
    {
        Spark.Conf.Set("spark.sql.ansi.enabled", "false");
        Source.Select(MakeDate("id", "id", "id")).Show();
        Source.Select(MakeDate(Lit(1), Lit(1), Lit(1))).Show();
        Source.Select(MakeDate(Col("id"), Col("id"), Col("id"))).Show();
        Spark.Conf.Set("spark.sql.ansi.enabled", "true");
    }

    /** GeneratedBy::AllArgsColumnOrName::Date **/
    [Fact]
    public void Datediff_Test()
    {
        Source.Select(Datediff("dt", "dt")).Show();
        Source.Select(Datediff(Lit(new DateOnly(1980, 04, 01)), Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(Datediff(Col("dt"), Col("dt"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrName::Date **/
    [Fact]
    public void DateDiff_Test()
    {
        Source.Select(DateDiff("dt", "dt")).Show();
        Source.Select(DateDiff(Lit(new DateOnly(1980, 04, 01)), Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(DateDiff(Col("dt"), Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void DateFromUnixDate_Test()
    {
        Source.Select(DateFromUnixDate("id")).Show();
        Source.Select(DateFromUnixDate(Lit(180))).Show();
        Source.Select(DateFromUnixDate(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void UnixDate_Test()
    {
        Source.Select(UnixDate("dt")).Show();
        Source.Select(UnixDate(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(UnixDate(Col("dt"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Timestamp **/
    [Fact]
    public void UnixMicros_Test()
    {
        Source.Select(UnixMicros("ts")).Show();
        Source.Select(UnixMicros(Lit(DateTime.Parse("1980-04-01 01:23:21")))).Show();
        Source.Select(UnixMicros(Col("ts"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Timestamp **/
    [Fact]
    public void UnixMillis_Test()
    {
        Source.Select(UnixMillis("ts")).Show();
        Source.Select(UnixMillis(Lit(DateTime.Parse("1980-04-01 01:23:21")))).Show();
        Source.Select(UnixMillis(Col("ts"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Timestamp **/
    [Fact]
    public void UnixSeconds_Test()
    {
        Source.Select(UnixSeconds("ts")).Show();
        Source.Select(UnixSeconds(Lit(DateTime.Parse("1980-04-01 01:23:21")))).Show();
        Source.Select(UnixSeconds(Col("ts"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void ToTimestamp_Test()
    {
        Source.Select(ToTimestamp("id")).Show();
        Source.Select(ToTimestamp(Lit(180))).Show();
        Source.Select(ToTimestamp(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Date **/
    [Fact]
    public void LastDay_Test()
    {
        Source.Select(LastDay("dt")).Show();
        Source.Select(LastDay(Lit(new DateOnly(1980, 04, 01)))).Show();
        Source.Select(LastDay(Col("dt"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void UnixTimestamp_Test()
    {
        Source.Select(UnixTimestamp()).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void TimestampSeconds_Test()
    {
        Source.Select(TimestampSeconds("id")).Show();
        Source.Select(TimestampSeconds(Lit(180))).Show();
        Source.Select(TimestampSeconds(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void TimestampMillis_Test()
    {
        Source.Select(TimestampMillis("id")).Show();
        Source.Select(TimestampMillis(Lit(180))).Show();
        Source.Select(TimestampMillis(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void TimestampMicros_Test()
    {
        Source.Select(TimestampMicros("id")).Show();
        Source.Select(TimestampMicros(Lit(180))).Show();
        Source.Select(TimestampMicros(Col("id"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void CurrentCatalog_Test()
    {
        Source.Select(CurrentCatalog()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void CurrentDatabase_Test()
    {
        Source.Select(CurrentDatabase()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void CurrentSchema_Test()
    {
        Source.Select(CurrentSchema()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void CurrentUser_Test()
    {
        Source.Select(CurrentUser()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void User_Test()
    {
        Source.Select(User()).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Binary **/
    [Fact]
    public void Crc32_Test()
    {
        Source.Select(Crc32("idbinary")).Show();
        Source.Select(Crc32(Lit(new byte[] { 0, 1, 2 }))).Show();
        Source.Select(Crc32(Col("idbinary"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Binary **/
    [Fact]
    public void Md5_Test()
    {
        Source.Select(Md5("idbinary")).Show();
        Source.Select(Md5(Lit(new byte[] { 0, 1, 2 }))).Show();
        Source.Select(Md5(Col("idbinary"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Binary **/
    [Fact]
    public void Sha1_Test()
    {
        Source.Select(Sha1("idbinary")).Show();
        Source.Select(Sha1(Lit(new byte[] { 0, 1, 2 }))).Show();
        Source.Select(Sha1(Col("idbinary"))).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Hash_Test()
    {
        Source.Select(Hash("id", "id")).Show();
        Source.Select(Hash(Lit(180), Lit(180))).Show();
        Source.Select(Hash(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Xxhash64_Test()
    {
        Source.Select(Xxhash64("id", "id")).Show();
        Source.Select(Xxhash64(Lit(180), Lit(180))).Show();
        Source.Select(Xxhash64(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Upper_Test()
    {
        Source.Select(Upper("id")).Show();
        Source.Select(Upper(Lit(180))).Show();
        Source.Select(Upper(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Lower_Test()
    {
        Source.Select(Lower("id")).Show();
        Source.Select(Lower(Lit(180))).Show();
        Source.Select(Lower(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Ascii_Test()
    {
        Source.Select(Ascii("id")).Show();
        Source.Select(Ascii(Lit(180))).Show();
        Source.Select(Ascii(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Binary **/
    [Fact]
    public void Base64_Test()
    {
        Source.Select(Base64("idbinary")).Show();
        Source.Select(Base64(Lit(new byte[] { 0, 1, 2 }))).Show();
        Source.Select(Base64(Col("idbinary"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Base64 **/
    [Fact]
    public void Unbase64_Test()
    {
        Source.Select(Unbase64("b64")).Show();
        Source.Select(Unbase64(Lit("SGVsbG8gRnJpZW5kcw=="))).Show();
        Source.Select(Unbase64(Col("b64"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Ltrim_Test()
    {
        Source.Select(Ltrim("id")).Show();
        Source.Select(Ltrim(Lit(180))).Show();
        Source.Select(Ltrim(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Rtrim_Test()
    {
        Source.Select(Rtrim("id")).Show();
        Source.Select(Rtrim(Lit(180))).Show();
        Source.Select(Rtrim(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Trim_Test()
    {
        Source.Select(Trim("id")).Show();
        Source.Select(Trim(Lit(180))).Show();
        Source.Select(Trim(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact(Skip = "To be implemented")]
    public void FormatNumber_Test()
    {
        Assert.Fail("To Be Implemented");
        //Source.Select(FormatNumber("id")).Show();
        //Source.Select(FormatNumber(Lit(180))).Show();
        //Source.Select(FormatNumber(Col("id"))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::Int **/
    [Fact]
    public void Repeat_Test()
    {
        Source.Select(Repeat("id", Lit(0))).Show();
        Source.Select(Repeat(Lit(10), Lit(0))).Show();
        Source.Select(Repeat(Col("id"), Lit(0))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Rlike_Test()
    {
        Source.Select(Rlike("id", "id")).Show();
        Source.Select(Rlike(Lit(1), Lit(1))).Show();
        Source.Select(Rlike(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Regexp_Test()
    {
        Source.Select(Regexp("id", "id")).Show();
        Source.Select(Regexp(Lit(1), Lit(1))).Show();
        Source.Select(Regexp(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegexpLike_Test()
    {
        Source.Select(RegexpLike("id", "id")).Show();
        Source.Select(RegexpLike(Lit(1), Lit(1))).Show();
        Source.Select(RegexpLike(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegexpCount_Test()
    {
        Source.Select(RegexpCount("id", "id")).Show();
        Source.Select(RegexpCount(Lit(1), Lit(1))).Show();
        Source.Select(RegexpCount(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void RegexpSubstr_Test()
    {
        Source.Select(RegexpSubstr("id", "id")).Show();
        Source.Select(RegexpSubstr(Lit(1), Lit(1))).Show();
        Source.Select(RegexpSubstr(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Initcap_Test()
    {
        Source.Select(Initcap("id")).Show();
        Source.Select(Initcap(Lit(180))).Show();
        Source.Select(Initcap(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::String **/
    [Fact]
    public void Soundex_Test()
    {
        Source.Select(Soundex("str")).Show();
        Source.Select(Soundex(Lit("Hello"))).Show();
        Source.Select(Soundex(Col("str"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Bin_Test()
    {
        Source.Select(Bin("id")).Show();
        Source.Select(Bin(Lit(180))).Show();
        Source.Select(Bin(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Hex_Test()
    {
        Source.Select(Hex("id")).Show();
        Source.Select(Hex(Lit(180))).Show();
        Source.Select(Hex(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Unhex_Test()
    {
        Source.Select(Unhex("id")).Show();
        Source.Select(Unhex(Lit(180))).Show();
        Source.Select(Unhex(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Length_Test()
    {
        Source.Select(Length("id")).Show();
        Source.Select(Length(Lit(180))).Show();
        Source.Select(Length(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void OctetLength_Test()
    {
        Source.Select(OctetLength("id")).Show();
        Source.Select(OctetLength(Lit(180))).Show();
        Source.Select(OctetLength(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitLength_Test()
    {
        Source.Select(BitLength("id")).Show();
        Source.Select(BitLength(Lit(180))).Show();
        Source.Select(BitLength(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void UrlDecode_Test()
    {
        Source.Select(UrlDecode("id")).Show();
        Source.Select(UrlDecode(Lit(180))).Show();
        Source.Select(UrlDecode(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void UrlEncode_Test()
    {
        Source.Select(UrlEncode("id")).Show();
        Source.Select(UrlEncode(Lit(180))).Show();
        Source.Select(UrlEncode(Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Endswith_Test()
    {
        Source.Select(Endswith("id", "id")).Show();
        Source.Select(Endswith(Lit(1), Lit(1))).Show();
        Source.Select(Endswith(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Startswith_Test()
    {
        Source.Select(Startswith("id", "id")).Show();
        Source.Select(Startswith(Lit(1), Lit(1))).Show();
        Source.Select(Startswith(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Char_Test()
    {
        Source.Select(Char("id")).Show();
        Source.Select(Char(Lit(180))).Show();
        Source.Select(Char(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void CharLength_Test()
    {
        Source.Select(CharLength("id")).Show();
        Source.Select(CharLength(Lit(180))).Show();
        Source.Select(CharLength(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void CharacterLength_Test()
    {
        Source.Select(CharacterLength("id")).Show();
        Source.Select(CharacterLength(Lit(180))).Show();
        Source.Select(CharacterLength(Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Contains_Test()
    {
        Source.Select(Contains("id", "id")).Show();
        Source.Select(Contains(Lit(1), Lit(1))).Show();
        Source.Select(Contains(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Elt_Test()
    {
        Spark.Conf.Set("spark.sql.ansi.enabled", "false");
        Source.Select(Elt("id", "id")).Show();
        Source.Select(Elt(Lit(180), Lit(180))).Show();
        Source.Select(Elt(Col("id"), Col("id"))).Show();
        Spark.Conf.Set("spark.sql.ansi.enabled", "true");
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void FindInSet_Test()
    {
        Source.Select(FindInSet("id", "id")).Show();
        Source.Select(FindInSet(Lit(1), Lit(1))).Show();
        Source.Select(FindInSet(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Lcase_Test()
    {
        Source.Select(Lcase("id")).Show();
        Source.Select(Lcase(Lit(180))).Show();
        Source.Select(Lcase(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Ucase_Test()
    {
        Source.Select(Ucase("id")).Show();
        Source.Select(Ucase(Lit(180))).Show();
        Source.Select(Ucase(Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Left_Test()
    {
        Source.Select(Left("id", "id")).Show();
        Source.Select(Left(Lit(1), Lit(1))).Show();
        Source.Select(Left(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Right_Test()
    {
        Source.Select(Right("id", "id")).Show();
        Source.Select(Right(Lit(1), Lit(1))).Show();
        Source.Select(Right(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrName::IntArray **/
    [Fact]
    public void MapFromArrays_Test()
    {
        Source.Select(MapFromArrays("idarray", "idarray")).Show();
        Source.Select(MapFromArrays(Lit(new[] { 0, 1, 2 }), Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(MapFromArrays(Col("idarray"), Col("idarray"))).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Array_Test()
    {
        Source.Select(Array("id", "id")).Show();
        Source.Select(Array(Lit(180), Lit(180))).Show();
        Source.Select(Array(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void Array_1_Test()
    {
        Source.Select(Array()).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::ArrayInt **/
    [Fact]
    public void ArrayContains_Test()
    {
        Source.Select(ArrayContains(Col("idarray"), Lit(1980))).Show();
        Source.Select(ArrayContains(Array(Lit(1), Lit(2)), Lit(1))).Show();
        Source.Select(ArrayContains("idarray", Lit(401))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrName::IntArray **/
    [Fact]
    public void ArraysOverlap_Test()
    {
        Source.Select(ArraysOverlap("idarray", "idarray")).Show();
        Source.Select(ArraysOverlap(Lit(new[] { 0, 1, 2 }), Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArraysOverlap(Col("idarray"), Col("idarray"))).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Concat_Test()
    {
        Source.Select(Concat("id", "id")).Show();
        Source.Select(Concat(Lit(180), Lit(180))).Show();
        Source.Select(Concat(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::ArrayInt **/
    [Fact]
    public void ArrayPosition_Test()
    {
        Source.Select(ArrayPosition(Col("idarray"), Lit(1980))).Show();
        Source.Select(ArrayPosition(Array(Lit(1), Lit(2)), Lit(1))).Show();
        Source.Select(ArrayPosition("idarray", Lit(401))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::ArrayInt **/
    [Fact]
    public void ElementAt_Test()
    {
        Spark.Conf.Set("spark.sql.ansi.enabled", "false");
        Source.Select(ElementAt(Col("idarray"), Lit(1980))).Show();
        Source.Select(ElementAt(Array(Lit(1), Lit(2)), Lit(1))).Show();
        Source.Select(ElementAt("idarray", Lit(401))).Show();
        Spark.Conf.Set("spark.sql.ansi.enabled", "true");
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::ArrayInt **/
    [Fact]
    public void ArrayPrepend_Test()
    {
        Source.Select(ArrayPrepend(Col("idarray"), Lit(1980))).Show();
        Source.Select(ArrayPrepend(Array(Lit(1), Lit(2)), Lit(1))).Show();
        Source.Select(ArrayPrepend("idarray", Lit(401))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::ArrayInt **/
    [Fact]
    public void ArrayRemove_Test()
    {
        Source.Select(ArrayRemove(Col("idarray"), Lit(1980))).Show();
        Source.Select(ArrayRemove(Array(Lit(1), Lit(2)), Lit(1))).Show();
        Source.Select(ArrayRemove("idarray", Lit(401))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void ArrayDistinct_Test()
    {
        Source.Select(ArrayDistinct("idarray")).Show();
        Source.Select(ArrayDistinct(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArrayDistinct(Col("idarray"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrName::IntArray **/
    [Fact]
    public void ArrayIntersect_Test()
    {
        Source.Select(ArrayIntersect("idarray", "idarray")).Show();
        Source.Select(ArrayIntersect(Lit(new[] { 0, 1, 2 }), Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArrayIntersect(Col("idarray"), Col("idarray"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrName::IntArray **/
    [Fact]
    public void ArrayUnion_Test()
    {
        Source.Select(ArrayUnion("idarray", "idarray")).Show();
        Source.Select(ArrayUnion(Lit(new[] { 0, 1, 2 }), Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArrayUnion(Col("idarray"), Col("idarray"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrName::IntArray **/
    [Fact]
    public void ArrayExcept_Test()
    {
        Source.Select(ArrayExcept("idarray", "idarray")).Show();
        Source.Select(ArrayExcept(Lit(new[] { 0, 1, 2 }), Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArrayExcept(Col("idarray"), Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void ArrayCompact_Test()
    {
        Source.Select(ArrayCompact("idarray")).Show();
        Source.Select(ArrayCompact(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArrayCompact(Col("idarray"))).Show();
    }

    /** GeneratedBy::ColumnOrNameHidingArgTwoType::ArrayInt **/
    [Fact]
    public void ArrayAppend_Test()
    {
        Source.Select(ArrayAppend(Col("idarray"), Lit(1980))).Show();
        Source.Select(ArrayAppend(Array(Lit(1), Lit(2)), Lit(1))).Show();
        Source.Select(ArrayAppend("idarray", Lit(401))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void Explode_Test()
    {
        Source.Select(Explode("idarray")).Show();
        Source.Select(Explode(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(Explode(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void Posexplode_Test()
    {
        Source.Select(Posexplode("idarray")).Show();
        Source.Select(Posexplode(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(Posexplode(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::ArrayOfMap **/
    [Fact]
    public void Inline_Test()
    {
        Source.Select(Inline("data")).Show();
        Source.Select(Inline(Col("data"))).Show();
        //Todo: Need Lit that can handle this
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void ExplodeOuter_Test()
    {
        Source.Select(ExplodeOuter("idarray")).Show();
        Source.Select(ExplodeOuter(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ExplodeOuter(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void PosexplodeOuter_Test()
    {
        Source.Select(PosexplodeOuter("idarray")).Show();
        Source.Select(PosexplodeOuter(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(PosexplodeOuter(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::ArrayOfMap **/
    [Fact]
    public void InlineOuter_Test()
    {
        Source.Select(InlineOuter("data")).Show();
        Source.Select(InlineOuter(Col("data"))).Show();
        //Todo: Need Lit that can handle this
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::JsonString **/
    [Fact]
    public void JsonArrayLength_Test()
    {
        Source.Select(JsonArrayLength("jstr")).Show();
        Source.Select(JsonArrayLength(Lit("[]"))).Show();
        Source.Select(JsonArrayLength(Col("jstr"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::JsonString **/
    [Fact]
    public void JsonObjectKeys_Test()
    {
        Source.Select(JsonObjectKeys("jstr")).Show();
        Source.Select(JsonObjectKeys(Lit("[]"))).Show();
        Source.Select(JsonObjectKeys(Col("jstr"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void Size_Test()
    {
        Source.Select(Size("idarray")).Show();
        Source.Select(Size(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(Size(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void ArrayMin_Test()
    {
        Source.Select(ArrayMin("idarray")).Show();
        Source.Select(ArrayMin(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArrayMin(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void ArrayMax_Test()
    {
        Source.Select(ArrayMax("idarray")).Show();
        Source.Select(ArrayMax(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArrayMax(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void ArraySize_Test()
    {
        Source.Select(ArraySize("idarray")).Show();
        Source.Select(ArraySize(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArraySize(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void Cardinality_Test()
    {
        Source.Select(Cardinality("idarray")).Show();
        Source.Select(Cardinality(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(Cardinality(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArray **/
    [Fact]
    public void Shuffle_Test()
    {
        Source.Select(Shuffle("idarray")).Show();
        Source.Select(Shuffle(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(Shuffle(Col("idarray"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Reverse_Test()
    {
        Source.Select(Reverse("id")).Show();
        Source.Select(Reverse(Lit(180))).Show();
        Source.Select(Reverse(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::IntArrayArray **/
    [Fact]
    public void Flatten_Test()
    {
        Source.Select(Flatten("idarrayarray")).Show();
        Source.Select(Flatten(Col("idarrayarray"))).Show();
        //Todo: Need a Lit that can handle this
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact(Skip = "To be implemented")]
    public void MapContainsKey_Test()
    {
        Assert.Fail("To Be Implemented");
        //Source.Select(MapContainsKey("id")).Show();
        //Source.Select(MapContainsKey(Lit(180))).Show();
        //Source.Select(MapContainsKey(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Map **/
    [Fact]
    public void MapKeys_Test()
    {
        Source.Select(MapKeys("m")).Show();
        Source.Select(MapKeys(Lit(new Dictionary<string, int> { { "a", 100 } }))).Show();
        Source.Select(MapKeys(Col("m"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Map **/
    [Fact]
    public void MapValues_Test()
    {
        Source.Select(MapValues("m")).Show();
        Source.Select(MapValues(Lit(new Dictionary<string, int> { { "a", 100 } }))).Show();
        Source.Select(MapValues(Col("m"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Map **/
    [Fact]
    public void MapEntries_Test()
    {
        Source.Select(MapEntries("m")).Show();
        Source.Select(MapEntries(Lit(new Dictionary<string, int> { { "a", 100 } }))).Show();
        Source.Select(MapEntries(Col("m"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::ArrayOfMap **/
    [Fact]
    public void MapFromEntries_Test()
    {
        Source.Select(MapFromEntries("data")).Show();
        Source.Select(MapFromEntries(Col("data"))).Show();
        //Todo: Need Lit that can handle this
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::IntArray **/
    [Fact]
    public void ArraysZip_Test()
    {
        Source.Select(ArraysZip("idarray")).Show();
        Source.Select(ArraysZip(Lit(new[] { 0, 1, 2 }))).Show();
        Source.Select(ArraysZip(Col("idarray"))).Show();
    }

    /** GeneratedBy::ParamsColumnOrNameFunction::Map **/
    [Fact]
    public void MapConcat_Test()
    {
        Source.Select(MapConcat("m")).Show();
        Source.Select(MapConcat(Lit(new Dictionary<string, int> { { "a", 100 } }))).Show();
        Source.Select(MapConcat(Lit(new Dictionary<string, int> { { "a", 100 } })),
            Lit(new Dictionary<string, int> { { "b", 100 } })).Show();
        Source.Select(MapConcat(Col("m"))).Show();
        Source.Select(MapConcat(Col("m"), Lit(new Dictionary<string, int> { { "MM", 100 } }))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void MapConcat_1_Test()
    {
        Source.Select(MapConcat()).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::PartitionFunction **/
    [Fact]
    public void Years_Test()
    {
        //TODO: Get Partition Functions Running Locally in PySpark and generate plan
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::PartitionFunction **/
    [Fact]
    public void Months_Test()
    {
        //TODO: Get Partition Functions Running Locally in PySpark and generate plan
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::PartitionFunction **/
    [Fact]
    public void Days_Test()
    {
        //TODO: Get Partition Functions Running Locally in PySpark and generate plan
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::PartitionFunction **/
    [Fact]
    public void Hours_Test()
    {
        //TODO: Get Partition Functions Running Locally in PySpark and generate plan
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void MakeTimestampNtz_Test()
    {
        Spark.Conf.Set("spark.sql.ansi.enabled", "false");
        Source.Select(MakeTimestampNtz("id", "id", "id", "id", "id", "id")).Show();
        Source.Select(MakeTimestampNtz(Lit(1), Lit(1), Lit(1), Lit(1), Lit(1), Lit(1))).Show();
        Source.Select(MakeTimestampNtz(Col("id"), Col("id"), Col("id"), Col("id"), Col("id"), Col("id"))).Show();
        Spark.Conf.Set("spark.sql.ansi.enabled", "true");
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Ifnull_Test()
    {
        Source.Select(Ifnull("id", "id")).Show();
        Source.Select(Ifnull(Lit(1), Lit(1))).Show();
        Source.Select(Ifnull(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Isnotnull_Test()
    {
        Source.Select(Isnotnull("id")).Show();
        Source.Select(Isnotnull(Lit(180))).Show();
        Source.Select(Isnotnull(Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void EqualNull_Test()
    {
        Source.Select(EqualNull("id", "id")).Show();
        Source.Select(EqualNull(Lit(1), Lit(1))).Show();
        Source.Select(EqualNull(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Nullif_Test()
    {
        Source.Select(Nullif("id", "id")).Show();
        Source.Select(Nullif(Lit(1), Lit(1))).Show();
        Source.Select(Nullif(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Nvl_Test()
    {
        Source.Select(Nvl("id", "id")).Show();
        Source.Select(Nvl(Lit(1), Lit(1))).Show();
        Source.Select(Nvl(Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::AllArgsColumnOrNames::EverythingElse **/
    [Fact]
    public void Nvl2_Test()
    {
        Source.Select(Nvl2("id", "id", "id")).Show();
        Source.Select(Nvl2(Lit(1), Lit(1), Lit(1))).Show();
        Source.Select(Nvl2(Col("id"), Col("id"), Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Binary **/
    [Fact]
    public void Sha_Test()
    {
        Source.Select(Sha("idbinary")).Show();
        Source.Select(Sha(Lit(new byte[] { 0, 1, 2 }))).Show();
        Source.Select(Sha(Col("idbinary"))).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void InputFileBlockLength_Test()
    {
        Source.Select(InputFileBlockLength()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void InputFileBlockStart_Test()
    {
        Source.Select(InputFileBlockStart()).Show();
    }

    /** GeneratedBy::NoArgsFunction **/
    [Fact]
    public void Version_Test()
    {
        Source.Select(Version()).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void Typeof_Test()
    {
        Source.Select(Typeof("id")).Show();
        Source.Select(Typeof(Lit(180))).Show();
        Source.Select(Typeof(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitmapBitPosition_Test()
    {
        Source.Select(BitmapBitPosition("id")).Show();
        Source.Select(BitmapBitPosition(Lit(180))).Show();
        Source.Select(BitmapBitPosition(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitmapBucketNumber_Test()
    {
        Source.Select(BitmapBucketNumber("id")).Show();
        Source.Select(BitmapBucketNumber(Lit(180))).Show();
        Source.Select(BitmapBucketNumber(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::EverythingElse **/
    [Fact]
    public void BitmapConstructAgg_Test()
    {
        Source.Select(BitmapConstructAgg("id")).Show();
        Source.Select(BitmapConstructAgg(Lit(180))).Show();
        Source.Select(BitmapConstructAgg(Col("id"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Binary **/
    [Fact]
    public void BitmapCount_Test()
    {
        Source.Select(BitmapCount("idbinary")).Show();
        Source.Select(BitmapCount(Lit(new byte[] { 0, 1, 2 }))).Show();
        Source.Select(BitmapCount(Col("idbinary"))).Show();
    }

    /** GeneratedBy::SingleArgColumnOrNameFunction::Binary **/
    [Fact]
    public void BitmapOrAgg_Test()
    {
        Source.Select(BitmapOrAgg("idbinary")).Show();
        Source.Select(BitmapOrAgg(Lit(new byte[] { 0, 1, 2 }))).Show();
        Source.Select(BitmapOrAgg(Col("idbinary"))).Show();
    }
}