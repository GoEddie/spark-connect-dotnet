using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;
namespace Spark.Connect.Dotnet.Tests.DataFrame;
public class GeneratedFunctionsTests : E2ETestBase
{

    /** SortFunctionHandler **/
    [Fact]
    public void Asc_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.OrderBy(Asc("id0"));
      df.Show();
      df = df.OrderBy(Asc(Col("id0")));
      df.Show();
    }

    /** SortFunctionHandler **/
    [Fact]
    public void Desc_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.OrderBy(Desc("id0"));
      df.Show();
      df = df.OrderBy(Desc(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Sqrt_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sqrt("id0"));
      df.Show();
      df = df.WithColumn("new_col", Sqrt(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TryAdd_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TryAdd("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", TryAdd(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TryAvg_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TryAvg("id0"));
      df.Show();
      df = df.WithColumn("new_col", TryAvg(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TryDivide_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TryDivide("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", TryDivide(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TryMultiply_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TryMultiply("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", TryMultiply(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TrySubtract_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TrySubtract("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", TrySubtract(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TrySum_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TrySum("id0"));
      df.Show();
      df = df.WithColumn("new_col", TrySum(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Abs_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Abs("id0"));
      df.Show();
      df = df.WithColumn("new_col", Abs(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Mode_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Mode("id0"));
      df.Show();
      df = df.WithColumn("new_col", Mode(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Max_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Max("id0"));
      df.Show();
      df = df.WithColumn("new_col", Max(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Min_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Min("id0"));
      df.Show();
      df = df.WithColumn("new_col", Min(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MaxBy_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MaxBy("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", MaxBy(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MinBy_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MinBy("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", MinBy(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Count_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Count("id0"));
      df.Show();
      df = df.WithColumn("new_col", Count(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Sum_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sum("id0"));
      df.Show();
      df = df.WithColumn("new_col", Sum(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Avg_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Avg("id0"));
      df.Show();
      df = df.WithColumn("new_col", Avg(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Mean_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Mean("id0"));
      df.Show();
      df = df.WithColumn("new_col", Mean(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Median_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Median("id0"));
      df.Show();
      df = df.WithColumn("new_col", Median(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void SumDistinct_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", SumDistinct("id0"));
      df.Show();
      df = df.WithColumn("new_col", SumDistinct(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Product_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Product("id0"));
      df.Show();
      df = df.WithColumn("new_col", Product(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Acos_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Acos("id0"));
      df.Show();
      df = df.WithColumn("new_col", Acos(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Acosh_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Acosh("id0"));
      df.Show();
      df = df.WithColumn("new_col", Acosh(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Asin_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Asin("id0"));
      df.Show();
      df = df.WithColumn("new_col", Asin(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Asinh_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Asinh("id0"));
      df.Show();
      df = df.WithColumn("new_col", Asinh(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Atan_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Atan("id0"));
      df.Show();
      df = df.WithColumn("new_col", Atan(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Atanh_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Atanh("id0"));
      df.Show();
      df = df.WithColumn("new_col", Atanh(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Cbrt_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Cbrt("id0"));
      df.Show();
      df = df.WithColumn("new_col", Cbrt(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Ceil_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Ceil("id0"));
      df.Show();
      df = df.WithColumn("new_col", Ceil(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Ceiling_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Ceiling("id0"));
      df.Show();
      df = df.WithColumn("new_col", Ceiling(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Cos_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Cos("id0"));
      df.Show();
      df = df.WithColumn("new_col", Cos(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Cosh_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Cosh("id0"));
      df.Show();
      df = df.WithColumn("new_col", Cosh(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Cot_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Cot("id0"));
      df.Show();
      df = df.WithColumn("new_col", Cot(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Csc_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Csc("id0"));
      df.Show();
      df = df.WithColumn("new_col", Csc(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Exp_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Exp("id0"));
      df.Show();
      df = df.WithColumn("new_col", Exp(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Expm1_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Expm1("id0"));
      df.Show();
      df = df.WithColumn("new_col", Expm1(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Floor_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Floor("id0"));
      df.Show();
      df = df.WithColumn("new_col", Floor(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Log_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Log("id0"));
      df.Show();
      df = df.WithColumn("new_col", Log(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Log10_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Log10("id0"));
      df.Show();
      df = df.WithColumn("new_col", Log10(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Log1p_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Log1p("id0"));
      df.Show();
      df = df.WithColumn("new_col", Log1p(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Negative_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Negative("id0"));
      df.Show();
      df = df.WithColumn("new_col", Negative(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Positive_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Positive("id0"));
      df.Show();
      df = df.WithColumn("new_col", Positive(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Rint_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Rint("id0"));
      df.Show();
      df = df.WithColumn("new_col", Rint(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Sec_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sec("id0"));
      df.Show();
      df = df.WithColumn("new_col", Sec(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Signum_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Signum("id0"));
      df.Show();
      df = df.WithColumn("new_col", Signum(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Sign_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sign("id0"));
      df.Show();
      df = df.WithColumn("new_col", Sign(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Sin_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sin("id0"));
      df.Show();
      df = df.WithColumn("new_col", Sin(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Sinh_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sinh("id0"));
      df.Show();
      df = df.WithColumn("new_col", Sinh(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Tan_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Tan("id0"));
      df.Show();
      df = df.WithColumn("new_col", Tan(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Tanh_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Tanh("id0"));
      df.Show();
      df = df.WithColumn("new_col", Tanh(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ToDegrees_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ToDegrees("id0"));
      df.Show();
      df = df.WithColumn("new_col", ToDegrees(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ToRadians_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ToRadians("id0"));
      df.Show();
      df = df.WithColumn("new_col", ToRadians(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitwiseNOT_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitwiseNOT("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitwiseNOT(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitwiseNot_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitwiseNot("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitwiseNot(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitCount_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitCount("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitCount(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitGet_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitGet("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", BitGet(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Getbit_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Getbit("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Getbit(Col("id0"),Col("id1")));
      df.Show();
    }

    /** SortFunctionHandler **/
    [Fact]
    public void AscNullsFirst_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.OrderBy(AscNullsFirst("id0"));
      df.Show();
      df = df.OrderBy(AscNullsFirst(Col("id0")));
      df.Show();
    }

    /** SortFunctionHandler **/
    [Fact]
    public void AscNullsLast_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.OrderBy(AscNullsLast("id0"));
      df.Show();
      df = df.OrderBy(AscNullsLast(Col("id0")));
      df.Show();
    }

    /** SortFunctionHandler **/
    [Fact]
    public void DescNullsFirst_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.OrderBy(DescNullsFirst("id0"));
      df.Show();
      df = df.OrderBy(DescNullsFirst(Col("id0")));
      df.Show();
    }

    /** SortFunctionHandler **/
    [Fact]
    public void DescNullsLast_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.OrderBy(DescNullsLast("id0"));
      df.Show();
      df = df.OrderBy(DescNullsLast(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Stddev_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Stddev("id0"));
      df.Show();
      df = df.WithColumn("new_col", Stddev(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Std_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Std("id0"));
      df.Show();
      df = df.WithColumn("new_col", Std(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void StddevSamp_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", StddevSamp("id0"));
      df.Show();
      df = df.WithColumn("new_col", StddevSamp(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void StddevPop_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", StddevPop("id0"));
      df.Show();
      df = df.WithColumn("new_col", StddevPop(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Variance_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Variance("id0"));
      df.Show();
      df = df.WithColumn("new_col", Variance(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void VarSamp_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", VarSamp("id0"));
      df.Show();
      df = df.WithColumn("new_col", VarSamp(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void VarPop_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", VarPop("id0"));
      df.Show();
      df = df.WithColumn("new_col", VarPop(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrAvgx_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrAvgx("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrAvgx(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrAvgy_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrAvgy("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrAvgy(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrCount_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrCount("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrCount(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrIntercept_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrIntercept("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrIntercept(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrR2_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrR2("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrR2(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrSlope_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrSlope("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrSlope(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrSxx_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrSxx("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrSxx(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrSxy_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrSxy("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrSxy(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegrSyy_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegrSyy("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegrSyy(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Every_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Every("id0"));
      df.Show();
      df = df.WithColumn("new_col", Every(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BoolAnd_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BoolAnd("id0"));
      df.Show();
      df = df.WithColumn("new_col", BoolAnd(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Some_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Some("id0"));
      df.Show();
      df = df.WithColumn("new_col", Some(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BoolOr_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BoolOr("id0"));
      df.Show();
      df = df.WithColumn("new_col", BoolOr(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitAnd_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitAnd("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitAnd(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitOr_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitOr("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitOr(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitXor_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitXor("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitXor(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Skewness_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Skewness("id0"));
      df.Show();
      df = df.WithColumn("new_col", Skewness(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Kurtosis_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Kurtosis("id0"));
      df.Show();
      df = df.WithColumn("new_col", Kurtosis(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CollectList_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CollectList("id0"));
      df.Show();
      df = df.WithColumn("new_col", CollectList(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArrayAgg_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArrayAgg("idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArrayAgg(Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CollectSet_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CollectSet("id0"));
      df.Show();
      df = df.WithColumn("new_col", CollectSet(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Degrees_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Degrees("id0"));
      df.Show();
      df = df.WithColumn("new_col", Degrees(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Radians_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Radians("id0"));
      df.Show();
      df = df.WithColumn("new_col", Radians(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Coalesce_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Coalesce(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Coalesce(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Corr_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Corr("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Corr(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CovarPop_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CovarPop("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", CovarPop(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CovarSamp_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CovarSamp("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", CovarSamp(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void First_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", First("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", First(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Grouping_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Grouping("id0"));
      df.Show();
      df = df.WithColumn("new_col", Grouping(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void GroupingId_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", GroupingId(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", GroupingId(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CountMinSketch_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CountMinSketch("id0","id1","id2","id3"));
      df.Show();
      df = df.WithColumn("new_col", CountMinSketch(Col("id0"),Col("id1"),Col("id2"),Col("id3")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Isnan_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Isnan("id0"));
      df.Show();
      df = df.WithColumn("new_col", Isnan(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Isnull_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Isnull("id0"));
      df.Show();
      df = df.WithColumn("new_col", Isnull(Col("id0")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Last_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Last("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Last(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Nanvl_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Nanvl("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Nanvl(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Round_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Round("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Round(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Bround_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Bround("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Bround(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void ShiftLeft_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ShiftLeft("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", ShiftLeft(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Shiftleft_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Shiftleft("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Shiftleft(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void ShiftRight_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ShiftRight("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", ShiftRight(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Shiftright_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Shiftright("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Shiftright(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void ShiftRightUnsigned_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ShiftRightUnsigned("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", ShiftRightUnsigned(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Shiftrightunsigned_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Shiftrightunsigned("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Shiftrightunsigned(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Struct_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Struct(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Struct(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void NamedStruct_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", NamedStruct(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", NamedStruct(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Greatest_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Greatest(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Greatest(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Least_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Least(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Least(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Ln_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Ln("id0"));
      df.Show();
      df = df.WithColumn("new_col", Ln(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Log2_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Log2("id0"));
      df.Show();
      df = df.WithColumn("new_col", Log2(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Factorial_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Factorial("id0"));
      df.Show();
      df = df.WithColumn("new_col", Factorial(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CountIf_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CountIf("id0"));
      df.Show();
      df = df.WithColumn("new_col", CountIf(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void HistogramNumeric_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", HistogramNumeric("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", HistogramNumeric(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void DateFormat_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", DateFormat("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", DateFormat(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Year_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Year("id0"));
      df.Show();
      df = df.WithColumn("new_col", Year(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Quarter_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Quarter("id0"));
      df.Show();
      df = df.WithColumn("new_col", Quarter(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Month_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Month("id0"));
      df.Show();
      df = df.WithColumn("new_col", Month(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Dayofweek_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Dayofweek("id0"));
      df.Show();
      df = df.WithColumn("new_col", Dayofweek(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Dayofmonth_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Dayofmonth("id0"));
      df.Show();
      df = df.WithColumn("new_col", Dayofmonth(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Day_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Day("id0"));
      df.Show();
      df = df.WithColumn("new_col", Day(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Dayofyear_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Dayofyear("id0"));
      df.Show();
      df = df.WithColumn("new_col", Dayofyear(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Hour_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Hour("id0"));
      df.Show();
      df = df.WithColumn("new_col", Hour(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Minute_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Minute("id0"));
      df.Show();
      df = df.WithColumn("new_col", Minute(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Second_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Second("id0"));
      df.Show();
      df = df.WithColumn("new_col", Second(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Weekofyear_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Weekofyear("id0"));
      df.Show();
      df = df.WithColumn("new_col", Weekofyear(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Weekday_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Weekday("id0"));
      df.Show();
      df = df.WithColumn("new_col", Weekday(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Extract_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Extract("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Extract(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void DatePart_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", DatePart("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", DatePart(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Datepart_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Datepart("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Datepart(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MakeDate_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MakeDate("id0","id1","id2"));
      df.Show();
      df = df.WithColumn("new_col", MakeDate(Col("id0"),Col("id1"),Col("id2")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Datediff_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Datediff("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Datediff(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void DateDiff_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", DateDiff("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", DateDiff(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void DateFromUnixDate_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", DateFromUnixDate("id0"));
      df.Show();
      df = df.WithColumn("new_col", DateFromUnixDate(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void UnixDate_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", UnixDate("id0"));
      df.Show();
      df = df.WithColumn("new_col", UnixDate(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void UnixMicros_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", UnixMicros("id0"));
      df.Show();
      df = df.WithColumn("new_col", UnixMicros(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void UnixMillis_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", UnixMillis("id0"));
      df.Show();
      df = df.WithColumn("new_col", UnixMillis(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void UnixSeconds_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", UnixSeconds("id0"));
      df.Show();
      df = df.WithColumn("new_col", UnixSeconds(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ToTimestamp_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ToTimestamp("id0"));
      df.Show();
      df = df.WithColumn("new_col", ToTimestamp(Col("id0")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void ToTimestamp_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ToTimestamp("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", ToTimestamp(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Xpath_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Xpath("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Xpath(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void XpathBoolean_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", XpathBoolean("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", XpathBoolean(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void XpathDouble_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", XpathDouble("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", XpathDouble(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void XpathNumber_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", XpathNumber("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", XpathNumber(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void XpathFloat_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", XpathFloat("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", XpathFloat(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void XpathInt_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", XpathInt("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", XpathInt(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void XpathLong_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", XpathLong("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", XpathLong(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void XpathShort_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", XpathShort("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", XpathShort(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void XpathString_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", XpathString("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", XpathString(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Trunc_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Trunc("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Trunc(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void NextDay_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", NextDay("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", NextDay(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void LastDay_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", LastDay("id0"));
      df.Show();
      df = df.WithColumn("new_col", LastDay(Col("id0")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void FromUnixtime_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", FromUnixtime("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", FromUnixtime(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void UnixTimestamp_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", UnixTimestamp("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", UnixTimestamp(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void FromUtcTimestamp_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", FromUtcTimestamp("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", FromUtcTimestamp(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ToUtcTimestamp_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ToUtcTimestamp("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", ToUtcTimestamp(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TimestampSeconds_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TimestampSeconds("id0"));
      df.Show();
      df = df.WithColumn("new_col", TimestampSeconds(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TimestampMillis_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TimestampMillis("id0"));
      df.Show();
      df = df.WithColumn("new_col", TimestampMillis(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TimestampMicros_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TimestampMicros("id0"));
      df.Show();
      df = df.WithColumn("new_col", TimestampMicros(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void WindowTime_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", WindowTime("id0"));
      df.Show();
      df = df.WithColumn("new_col", WindowTime(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Crc32_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Crc32("id0"));
      df.Show();
      df = df.WithColumn("new_col", Crc32(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Md5_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Md5("id0"));
      df.Show();
      df = df.WithColumn("new_col", Md5(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Sha1_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sha1("id0"));
      df.Show();
      df = df.WithColumn("new_col", Sha1(Col("id0")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Sha2_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sha2("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Sha2(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Hash_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Hash(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Hash(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Xxhash64_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Xxhash64(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Xxhash64(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Upper_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Upper("id0"));
      df.Show();
      df = df.WithColumn("new_col", Upper(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Lower_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Lower("id0"));
      df.Show();
      df = df.WithColumn("new_col", Lower(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Ascii_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Ascii("id0"));
      df.Show();
      df = df.WithColumn("new_col", Ascii(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Base64_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Base64("id0"));
      df.Show();
      df = df.WithColumn("new_col", Base64(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Unbase64_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Unbase64("id0"));
      df.Show();
      df = df.WithColumn("new_col", Unbase64(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Ltrim_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Ltrim("id0"));
      df.Show();
      df = df.WithColumn("new_col", Ltrim(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Rtrim_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Rtrim("id0"));
      df.Show();
      df = df.WithColumn("new_col", Rtrim(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Trim_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Trim("id0"));
      df.Show();
      df = df.WithColumn("new_col", Trim(Col("id0")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Decode_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Decode("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Decode(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Encode_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Encode("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Encode(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void FormatNumber_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", FormatNumber("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", FormatNumber(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Instr_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Instr("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Instr(Col("id0"),Col("id1")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void Repeat_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Repeat("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Repeat(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Rlike_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Rlike("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Rlike(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Regexp_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Regexp("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Regexp(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegexpLike_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegexpLike("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegexpLike(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegexpCount_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegexpCount("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegexpCount(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void RegexpSubstr_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", RegexpSubstr("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", RegexpSubstr(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Initcap_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Initcap("id0"));
      df.Show();
      df = df.WithColumn("new_col", Initcap(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Soundex_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Soundex("id0"));
      df.Show();
      df = df.WithColumn("new_col", Soundex(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Bin_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Bin("id0"));
      df.Show();
      df = df.WithColumn("new_col", Bin(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Hex_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Hex("id0"));
      df.Show();
      df = df.WithColumn("new_col", Hex(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Unhex_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Unhex("id0"));
      df.Show();
      df = df.WithColumn("new_col", Unhex(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Length_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Length("id0"));
      df.Show();
      df = df.WithColumn("new_col", Length(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void OctetLength_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", OctetLength("id0"));
      df.Show();
      df = df.WithColumn("new_col", OctetLength(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitLength_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitLength("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitLength(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ToChar_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ToChar("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", ToChar(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ToVarchar_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ToVarchar("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", ToVarchar(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ToNumber_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ToNumber("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", ToNumber(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void SplitPart_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", SplitPart("id0","id1","id2"));
      df.Show();
      df = df.WithColumn("new_col", SplitPart(Col("id0"),Col("id1"),Col("id2")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void UrlDecode_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", UrlDecode("id0"));
      df.Show();
      df = df.WithColumn("new_col", UrlDecode(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void UrlEncode_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", UrlEncode("id0"));
      df.Show();
      df = df.WithColumn("new_col", UrlEncode(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Endswith_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Endswith("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Endswith(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Startswith_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Startswith("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Startswith(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Char_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Char("id0"));
      df.Show();
      df = df.WithColumn("new_col", Char(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CharLength_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CharLength("id0"));
      df.Show();
      df = df.WithColumn("new_col", CharLength(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CharacterLength_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CharacterLength("id0"));
      df.Show();
      df = df.WithColumn("new_col", CharacterLength(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TryToNumber_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TryToNumber("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", TryToNumber(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Contains_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Contains("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Contains(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Elt_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Elt(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Elt(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void FindInSet_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", FindInSet("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", FindInSet(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Lcase_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Lcase("id0"));
      df.Show();
      df = df.WithColumn("new_col", Lcase(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Ucase_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Ucase("id0"));
      df.Show();
      df = df.WithColumn("new_col", Ucase(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Left_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Left("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Left(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Right_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Right("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Right(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void CreateMap_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", CreateMap(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", CreateMap(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MapFromArrays_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MapFromArrays("idarray","idarray"));
      df.Show();
      df = df.WithColumn("new_col", MapFromArrays(Col("idarray"),Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Array_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Array(new List<string>(){"idarray"}));
      df.Show();
      df = df.WithColumn("new_col", Array(new List<SparkColumn>(){Col("idarray"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArraysOverlap_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArraysOverlap("idarray","idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArraysOverlap(Col("idarray"),Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Concat_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Concat(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Concat(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void TryElementAt_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", TryElementAt("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", TryElementAt(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArrayDistinct_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArrayDistinct("idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArrayDistinct(Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArrayIntersect_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArrayIntersect("idarray","idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArrayIntersect(Col("idarray"),Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArrayUnion_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArrayUnion("idarray","idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArrayUnion(Col("idarray"),Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArrayExcept_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArrayExcept("idarray","idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArrayExcept(Col("idarray"),Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArrayCompact_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArrayCompact("idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArrayCompact(Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Explode_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Explode("id0"));
      df.Show();
      df = df.WithColumn("new_col", Explode(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Posexplode_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Posexplode("id0"));
      df.Show();
      df = df.WithColumn("new_col", Posexplode(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Inline_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Inline("id0"));
      df.Show();
      df = df.WithColumn("new_col", Inline(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ExplodeOuter_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ExplodeOuter("id0"));
      df.Show();
      df = df.WithColumn("new_col", ExplodeOuter(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void PosexplodeOuter_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", PosexplodeOuter("id0"));
      df.Show();
      df = df.WithColumn("new_col", PosexplodeOuter(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void InlineOuter_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", InlineOuter("id0"));
      df.Show();
      df = df.WithColumn("new_col", InlineOuter(Col("id0")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void GetJsonObject_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", GetJsonObject("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", GetJsonObject(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void JsonTuple_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", JsonTuple("id0"));
      df.Show();
      df = df.WithColumn("new_col", JsonTuple(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void JsonArrayLength_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", JsonArrayLength("idarray"));
      df.Show();
      df = df.WithColumn("new_col", JsonArrayLength(Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void JsonObjectKeys_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", JsonObjectKeys("id0"));
      df.Show();
      df = df.WithColumn("new_col", JsonObjectKeys(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Size_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Size("id0"));
      df.Show();
      df = df.WithColumn("new_col", Size(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArrayMin_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArrayMin("idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArrayMin(Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArrayMax_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArrayMax("idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArrayMax(Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArraySize_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArraySize("idarray"));
      df.Show();
      df = df.WithColumn("new_col", ArraySize(Col("idarray")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Cardinality_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Cardinality("id0"));
      df.Show();
      df = df.WithColumn("new_col", Cardinality(Col("id0")));
      df.Show();
    }
/** ColumnOrNameThenSingleSimpleTypeTest  **/
    [Fact]
    public void SortArray_Test_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", SortArray("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", SortArray(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Shuffle_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Shuffle("id0"));
      df.Show();
      df = df.WithColumn("new_col", Shuffle(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Reverse_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Reverse("id0"));
      df.Show();
      df = df.WithColumn("new_col", Reverse(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Flatten_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Flatten("id0"));
      df.Show();
      df = df.WithColumn("new_col", Flatten(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MapKeys_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MapKeys("id0"));
      df.Show();
      df = df.WithColumn("new_col", MapKeys(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MapValues_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MapValues("id0"));
      df.Show();
      df = df.WithColumn("new_col", MapValues(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MapEntries_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MapEntries("id0"));
      df.Show();
      df = df.WithColumn("new_col", MapEntries(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MapFromEntries_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MapFromEntries("id0"));
      df.Show();
      df = df.WithColumn("new_col", MapFromEntries(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void ArraysZip_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", ArraysZip(new List<string>(){"idarray"}));
      df.Show();
      df = df.WithColumn("new_col", ArraysZip(new List<SparkColumn>(){Col("idarray"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MapConcat_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MapConcat(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", MapConcat(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Years_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Years("id0"));
      df.Show();
      df = df.WithColumn("new_col", Years(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Months_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Months("id0"));
      df.Show();
      df = df.WithColumn("new_col", Months(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Days_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Days("id0"));
      df.Show();
      df = df.WithColumn("new_col", Days(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Hours_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Hours("id0"));
      df.Show();
      df = df.WithColumn("new_col", Hours(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void MakeTimestampNtz_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", MakeTimestampNtz("id0","id1","id2","id3","id4","id5"));
      df.Show();
      df = df.WithColumn("new_col", MakeTimestampNtz(Col("id0"),Col("id1"),Col("id2"),Col("id3"),Col("id4"),Col("id5")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void UnwrapUdt_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", UnwrapUdt("id0"));
      df.Show();
      df = df.WithColumn("new_col", UnwrapUdt(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void HllSketchEstimate_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", HllSketchEstimate("id0"));
      df.Show();
      df = df.WithColumn("new_col", HllSketchEstimate(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Ifnull_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Ifnull("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Ifnull(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Isnotnull_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Isnotnull("id0"));
      df.Show();
      df = df.WithColumn("new_col", Isnotnull(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void EqualNull_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", EqualNull("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", EqualNull(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Nullif_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Nullif("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Nullif(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Nvl_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Nvl("id0","id1"));
      df.Show();
      df = df.WithColumn("new_col", Nvl(Col("id0"),Col("id1")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Nvl2_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Nvl2("id0","id1","id2"));
      df.Show();
      df = df.WithColumn("new_col", Nvl2(Col("id0"),Col("id1"),Col("id2")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Sha_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Sha("id0"));
      df.Show();
      df = df.WithColumn("new_col", Sha(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Reflect_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Reflect(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Reflect(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Typeof_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Typeof("id0"));
      df.Show();
      df = df.WithColumn("new_col", Typeof(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void Stack_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", Stack(new List<string>(){"id0"}));
      df.Show();
      df = df.WithColumn("new_col", Stack(new List<SparkColumn>(){Col("id0"),}));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitmapBitPosition_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitmapBitPosition("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitmapBitPosition(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitmapBucketNumber_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitmapBucketNumber("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitmapBucketNumber(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitmapConstructAgg_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitmapConstructAgg("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitmapConstructAgg(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitmapCount_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitmapCount("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitmapCount(Col("id0")));
      df.Show();
    }
/** AllArgsColumnOrNameTest  **/
    [Fact]
    public void BitmapOrAgg_()
    {
      var df = Spark.Sql("SELECT array(id, id + 1, id + 2) as idarray, id, id as id0, id as id1, id as id2, id as id3, id as id4 FROM range(100)");
      df = df.WithColumn("new_col", BitmapOrAgg("id0"));
      df.Show();
      df = df.WithColumn("new_col", BitmapOrAgg(Col("id0")));
      df.Show();
    }
}
