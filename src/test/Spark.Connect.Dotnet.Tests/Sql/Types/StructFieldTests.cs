using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Tests.Sql.Types;

public class StructFieldTests
{
    [Fact]
    public void Ddl_StartsWith_FieldName()
    {
        var field = new StructField("col_a", new StringType(), true);
        var result = field.Ddl();
        Assert.StartsWith("col_a", result);
    }

    [Fact]
    public void ForNullable_Ddl_EndsWith_FieldType()
    {
        var field = new StructField("col_a", new StringType(), true);
        var result = field.Ddl();
        Assert.EndsWith("string", result);
    }

    [Fact]
    public void Ddl_FollowsFieldNameWith_Space()
    {
        var field = new StructField("col_a", new StringType(), true);
        var result = field.Ddl();
        Assert.Equal(' ', result[5]);
    }

    [Fact]
    public void ForNonNullable_Ddl_EndsWith_NotNull()
    {
        var field = new StructField("col_a", new StringType(), false);
        var result = field.Ddl();
        Assert.EndsWith(" NOT NULL", result);
    }

    [Fact]
    public void ForNullable_Ddl_DoesNotContain_NotNull()
    {
        var field = new StructField("col_a", new StringType(), true);
        var result = field.Ddl();
        Assert.DoesNotContain("NOT NULL", result);
    }
}