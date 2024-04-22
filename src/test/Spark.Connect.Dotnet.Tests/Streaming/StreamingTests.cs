using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace Spark.Connect.Dotnet.Tests.Streaming;

public class StreamingTests : E2ETestBase
{
    [Fact]
    public void Writer()
    {
        var reader = Spark.ReadStream();
        
        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream();
        writer.Trigger(availableNow: true);
        var query = writer.Format("console").Start();
        
        Thread.Sleep(1000 * 3);
        if (query.IsActive())
        {
            query.Stop();    
        }
    }
    
    [Fact]
    public void Write_ToTable()
    {
        Spark.Sql("DROP TABLE IF EXISTS streamed_table3");
        var reader = Spark.ReadStream();
        
        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream().Option("checkpointLocation", "/tmp/spark-checkpoint/streamed_table3");
        var query = writer.Start(tableName: "streamed_table3");
        
        Thread.Sleep(1000 * 3);
        if (query.IsActive())
        {
            query.Stop();    
        }
        
        Spark.Sql("SELECT * FROM streamed_table3").Show();
        Spark.Sql("DROP TABLE IF EXISTS streamed_table3");
    }
}