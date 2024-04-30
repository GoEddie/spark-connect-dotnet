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
        var tableName = $"streamed_{Guid.NewGuid().ToString().Replace("-", "")}";
        Spark.Sql($"DROP TABLE IF EXISTS {tableName}");
        var reader = Spark.ReadStream();

        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream().Option("checkpointLocation", $"/tmp/spark-checkpoint/{tableName}");
        var query = writer.Start(tableName: tableName);

        Thread.Sleep(1000 * 3);
        if (query.IsActive())
        {
            query.Stop();
        }
        Thread.Sleep(1000 * 3);
        Spark.Sql($"SELECT * FROM {tableName}").Show();
        Spark.Sql($"DROP TABLE IF EXISTS {tableName}");
    }

    [Fact]
    public async Task AwaitTermination_Terminated_Test()
    {
        var reader = Spark.ReadStream();

        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream();

        var query = writer.Format("console").Start();

        Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromSeconds(5));
            Console.WriteLine("Stopping Query");
            query.Stop();
        });

        if (!query.AwaitTermination(30))
        {
            Assert.Fail("Query was terminated so should have failed");
        }
    }

    [Fact]
    public async Task AwaitTermination_TimedOut_Test()
    {
        var reader = Spark.ReadStream();

        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream();

        var query = writer.Format("console").Start();

        Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromSeconds(3));
            Console.WriteLine("Stopping Query");
            query.Stop();
        });

        if (query.AwaitTermination(1))
        {
            Task.Delay(5);
            Assert.Fail("Query was not terminated terminated so should have failed");
        }
    }

    [Fact]
    public async Task AwaitAnyTermination_Test()
    {
        var reader = Spark.ReadStream();

        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream();

        var query1 = writer.Format("console").Start();
        var query2 = writer.Format("console").Start();
        var query3 = writer.Format("console").Start();
        try
        {
            Assert.Equal(3, Spark.Streams.Active.Count());
            Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(3));
                Console.WriteLine("Stopping Query");
                query1.Stop();
            });

            Spark.Streams.AwaitAnyTermination(10);
            Assert.False(query1.IsActive());
            Assert.True(query2.IsActive());
            Assert.True(query3.IsActive());

            query2.Stop();
            query3.Stop();
        }
        catch (Exception ex)
        {
            try
            {
                query1.Stop();
                query2.Stop();
                query3.Stop();
            }
            catch (Exception)
            {
            }

            throw ex;
        }
    }

    [Fact]
    public void NoExceptionTest()
    {
        var reader = Spark.ReadStream();

        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream().Format("console");
        var query = writer.Start();
        query.Stop();
        Assert.Null(query.Exception());
    }

    [Fact]
    public void ProcessAllAvailable_Test()
    {
        var reader = Spark.ReadStream();

        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream();
        writer.Trigger(availableNow: true);
        var query = writer.Format("console").Start();
        query.ProcessAllAvailable();
        query.Stop();
    }

    [Fact]
    public void RecentProgress_Test()
    {
        var reader = Spark.ReadStream();

        var df = reader.Format("rate").Load();
        df = df.Select((Column("value") % 3).Alias("v"));
        var writer = df.WriteStream().Option("spark.sql.streaming.numRecentProgressUpdates", "100");
        writer.Trigger(availableNow: true);
        var query = writer.Format("console").Start();
        Thread.Sleep(TimeSpan.FromSeconds(2));
        var recentProgress = query.RecentProgress();
        query.Stop();
        Assert.NotEmpty(recentProgress);
    }
}