using Spark.Connect.Dotnet.Pipelines.Attributes;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.Tests.Pipelines;

/// <summary>
/// Example declarative pipeline demonstrating advanced features:
/// - ClusteringColumns for optimized table layout
/// - Once property for one-time/backfill flows
/// - Sink output type for streaming writes
/// </summary>
[DeclarativePipeline(DefaultDatabase = "advanced_demo", Storage = "file:///tmp/spark-pipelines-test")]
public class AdvancedFeaturesDeclarativePipeline
{
    /// <summary>
    /// A streaming table with clustering columns for optimized queries.
    /// Clustering columns help with data skipping and query performance.
    /// </summary>
    [StreamingTable(
        Name = "clustered_events",
        Comment = "Event stream with clustering for efficient queries",
        ClusteringColumns = new[] { "event_date", "event_type" },
        PartitionCols = new[] { "year", "month" })]
    public Dotnet.Sql.DataFrame ClusteredEvents(SparkSession spark)
    {
        return spark.ReadStream()
            .Format("rate")
            .Option("rowsPerSecond", "10")
            .Load()
            .WithColumn("event_date", Functions.CurrentDate())
            .WithColumn("event_type", Functions.Lit("click"))
            .WithColumn("year", Functions.Year(Functions.CurrentDate()))
            .WithColumn("month", Functions.Month(Functions.CurrentDate()));
    }

    /// <summary>
    /// A one-time backfill table that only runs once.
    /// Useful for historical data loads or initial seeding.
    /// </summary>
    [StreamingTable(
        Name = "historical_backfill",
        Comment = "One-time backfill of historical data",
        Once = true)]
    public Dotnet.Sql.DataFrame HistoricalBackfill(SparkSession spark)
    {
        // This would typically read from a historical data source
        return spark.Range(1000)
            .WithColumn("backfill_date", Functions.Lit("2024-01-01"))
            .WithColumn("source", Functions.Lit("historical"));
    }

    /// <summary>
    /// A materialized view with clustering columns.
    /// </summary>
    [MaterializedView(
        Name = "aggregated_metrics",
        Comment = "Aggregated metrics with clustering",
        ClusteringColumns = new[] { "metric_date" },
        Format = "delta")]
    public Dotnet.Sql.DataFrame AggregatedMetrics(SparkSession spark)
    {
        return spark.Read.Table("clustered_events")
            .GroupBy(Functions.Col("event_date").Alias("metric_date"), Functions.Col("event_type"))
            .Agg(Functions.Count("*").Alias("event_count"));
    }

    /// <summary>
    /// A one-time materialized view for initial computation.
    /// </summary>
    [MaterializedView(
        Name = "initial_summary",
        Comment = "One-time summary computation",
        Once = true)]
    public Dotnet.Sql.DataFrame InitialSummary(SparkSession spark)
    {
        return spark.Read.Table("historical_backfill")
            .GroupBy(Functions.Col("backfill_date"))
            .Agg(Functions.Count("*").Alias("total_records"));
    }

    /// <summary>
    /// A temporary view with one-time execution.
    /// </summary>
    [TemporaryView(
        Name = "temp_lookup",
        Comment = "Temporary lookup data",
        Once = true)]
    public Dotnet.Sql.DataFrame TempLookup(SparkSession spark)
    {
        return spark.Range(100)
            .WithColumn("lookup_key", Functions.Col("id"))
            .WithColumn("lookup_value", Functions.Concat(Functions.Lit("value_"), Functions.Col("id")));
    }

    /// <summary>
    /// A sink that writes processed data to an external destination.
    /// </summary>
    [Sink(
        Name = "processed_events_sink",
        Comment = "Sink for processed events",
        Format = "delta")]
    public Dotnet.Sql.DataFrame ProcessedEventsSink(SparkSession spark)
    {
        return spark.Read.Table("clustered_events")
            .Filter(Functions.Col("event_type").EqualTo(Functions.Lit("click")));
    }

    /// <summary>
    /// SQL configuration for specific tables.
    /// </summary>
    [SqlConfFor(Name = "clustered_events")]
    public Dictionary<string, string> ClusteredEventsSqlConf()
    {
        return new Dictionary<string, string>
        {
            { "spark.sql.shuffle.partitions", "200" }
        };
    }

    /// <summary>
    /// Table properties for the clustered events table.
    /// </summary>
    [TableOptionsFor(Name = "clustered_events")]
    public Dictionary<string, string> ClusteredEventsTableOptions()
    {
        return new Dictionary<string, string>
        {
            { "delta.autoOptimize.optimizeWrite", "true" },
            { "delta.autoOptimize.autoCompact", "true" }
        };
    }
}
