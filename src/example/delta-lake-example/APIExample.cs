using System.Runtime.CompilerServices;
using Spark.Connect;
using Spark.Connect.Dotnet.DeltaLake;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace delta_lake_example;

public class APIExample
{
    public void Run(SparkSession spark, string deltaPath, string csvPath)
    {
        Console.WriteLine($"Is the path '{csvPath} as deltaTable? {DeltaTable.IsDeltaTable(spark, csvPath)}");
        Console.WriteLine($"Is the path '{deltaPath} as deltaTable? {DeltaTable.IsDeltaTable(spark, deltaPath)}");
        
        var deltaTable = DeltaTable.ForPath(spark, deltaPath);
        deltaTable.History().Show(10, 1000);

        deltaTable.Update("id < 10", (Col("id"), Lit(0)));
        
        deltaTable.ToDF().Show(20);

        var source = spark.Range( 5).WithColumn("name", Lit("teddy")).Alias("src");
        deltaTable
            .As("tgt")
            .Merge(source, "src.id = tgt.id")
            .WithSchemaEvolution()
            .WhenNotMatchedInsertAll()
            .WhenMatchedUpdateAll()
            .Execute(spark);
        
        deltaTable.ToDF().Show(50);

        deltaTable.RestoreToVersion(1);
        
        deltaTable.ToDF().Show(50);
        deltaTable.Delete(Lit(true));
        deltaTable.ToDF().Show(50);
        
        deltaTable.Detail().Show();

        var newDeltaTable = DeltaTable
            .CreateOrReplace(spark)
            .TableName("deltatablefuntest")
            .AddColumn(
                new DeltaTableColumnBuilder("col_a")
                    .DataType("int")
                    .Build()
            )
            .AddColumn(
                new DeltaTableColumnBuilder("col_b")
                    .DataType("int")
                    .GeneratedAlwaysAs("1980")
                    .Nullable(false)
                    .Build()
            )
            .Execute();
        
        newDeltaTable.ToDF().Show();
        
        var sourceDataFrame = spark.CreateDataFrame((new List<(int, long)>()
        {
            (1, 1980), (2, 1980), (3, 1980), (4, 1980), (5, 1980), (6, 1980)
        }).Cast<ITuple>(), new StructType((List<StructField>) [new StructField("this_is_cola", new IntegerType(), false), new StructField("colb", new BigIntType(), false)])).Alias("source");

        sourceDataFrame.Show();
        newDeltaTable.As("target")
            .Merge(sourceDataFrame, "source.this_is_cola = target.col_a")
            .WhenMatchedUpdate(
                (Col("target.col_a"), Col("source.this_is_cola")), 
                (Col("target.col_b"), Col("source.colb")))   
            .WhenNotMatchedInsert(
                (Col("target.col_a"), Col("source.this_is_cola")), 
                (Col("target.col_b"), Col("source.colb")))
            .WithSchemaEvolution()
            .Execute(spark);
        
        newDeltaTable.ToDF().Show();
        
        newDeltaTable.Optimize().ExecuteCompaction();
        
        newDeltaTable.Vacuum();
    }
}