using Apache.Arrow;
using Delta.Connect;
using Google.Protobuf.WellKnownTypes;
using Spark.Connect;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using static Spark.Connect.Dotnet.Sql.Functions;

namespace delta_lake_example;

public class ProtoExample
{
    public void Run(SparkSession spark, string deltaPath, string csvPath)
    {
        
        bool IsDeltaTable(DeltaTable deltaTable)
        {
            if (deltaTable.HasTableOrViewName)
            {
                throw new InvalidOperationException("The delta table must have a path and not a table name");
            }
            
            var deltaRelation = new DeltaRelation()
            {
                IsDeltaTable = new IsDeltaTable()
                {
                    Path = deltaTable.Path.Path_
                }
            };

            var relation = new Relation
            {
                Extension = Any.Pack(deltaRelation)
            };

            var plan = new Plan()
            {
                Root = relation
            };

            var requestExecutor = new RequestExecutor(spark, plan, ArrowHandling.ArrowBuffers);
            requestExecutor.Exec();

            var recordBatches = requestExecutor.GetArrowBatches();
                
            var boolArray = recordBatches.First().Column("value") as BooleanArray;
            var result = boolArray!.GetValue(0);
            return result!.Value;
        }

        void UpdateDeltaTable(DeltaTable deltaTable, IList<Assignment> assignments, Expression condition)
        {
            var deltaScan = new DeltaRelation()
            {
                Scan = new Scan()
                {
                    Table = deltaTable
                }
            };
                
            var updateRelation = new DeltaRelation
            {
                UpdateTable = new UpdateTable()
                {
                    Assignments =
                    {
                        assignments
                    }, 
                    Condition = condition,
                    Target = new Relation()
                    {
                        Extension = Any.Pack(deltaScan)
                    }
                }
            };
            
            var plan = new Plan()
            {
                Root = new Relation()
                {
                    Extension = Any.Pack(updateRelation)
                }
            };
            
            var requestExecutor = new RequestExecutor(spark, plan, ArrowHandling.ArrowBuffers);
            requestExecutor.Exec();
            var recordBatches = requestExecutor.GetArrowBatches();
            var longArray = (recordBatches.First().Column("num_affected_rows") as Int64Array)!;
            
            Console.WriteLine($"Update affected {longArray.GetValue(0)!.Value} rows");
        }


        var deltaTable = new DeltaTable()
        {
            Path = new DeltaTable.Types.Path()
            {
                Path_ = deltaPath
            }
        };

        var csvTable = new DeltaTable()
        {
            Path = new DeltaTable.Types.Path()
            {
                Path_ = csvPath
            }
        };

        Console.WriteLine($"The path '{deltaPath}' IsDeltaTable = {IsDeltaTable(deltaTable)}");
        Console.WriteLine($"The path '{csvPath}' IsDeltaTable = {IsDeltaTable(csvTable)}");

        UpdateDeltaTable(deltaTable,                //UPDATE TABLE
                new List<Assignment>()                           
                            {                                               
                                new()                                      
                                {                                          //
                                    Field = Col("id").Expression,    // SET id = 999
                                    Value = Lit(999).Expression            //
                                }                                          
                            },                                                  
                      Col("id").Le(50).Expression); // WHERE id <= 50

    }
}