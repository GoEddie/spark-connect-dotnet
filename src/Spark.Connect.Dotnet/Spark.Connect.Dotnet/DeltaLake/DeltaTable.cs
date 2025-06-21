using System.Diagnostics;
using Apache.Arrow;
using Delta.Connect;
using Google.Protobuf.WellKnownTypes;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql;
using Column = Spark.Connect.Dotnet.Sql.Column;

namespace Spark.Connect.Dotnet.DeltaLake;

public class DeltaTable
{
    private readonly SparkSession _spark;
    private readonly Delta.Connect.DeltaTable _deltaTable;
    private DataFrame _dataFrame;
    
    public Delta.Connect.DeltaTable ProtoDeltaTable => _deltaTable;

    public DeltaTable(SparkSession spark, Delta.Connect.DeltaTable deltaTable)
    {
        _spark = spark;
        _deltaTable = deltaTable;
        _dataFrame = ForTable(spark, deltaTable);
    }
    
    public DeltaTable(SparkSession spark, DataFrame dataFrame, Delta.Connect.DeltaTable deltaTable)
    {
        _spark = spark;
        _deltaTable = deltaTable;
        _dataFrame = dataFrame;
    }

    /// <summary>
    /// Get a `DataFrame` of the latest version of the delta table
    /// </summary>
    /// <returns></returns>
    public DataFrame ToDF() => _dataFrame;
    
    public static DeltaTable ForPath(SparkSession spark, string path)
    {
        var dt = new Delta.Connect.DeltaTable()
        {
            Path = new ()
            {
                Path_ = path
            }
        };
        
        DeltaTable deltaTable = new DeltaTable(spark, dt);
        return deltaTable;
    }

    public static DeltaTable ForName(SparkSession spark, string tableName)
    {
        var dt = new Delta.Connect.DeltaTable()
        {
            TableOrViewName = tableName
        };
        
        DeltaTable deltaTable = new DeltaTable(spark, dt);
        return deltaTable;
    }
    
    private DataFrame ForTable(SparkSession spark, Delta.Connect.DeltaTable deltaTable)
    {
        var relation = new DeltaRelation()
        {
            Scan = new Scan()
            {
                Table = deltaTable
            }
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        return new DataFrame(spark, sparkRelation);
    }

    /// <summary>
    /// Apply an alias to the DeltaTable. This is similar to `DataFrame.Alias(alias)` or SQL `tableName AS alias`.
    /// </summary>
    /// <param name="alias"></param>
    /// <returns>The aliased `DeltaTable`</returns>
    public DeltaTable As(string alias) => Alias(alias);
    
    /// <summary>
    /// Apply an alias to the DeltaTable. This is similar to `DataFrame.Alias(alias)` or SQL `tableName AS alias`.
    /// </summary>
    /// <param name="alias"></param>
    /// <returns>The aliased `DeltaTable`</returns>
    public DeltaTable Alias(string alias)
    {
        return new DeltaTable(_spark, _dataFrame.Alias(alias), _deltaTable);
    }
    
    /// <summary>
    /// Recursively delete files and directories in the table that are not needed by the table for
    /// maintaining older versions up to the given retention threshold. 
    /// </summary>
    /// <param name="retentionHours"></param>
    /// <returns>This `DeltaTable`</returns>
    public DeltaTable Vacuum(double retentionHours)
    {
        var command = new DeltaCommand()
        {
            VacuumTable = new VacuumTable()
            {
                RetentionHours = retentionHours, Table = _deltaTable
            }
        };

        var plan = new Plan()
        {
            Command = new Command()
            {
                Extension = Any.Pack(command)
            }
        };
        
        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
        return this;
    }

    /// <summary>
    /// Recursively delete files and directories in the table that are not needed by the table for
    /// maintaining older versions up to the given retention threshold. Uses the default of 7 days.
    /// </summary>
    /// <returns>This `DeltaTable`</returns>
    public DeltaTable Vacuum() => Vacuum(TimeSpan.FromDays(7).TotalHours);
    
    /// <summary>
    /// Get the information available commits on this table as a Spark DataFrame.
    /// The information is in reverse chronological order.
    /// </summary>
    /// <param name="limit">Max hostory items to retrieve</param>
    /// <returns>`DataFrame` of the history</returns>
    public DataFrame History(int limit) => History().Limit(limit);

    /// <summary>
    /// Get the information available commits on this table as a Spark DataFrame.
    /// The information is in reverse chronological order.
    /// </summary>
    /// <returns>`DataFrame` of the history</returns>
    public DataFrame History()
    {
        var deltaRelation = new DeltaRelation()
        {
            DescribeHistory = new DescribeHistory()
            {
                Table = _deltaTable,
            }
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(deltaRelation)
        };

        return new DataFrame(_spark, sparkRelation);
    }

    /// <summary>
    /// Get the details of a Delta table such as the format, name, and size.
    /// </summary>
    /// <returns>`DataFrame` with the details in</returns>
    public DataFrame Detail()
    {
        var deltaRelation = new DeltaRelation()
        {
            DescribeDetail = new DescribeDetail()
            {
                Table = _deltaTable,
            }
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(deltaRelation)
        };
        
        return new DataFrame(_spark, sparkRelation);
    }

    /// <summary>
    /// Delete data from the table that match the given `condition`.
    /// </summary>
    /// <param name="condition">string condition that is compatible with `Functions.Expr` such as `id == 1`</param>
    public void Delete(string condition)
    {
        var delete = new DeleteFromTable()
        {
            Target = _dataFrame.Relation,
            Condition = Functions.Expr(condition).Expression
        };

        var relation = new DeltaRelation()
        {
            DeleteFromTable = delete
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        var df = new DataFrame(_spark, sparkRelation);
        df.Collect();
    }
    
    /// <summary>
    /// Delete data from the table that match the given `condition`.
    /// </summary>
    /// <param name="condition">`Column` such as `Functions.Col("id") == 1`</param>
    public void Delete(Column condition)
    {
        var delete = new DeleteFromTable()
        {
            Target = _dataFrame.Relation,
            Condition = condition.Expression
        };

        var relation = new DeltaRelation()
        {
            DeleteFromTable = delete
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        var df = new DataFrame(_spark, sparkRelation);
        df.Collect();
    }
    
    private IEnumerable<Assignment> ToAssignments((string key, Column value)[] setClauses){
        return setClauses.Select(setClause => new Assignment() { Field = Functions.Expr(setClause.key).Expression, Value = setClause.value.Expression}).ToList();    
    }

    private IEnumerable<Assignment> ToAssignments((Column col, Column value)[] setClauses){
        return setClauses.Select(setClause => new Assignment() { Field  = setClause.col.Expression, Value = setClause.value.Expression}).ToList();    
    }
    
    /// <summary>
    ///  Update rows in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">Where clause</param>
    /// <param name="setClauses">Array of Tuples of Column name and new Value</param>
    /// <example>
    /// 
    /// dt.Update(Functions.Col("id") &lt;= 49, ("Cola", Lit("ABC")), ("Colb", Lit("DEF")));
    /// </example>
    /// <returns></returns>
    public DeltaTable Update(Column? condition, params (string key, Column value)[] setClauses)
    {
        var protoAssignments = ToAssignments(setClauses);
        var relation = new DeltaRelation()
        {
            UpdateTable = new UpdateTable()
            {
                Assignments = { protoAssignments }, 
                Target = _dataFrame.Relation,
            }
        };

        if (condition is not null)
        {
            relation.UpdateTable.Condition = condition.Expression;
        }
        
        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        var df = new DataFrame(_spark, sparkRelation);
        df.Collect();
        return this;
    }
    
    /// <summary>
    ///  Update rows in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">Where clause</param>
    /// <param name="setClauses">Array of Tuples of Column name and new Value</param>
    /// <example>
    /// 
    /// dt.Update(Functions.Col("id") &lt;= 49, ("Cola", Lit("ABC")), ("Colb", Lit("DEF")));
    /// </example>
    /// <returns></returns>
    public DeltaTable Update(string? condition, params (string key, Column value)[] setClauses)
    {
        var protoAssignments = ToAssignments(setClauses);
        var relation = new DeltaRelation()
        {
            UpdateTable = new UpdateTable()
            {
                Assignments = { protoAssignments }, 
                Target = _dataFrame.Relation,
            }
        };

        if (condition is not null)
        {
            relation.UpdateTable.Condition = Functions.Expr(condition).Expression;
        }
        
        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        var df = new DataFrame(_spark, sparkRelation);
        df.Collect();
        return this;
    }
    
    /// <summary>
    ///  Update rows in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">Where clause</param>
    /// <param name="setClauses">Array of Tuples of Column name and new Value</param>
    /// <example>
    /// 
    /// dt.Update(Functions.Col("id") &lt;= 49, ("Cola", Lit("ABC")), ("Colb", Lit("DEF")));
    /// </example>
    /// <returns></returns>
    public DeltaTable Update(string? condition, params (Column key, Column value)[] setClauses)
    {
        var protoAssignments = ToAssignments(setClauses);
        var relation = new DeltaRelation()
        {
            UpdateTable = new UpdateTable()
            {
                Assignments = { protoAssignments }, 
                Target = _dataFrame.Relation,
            }
        };

        if (condition is not null)
        {
            relation.UpdateTable.Condition = Functions.Expr(condition).Expression;
        }
        
        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        var df = new DataFrame(_spark, sparkRelation);
        df.Collect();
        return this;
    }
    
    /// <summary>
    ///  Update rows in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">Where clause</param>
    /// <param name="setClauses">Array of Tuples of Column and new Value</param>
    /// <example>
    /// 
    /// dt.Update(Functions.Col("id") &lt;= 49, (Col("Cola"), Lit("ABC")), (DataFrame["Colb"], Lit("DEF")));
    /// </example>
    /// <returns></returns>
    public DeltaTable Update(Column? condition, params (Column key, Column value)[] setClauses)
    {
        var protoAssignments = ToAssignments(setClauses);
        var relation = new DeltaRelation()
        {
            UpdateTable = new UpdateTable()
            {
                Assignments = { protoAssignments }, 
                Target = _dataFrame.Relation,
            }
        };

        if (condition is not null)
        {
            relation.UpdateTable.Condition = condition.Expression;
        }
        
        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        var df = new DataFrame(_spark, sparkRelation);
        df.Collect();
        return this;
    }

    
    /// <summary>
    ///  Update rows in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">Where clause</param>
    /// <param name="set">Dictionary of column references (Functions.Col("col_name")) and their new values</param>
    /// <example>
    /// var map = new Dictionary&lt;Column, Column&gt; {{Functions.Col("id"), Functions.Lit(-999)}};
    /// dt.Update(Functions.Col("id") &lt;= 49, map);
    /// </example>
    /// <returns></returns>
    public DeltaTable Update(Column? condition, IDictionary<Column, Column> set)
    {
        var protoAssignments = set.Select(assignment => new Assignment() { Field = assignment.Key.Expression, Value = assignment.Value.Expression }).ToList();
        var relation = new DeltaRelation()
        {
            UpdateTable = new UpdateTable()
            {
                Assignments = { protoAssignments }, 
                Target = _dataFrame.Relation,
            }
        };

        if (condition is not null)
        {
            relation.UpdateTable.Condition = condition.Expression;
        }
        
        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };
        
        var df = new DataFrame(_spark, sparkRelation);
        df.Collect();
        return this;
    }

    /// <summary>
    /// Update rows in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">Optional where clause</param>
    /// <param name="set">Dictionary of column names and their new values</param>
    /// <example>
    /// var map = new Dictionary&lt;string, Column&gt; {{"id", Functions.Lit(-999)}};
    /// dt.Update(Functions.Col("id") &lt;= 49, map);
    /// </example>
    /// <returns>This `DeltaTable`</returns>
    public DeltaTable Update(Column? condition, IDictionary<string, Column> set)
    {
        var columnSets = new Dictionary<Column, Column>();
        foreach (var (key, value) in set)
        {
            columnSets.Add(Functions.Col(key), value);
        }
        
        return Update(condition, columnSets);
    }
    
    /// <summary>
    /// Update all rows in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="set">Dictionary of column names and their new values</param>
    /// <example>
    /// var map = new Dictionary&lt;string, Column&gt; {{"id", Functions.Lit(-999)}};
    /// dt.Update(map);
    /// </example>
    /// <returns>This `DeltaTable`</returns>
    public DeltaTable Update(IDictionary<string, Column> set) => Update(null as Column, set);
    
    /// <summary>
    /// Update all rows in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="set">Dictionary of columns and their new values</param>
    /// <example>
    /// var map = new Dictionary&lt;Column, Column&gt; {{Functions.Col("id"), Functions.Lit(-999)}};
    /// dt.Update(map);
    /// </example>
    /// <returns>This `DeltaTable`</returns>
    public DeltaTable Update(IDictionary<Column, Column> set) => Update(null as Column, set);
    
    /// <summary>
    /// Update rows that match `condition` in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">A SQL condition that can be parsed by `Functions.Expr`</param>
    /// <param name="set">The columns to update and the values to set them to (column names = expression)</param>
    /// <returns></returns>
    public DeltaTable Update(string condition, IDictionary<string, Column> set) => Update(Functions.Expr(condition), set);
    
    /// <summary>
    /// Update rows that match `condition` in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">A SQL condition that can be parsed by `Functions.Expr`</param>
    /// <param name="set">The columns to update and the values to set them to (columns = expressions)</param>
    /// <returns></returns>
    public DeltaTable Update(string condition, IDictionary<Column, Column> set) => Update(Functions.Expr(condition), set);
    
    /// <summary>
    /// Update rows that match `condition` in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">A SQL condition that can be parsed by `Functions.Expr`</param>
    /// <param name="set">The columns to update and the values to set them to (column names = expression)</param>
    /// <returns></returns>
    public DeltaTable UpdateExpr(string condition, IDictionary<string, Column> set) => Update(Functions.Expr(condition), set);
    
    /// <summary>
    /// Update rows that match `condition` in the table based on the rules defined by `set`.
    /// </summary>
    /// <param name="condition">A SQL condition that can be parsed by `Functions.Expr`</param>
    /// <param name="set">The columns to update and the values to set them to (columns = expressions)</param>
    /// <returns></returns>
    public DeltaTable UpdateExpr(string condition, IDictionary<Column, Column> set) => Update(Functions.Expr(condition), set);
    
    /// <summary>
    /// Merge data from the source DataFrame based on the given merge condition. This returns a DeltaMergeBuilder object that can be used to specify the update, delete, or insert actions to be performed on rows based on whether the rows matched the condition or not.
    ///
    /// See https://docs.delta.io/latest/api/python/spark/index.html to understand how you can use the different merge types
    /// </summary>
    /// <param name="source">The data that will be merged into the delta table</param>
    /// <param name="expression">The Join expression between the source and the target</param>
    /// <returns></returns>
    public DeltaMergeBuilder Merge(DataFrame source, string expression) => Merge(source, Functions.Expr(expression));

    /// <summary>
    /// Merge data from the source DataFrame based on the given merge condition. This returns a DeltaMergeBuilder object that can be used to specify the update, delete, or insert actions to be performed on rows based on whether the rows matched the condition or not.
    ///
    /// See https://docs.delta.io/latest/api/python/spark/index.html to understand how you can use the different merge types
    /// </summary>
    /// <param name="source">The data that will be merged into the delta table</param>
    /// <param name="expression">The join expression between the source and the target, `source_df["id"] == Col("target.id")`</param>
    /// <returns></returns>
    public DeltaMergeBuilder Merge(DataFrame source, Column expression)
    {
        return new DeltaMergeBuilder(this, source, expression);
    }

    
    /// <summary>
    /// Return DeltaTableBuilder object that can be used to specify the table name, location, columns, partitioning columns, table comment, and table properties to create a Delta table, error if the table exists (the same as SQL CREATE TABLE).
    /// </summary>
    /// <param name="spark"></param>
    /// <returns></returns>
    public static DeltaTableBuilder Create(SparkSession spark)
    {
        return new DeltaTableBuilder(spark).Mode(CreateDeltaTable.Types.Mode.Create);
    }
    
    /// <summary>
    /// Return DeltaTableBuilder object that can be used to specify the table name, location, columns, partitioning columns, table comment, and table properties to create a Delta table, if it does not exists (the same as SQL CREATE TABLE IF NOT EXISTS).
    /// </summary>
    /// <param name="spark"></param>
    /// <returns></returns>
    public static DeltaTableBuilder CreateIfNotExist(SparkSession spark)
    {
        return new DeltaTableBuilder(spark).Mode(CreateDeltaTable.Types.Mode.CreateIfNotExists);
    }
    
    /// <summary>
    /// Return DeltaTableBuilder object that can be used to specify the table name, location, columns, partitioning columns, table comment, and table properties to replace a Delta table, error if the table doesn’t exist (the same as SQL REPLACE TABLE).
    /// </summary>
    /// <param name="spark"></param>
    /// <returns></returns>
    public static DeltaTableBuilder Replace(SparkSession spark)
    {
        return new DeltaTableBuilder(spark).Mode(CreateDeltaTable.Types.Mode.Replace);
    }    

    /// <summary>
    /// Return DeltaTableBuilder object that can be used to specify the table name, location, columns, partitioning columns, table comment, and table properties replace a Delta table, error if the table doesn’t exist (the same as SQL REPLACE TABLE).
    /// </summary>
    /// <param name="spark"></param>
    /// <returns></returns>
    public static DeltaTableBuilder CreateOrReplace(SparkSession spark)
    {
        return new DeltaTableBuilder(spark).Mode(CreateDeltaTable.Types.Mode.CreateOrReplace);
    }

    /// <summary>
    /// Check if the provided identifier string, in this case a file path, is the root of a Delta table using the given SparkSession.
    /// </summary>
    /// <param name="spark"></param>
    /// <param name="path"></param>
    /// <returns></returns>
    public static bool IsDeltaTable(SparkSession spark, string path)
    {
        var deltaRelation = new DeltaRelation()
        {
            IsDeltaTable = new IsDeltaTable()
            {
                Path = path
            }
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(deltaRelation)
        };

        var plan = new Plan()
        {
            Root = sparkRelation
        };
        
        var requestExecutor = new RequestExecutor(spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();

        var recordBatches = requestExecutor.GetArrowBatches();
        
        var boolArray = recordBatches.First().Column("value") as BooleanArray;
        var result = boolArray!.GetValue(0);
        return result!.Value;
    }

    /// <summary>
    /// Updates the protocol version of the table to leverage new features. Upgrading the reader version will prevent all clients that have an older version of Delta Lake from accessing this table. Upgrading the writer version will prevent older versions of Delta Lake to write to this table. The reader or writer version cannot be downgraded.
    /// See online documentation and Delta’s protocol specification at PROTOCOL.md for more details.
    /// </summary>
    /// <param name="readerVersion"></param>
    /// <param name="writerVersion"></param>
    public void UpgradeTableProtocol(int readerVersion, int writerVersion)
    {
        var command = new DeltaCommand()
        {
            UpgradeTableProtocol = new UpgradeTableProtocol()
            {
                ReaderVersion = readerVersion,
                WriterVersion = writerVersion,
                Table = _deltaTable
            }
        };

        var sparkCommand = new Command()
        {
            Extension = Any.Pack(command)
        };

        var plan = new Plan()
        {
            Command = sparkCommand

        };
        
        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
    }

    /// <summary>
    /// Restore the DeltaTable to an older version of the table specified by version number.
    /// </summary>
    /// <param name="version"></param>
    public DataFrame RestoreToVersion(int version)
    {
        var relation = new DeltaRelation()
        {
            RestoreTable = new RestoreTable()
            {
                Version = version, Table = _deltaTable
            }
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };

        var plan = new Plan()
        {
            Root = sparkRelation
        };
        
        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
        return new DataFrame(_spark, requestExecutor.GetRelation());
    }

    /// <summary>
    /// Restore the DeltaTable to an older version of the table specified by timestamp
    /// </summary>
    /// <param name="timestamp"></param>
    public DataFrame RestoreToTimestamp(DateTime timestamp)
    {
        var relation = new DeltaRelation()
        {
            RestoreTable = new RestoreTable()
            {
                Timestamp = timestamp.ToString("yyyy-MM-dd HH:mm:ss"), 
                Table = _deltaTable
            }
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };

        var plan = new Plan()
        {
            Root = sparkRelation
        };
        
        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
        return new DataFrame(_spark, requestExecutor.GetRelation());
    }
    
    /// <summary>
    /// Restore the DeltaTable to an older version of the table specified by timestamp
    /// </summary>
    /// <param name="timestamp"></param>
    public DataFrame RestoreToTimestamp(string timestamp)
    {
        var relation = new DeltaRelation()
        {
            RestoreTable = new RestoreTable()
            {
                Timestamp = timestamp, 
                Table = _deltaTable
            }
        };

        var sparkRelation = new Relation()
        {
            Extension = Any.Pack(relation)
        };

        var plan = new Plan()
        {
            Root = sparkRelation
        };
        
        var requestExecutor = new RequestExecutor(_spark, plan, ArrowHandling.ArrowBuffers);
        requestExecutor.Exec();
        return new DataFrame(_spark, requestExecutor.GetRelation());
    }
    
    /// <summary>
    /// Optimize the data layout of the table. This returns a DeltaOptimizeBuilder object that can be used to specify the partition filter to limit the scope of optimize and also execute different optimization techniques such as file compaction or order data using Z-Order curves.
    /// </summary>
    /// <returns></returns>
    public DeltaOptimizeBuilder Optimize() => new DeltaOptimizeBuilder(_spark, this);
}