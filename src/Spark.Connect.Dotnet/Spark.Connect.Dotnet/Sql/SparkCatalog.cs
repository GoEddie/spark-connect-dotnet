using System.Text;
using Apache.Arrow;
using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Sql;

public class SparkCatalog
{
    private readonly SparkSession _sparkSession;

    public SparkCatalog(SparkSession sparkSession)
    {
        _sparkSession = sparkSession;
    }

    public void CacheTable(string tableName, StorageLevel storageLevel)
    {
        var plan = Plan();
        plan.Root.Catalog.CacheTable = new CacheTable
        {
            StorageLevel = storageLevel, TableName = tableName
        };

        var executor = new RequestExecutor(_sparkSession, plan);
        Task.Run(() => executor.ExecAsync()).Wait();
    }

    public void ClearCache()
    {
        var plan = Plan();
        plan.Root.Catalog.ClearCache = new ClearCache();
        
        var executor = new RequestExecutor(_sparkSession, plan);
        var task = Task.Run(() => executor.ExecAsync());
        task.Wait();
    }

    public DataFrame CreateExternalTable(string tableName, string path, string source = "", StructType? schema = null,
        IEnumerable<MapField<string, string>>? options = null)
    {
        var plan = Plan();
        plan.Root.Catalog.CreateExternalTable = new CreateExternalTable
        {
            TableName = tableName, Path = path
        };

        if (!string.IsNullOrEmpty(source))
        {
            plan.Root.Catalog.CreateExternalTable.Source = source;
        }

        if (schema != null)
        {
            plan.Root.Catalog.CreateExternalTable.Schema = schema.ToDataType();
        }

        if (options != null && options.Any())
        {
            foreach (var option in options)
            {
                plan.Root.Catalog.CreateExternalTable.Options.Add(option);
            }
        }

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
        return new DataFrame(_sparkSession, executor.GetRelation());
    }

    public DataFrame CreateTable(string tableName, string? path = null, string? source = null,
        StructType? schema = null, string? description = null, Dictionary<string, string>? options = null)
    {
        var plan = Plan();
        plan.Root.Catalog.CreateTable = new CreateTable
        {
            TableName = tableName
        };

        if (!string.IsNullOrEmpty(path))
        {
            plan.Root.Catalog.CreateTable.Path = path;
        }

        if (!string.IsNullOrEmpty(source))
        {
            plan.Root.Catalog.CreateTable.Source = source;
        }

        if (schema != null)
        {
            plan.Root.Catalog.CreateTable.Schema = schema.ToDataType();
        }

        if (!string.IsNullOrEmpty(description))
        {
            plan.Root.Catalog.CreateTable.Description = description;
        }

        foreach (var option in options)
        {
            plan.Root.Catalog.CreateTable.Options.Add(option.Key, option.Value);
        }

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
        return new DataFrame(_sparkSession, executor.GetRelation());
    }

    public string CurrentCatalog()
    {
        var plan = Plan();
        plan.Root.Catalog.CurrentCatalog = new CurrentCatalog();
        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var recordBatches = executor.GetArrowBatches();
        var builder = new StringBuilder();
        foreach (var recordBatch in recordBatches)
        {
            builder.Append((recordBatch.Column("value") as StringArray).GetString(0));
        }
        
        return builder.ToString(); 
    }

    public string CurrentDatabase()
    {
        var plan = Plan();
        plan.Root.Catalog.CurrentDatabase = new CurrentDatabase();
        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var recordBatches = executor.GetArrowBatches();
        var builder = new StringBuilder();
        foreach (var recordBatch in recordBatches)
        {
            builder.Append((recordBatch.Column("value") as StringArray).GetString(0));
        }
        
        return builder.ToString(); 
    }

    public bool DatabaseExists(string dbName)
    {
        var plan = Plan();
        plan.Root.Catalog.DatabaseExists = new DatabaseExists
        {
            DbName = dbName
        };
        
        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var recordBatches = executor.GetArrowBatches();
        return recordBatches.Any(p => (p.Column("value") as BooleanArray).GetValue(0)!.Value);
    }

    public bool DropGlobalTempView(string viewName)
    {
        var plan = Plan();
        plan.Root.Catalog.DropGlobalTempView = new DropGlobalTempView
        {
            ViewName = viewName
        };

        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var recordBatches = executor.GetArrowBatches();
        return recordBatches.Any(p => (p.Column("value") as BooleanArray).GetValue(0)!.Value);
    }

    public bool DropTempView(string viewName)
    {
        var plan = Plan();
        plan.Root.Catalog.DropTempView = new DropTempView
        {
            ViewName = viewName
        };

        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var recordBatches = executor.GetArrowBatches();
        return recordBatches.Any(p => (p.Column("value") as BooleanArray).GetValue(0)!.Value);
    }

    public bool FunctionExists(string functionName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.FunctionExists = new FunctionExists
        {
            FunctionName = functionName
        };

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.FunctionExists.DbName = dbName;
        }

        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var recordBatches = executor.GetArrowBatches();
        return recordBatches.Any(p => (p.Column("value") as BooleanArray).GetValue(0)!.Value);
    }

    public Database GetDatabase(string dbName)
    {
        var plan = Plan();
        plan.Root.Catalog.GetDatabase = new GetDatabase
        {
            DbName = dbName
        };

        var executor = new RequestExecutor(_sparkSession, plan,  ArrowHandling.ArrowBuffers);
        executor.Exec();
        
        var recordBatches = executor.GetArrowBatches();
        var firstBatch = recordBatches.First();
        var nameArray = firstBatch.Column("name") as StringArray;
        var catalogArray = firstBatch.Column("catalog") as StringArray;
        var descriptionArray = firstBatch.Column("description") as StringArray;
        var locationArray = firstBatch.Column("locationUri") as StringArray;
        
        return new Database(nameArray.GetString(0), catalogArray.GetString(0), descriptionArray.GetString(0), locationArray.GetString(0));
    }

    public Function GetFunction(string functionName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.GetFunction = new GetFunction
        {
            FunctionName = functionName
        };

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.GetFunction.DbName = dbName;
        }

        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var recordBatches = executor.GetArrowBatches();
        var firstBatch = recordBatches.First();
        var nameArray = firstBatch.Column("name") as StringArray;
        var catalogArray = firstBatch.Column("catalog") as StringArray;
        var namespaceArray = firstBatch.Column("namespace") as ListArray;
        var descriptionArray = firstBatch.Column("description") as StringArray;
        var classNameArray = firstBatch.Column("className") as StringArray;
        var isTemporaryArray = firstBatch.Column("isTemporary") as BooleanArray;
        
        var namespaces = new List<string>();
        var namespaceValuesArray = namespaceArray.Values as StringArray;
        if (namespaceArray != null)
        {
            for (var i = 0; i < namespaceValuesArray.Length; i++)
            {
                namespaces.Add(namespaceValuesArray.GetString(i));
            }
        }
        
        return new Function(
            nameArray.GetString(0), 
            catalogArray.GetString(0), 
            namespaces.ToArray(), 
            descriptionArray.GetString(0), 
            classNameArray.GetString(0), 
            isTemporaryArray.GetValue(0)!.Value);
    }

    public Table GetTable(string functionName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.GetTable = new GetTable
        {
            TableName = functionName
        };

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.GetTable.DbName = dbName;
        }

        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();
        
        
        var recordBatches = executor.GetArrowBatches();
        var firstBatch = recordBatches.First();
        var nameArray = firstBatch.Column("name") as StringArray;
        var catalogArray = firstBatch.Column("catalog") as StringArray;
        var namespaceArray = firstBatch.Column("namespace") as ListArray;
        var descriptionArray = firstBatch.Column("description") as StringArray;
        var tableTypeArray = firstBatch.Column("tableType") as StringArray;
        var isTemporaryArray = firstBatch.Column("isTemporary") as BooleanArray;

        var namespaces = new List<string>();
        if (namespaceArray.Values is StringArray namespaceValuesArray)
        {
            for (var i = 0; i < namespaceValuesArray.Length; i++)
            {
                namespaces.Add(namespaceValuesArray.GetString(i));
            }
        }
        
        return new Table(
            nameArray.GetString(0), 
            catalogArray.GetString(0), 
            namespaces.ToArray(), 
            descriptionArray.GetString(0), 
            tableTypeArray.GetString(0), 
            isTemporaryArray.GetValue(0)!.Value);
    }

    public bool IsCached(string tableName)
    {
        var plan = Plan();
        plan.Root.Catalog.IsCached = new IsCached
        {
            TableName = tableName
        };

        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();
        
        var recordBatch = executor.GetArrowBatches();
        var firstBatch = recordBatch.First();
        var valuesArray = firstBatch.Column("value") as BooleanArray;
        return valuesArray.GetValue(0)!.Value;
    }

    public List<CatalogMetadata> ListCatalogs(string? patternName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.ListCatalogs = new ListCatalogs();
        if (!string.IsNullOrEmpty(patternName))
        {
            plan.Root.Catalog.ListCatalogs.Pattern = patternName;
        }
        
        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var result = executor.GetArrowBatches();
        
        var catalogs = new List<CatalogMetadata>();
        
        foreach (var batch in result)
        {
            for (var row = 0; row < batch.Length; row++)
            {
                var nameArray = batch.Column("name") as StringArray;
                var descriptionArray = batch.Column("description") as StringArray;
        
                catalogs.Add(new CatalogMetadata(
                    nameArray.GetString(row), 
                    descriptionArray.GetString(row)
                ));
            }
        }
        
        return catalogs;
    }

    public List<Column> ListColumns(string tableName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.ListColumns = new ListColumns
        {
            TableName = tableName
        };
        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.ListColumns.DbName = dbName;
        }
        
        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var result = executor.GetArrowBatches();
        
        var columns = new List<Column>();
        
        foreach (var batch in result)
        {
            for (var row = 0; row < batch.Length; row++)
            {
                var nameArray = batch.Column("name") as StringArray;
                var descriptionArray = batch.Column("description") as StringArray;
                var dataTypeArray = batch.Column("dataType") as StringArray;
                var nullableArray = batch.Column("nullable") as BooleanArray;
                var isPartitionArray = batch.Column("isPartition") as BooleanArray;
                var isBucketArray = batch.Column("isBucket") as BooleanArray;
        
                columns.Add(new Column(
                    nameArray.GetString(row), 
                    descriptionArray.GetString(row),
                    dataTypeArray.GetString(row),
                    nullableArray.GetValue(row)!.Value,
                    isPartitionArray.GetValue(row)!.Value,
                    isBucketArray.GetValue(row)!.Value
                ));
            }
        }
        
        return columns;
    }

    public List<Database> ListDatabases(string? patternName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.ListDatabases = new ListDatabases();
        if (!string.IsNullOrEmpty(patternName))
        {
            plan.Root.Catalog.ListDatabases.Pattern = patternName;
        }
        
        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var result = executor.GetArrowBatches();
        
        var databases = new List<Database>();
       
        foreach (var batch in result)
        {
            for (var row = 0; row < batch.Length; row++)
            {
                var nameArray = batch.Column("name") as StringArray;
                var catalogArray = batch.Column("catalog") as StringArray;
                var descriptionArray = batch.Column("description") as StringArray;
                var locationUriArray = batch.Column("locationUri") as StringArray;
        
                databases.Add(new Database(
                    nameArray.GetString(row), 
                    catalogArray.GetString(row),
                    descriptionArray.GetString(row),
                    locationUriArray.GetString(row)
                  ));
            }
        }
        
        return databases;
    }

    public List<Function> ListFunctions(string? dbName = null, string? patternName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.ListFunctions = new ListFunctions();
        if (!string.IsNullOrEmpty(patternName))
        {
            plan.Root.Catalog.ListFunctions.Pattern = patternName;
        }

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.ListFunctions.DbName = dbName;
        }
        
        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var result = executor.GetArrowBatches();
        
        var functions = new List<Function>();
        
        foreach (var batch in result)
        {
            for (var row = 0; row < batch.Length; row++)
            {
                var nameArray = batch.Column("name") as StringArray;
                var catalogArray = batch.Column("catalog") as StringArray;
                var namespaceArray = batch.Column("namespace") as ListArray;
                var descriptionArray = batch.Column("description") as StringArray;
                var  classNameArray = batch.Column("className") as StringArray;
                var isTemporaryArray = batch.Column("isTemporary") as BooleanArray;

                var namespaces = new List<string>();
                if (namespaceArray.Values is StringArray namespaceValuesArray)
                {   //TODO - is this right? If one item has multiple namespaces then we will
                    //  return the first namespace for one item then its next namespace will be for the next row?
                    
                    for (var i = 0; i < namespaceValuesArray.Length; i++)
                    {
                        namespaces.Add(namespaceValuesArray.GetString(i));
                    }
                }
        
                functions.Add(new Function(
                    nameArray.GetString(row), 
                    catalogArray.GetString(row), 
                    namespaces.ToArray(), 
                    descriptionArray.GetString(row), 
                    classNameArray.GetString(row), 
                    isTemporaryArray.GetValue(row)!.Value));
            }
        }
        
        return functions;
    }

    public List<Table> ListTables(string? dbName = null, string? patternName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.ListTables = new ListTables();
        if (!string.IsNullOrEmpty(patternName))
        {
            plan.Root.Catalog.ListTables.Pattern = patternName;
        }

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.ListTables.DbName = dbName;
        }

        var tables = new List<Table>();
        
        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();

        var result = executor.GetArrowBatches();
        foreach (var batch in result)
        {
            for (var row = 0; row < batch.Length; row++)
            {
                var nameArray = batch.Column("name") as StringArray;
                var catalogArray = batch.Column("catalog") as StringArray;
                var namespaceArray = batch.Column("namespace") as ListArray;
                var descriptionArray = batch.Column("description") as StringArray;
                var tableTypeArray = batch.Column("tableType") as StringArray;
                var isTemporaryArray = batch.Column("isTemporary") as BooleanArray;

                var namespaces = new List<string>();
                if (namespaceArray.Values is StringArray namespaceValuesArray)
                {   //TODO - is this right? If one item has multiple namespaces then we will
                    //  return the first namespace for one item then its next namespace will be for the next row?
                    
                    for (var i = 0; i < namespaceValuesArray.Length; i++)
                    {
                        namespaces.Add(namespaceValuesArray.GetString(i));
                    }
                }
        
                tables.Add(new Table(
                    nameArray.GetString(row), 
                    catalogArray.GetString(row), 
                    namespaces.ToArray(), 
                    descriptionArray.GetString(row), 
                    tableTypeArray.GetString(row), 
                    isTemporaryArray.GetValue(row)!.Value));
            }
        }
        
        return tables;
    }

    public void RecoverPartitions(string tableName)
    {
        var plan = Plan();
        plan.Root.Catalog.RecoverPartitions = new RecoverPartitions
        {
            TableName = tableName
        };

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
    }

    public void RefreshByPath(string path)
    {
        var plan = Plan();
        plan.Root.Catalog.RefreshByPath = new RefreshByPath
        {
            Path = path
        };

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
    }

    public void RefreshTable(string tableName)
    {
        var plan = Plan();
        plan.Root.Catalog.RefreshTable = new RefreshTable
        {
            TableName = tableName
        };

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
    }

    public void SetCurrentCatalog(string catalogName)
    {
        var plan = Plan();
        plan.Root.Catalog.SetCurrentCatalog = new SetCurrentCatalog
        {
            CatalogName = catalogName
        };

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
    }

    public void SetCurrentDatabase(string dbName)
    {
        var plan = Plan();
        plan.Root.Catalog.SetCurrentDatabase = new SetCurrentDatabase
        {
            DbName = dbName
        };

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
    }

    public bool TableExists(string tableName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.TableExists = new TableExists
        {
            TableName = tableName
        };

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.TableExists.DbName = dbName;
        }

        var executor = new RequestExecutor(_sparkSession, plan, ArrowHandling.ArrowBuffers);
        executor.Exec();
        
        var recordBatch = executor.GetArrowBatches();
        var items = recordBatch.First().Column("value") as BooleanArray;
        
        return items.GetValue(0)!.Value;
    }

    public void UncacheTable(string tableName)
    {
        var plan = Plan();
        plan.Root.Catalog.UncacheTable = new UncacheTable
        {
            TableName = tableName
        };

        var executor = new RequestExecutor(_sparkSession, plan);
        executor.Exec();
    }

    private Plan Plan()
    {
        return new Plan
        {
            Root = new Relation
            {
                Catalog = new Catalog(), Common = new RelationCommon
                {
                    PlanId = _sparkSession.GetPlanId()
                }
            }
        };
    }

    public record Database(string name, string catalog, string description, string locationUri);

    public record Function(
        string name
        , string catalog
        , string[] namesSpace
        , string description
        , string className
        , bool isTemporary);

    public record Table(
        string name
        , string catalog
        , string[] nameSpace
        , string description
        , string tableType
        , bool isTemporary);

    public record CatalogMetadata(string name, string description);

    public record Column(
        string name
        , string description
        , string dataType
        , bool nullable
        , bool isPartition
        , bool isBucket);
}