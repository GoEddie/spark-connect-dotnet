using System.Runtime.InteropServices;
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
        plan.Root.Catalog.CacheTable = new CacheTable()
        {
            StorageLevel = storageLevel,
            TableName = tableName
        };

        GrpcInternal.Exec(_sparkSession, plan);
    }
    
    public void ClearCache()
    {
        var plan = Plan();
        plan.Root.Catalog.ClearCache = new ClearCache()
        {

        };

        GrpcInternal.Exec(_sparkSession, plan);
    }
    
    public DataFrame CreateExternalTable(string tableName, string path, string source = "", StructType? schema = null, IEnumerable<MapField<string, string>>? options = null)
    {
        var plan = Plan();
        plan.Root.Catalog.CreateExternalTable = new CreateExternalTable()
        {
            TableName = tableName,
            Path = path
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

        return new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan));
    }

    public DataFrame CreateTable(string tableName, string? path = null, string? source = null,
        StructType? schema = null, string? description = null, Dictionary<string, string>? options = null)
    {
        var plan = Plan();
        plan.Root.Catalog.CreateTable = new CreateTable()
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
        
        return new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan));
        
    }

    public string CurrentCatalog()
    {
        var plan = Plan();
        plan.Root.Catalog.CurrentCatalog = new CurrentCatalog();
        return new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect()[0][0].ToString();
    }
    
    public string CurrentDatabase()
    {
        var plan = Plan();
        plan.Root.Catalog.CurrentDatabase = new CurrentDatabase();
        return new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect()[0][0].ToString();
    }
    
    public bool DatabaseExists(string dbName)
    {
        var plan = Plan();
        plan.Root.Catalog.DatabaseExists = new DatabaseExists()
        {
            DbName = dbName
        };
        return (bool)new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect()[0][0];
    }
    
    public bool DropGlobalTempView(string viewName)
    {
        var plan = Plan();
        plan.Root.Catalog.DropGlobalTempView = new DropGlobalTempView()
        {
            ViewName = viewName
        };
        
        return (bool)new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect()[0][0];
    }
    
    public bool DropTempView(string viewName)
    {
        var plan = Plan();
        plan.Root.Catalog.DropTempView = new DropTempView()
        {
            ViewName = viewName
        };
        
        return (bool)new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect()[0][0];
    }
    
    public bool FunctionExists(string functionName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.FunctionExists = new FunctionExists()
        {
            FunctionName = functionName
        };

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.FunctionExists.DbName = dbName;
        }
        
        return (bool)new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect()[0][0];
    }
    
    public Database GetDatabase(string dbName)
    {
        var plan = Plan();
        plan.Root.Catalog.GetDatabase = new GetDatabase()
        {
            DbName = dbName
        };
        
        var row = new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect().First();
        return new Database((string)row[0], (string)row[1], (string)row[2], (string)row[3]);
    }
    
    public Function GetFunction(string functionName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.GetFunction = new GetFunction()
        {
            FunctionName = functionName
        };

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.GetFunction.DbName = dbName;
        }
        
        var row = new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect().First();
        return new Function((string)row[0], (string)row[1], (string[])row[2], (string)row[3], (string)row[4],(bool)row[5]);
    }
    
    public Table GetTable(string functionName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.GetTable = new GetTable()
        {
            TableName = functionName
        };

        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.GetTable.DbName = dbName;
        }
        
        var row = new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect().First();
        return new Table((string)row[0], (string)row[1], (string)row[2], (string)row[3], (string)row[4],(bool)row[5]);
    }
    
    public bool IsCached(string tableName)
    {
        var plan = Plan();
        plan.Root.Catalog.IsCached = new IsCached()
        {
           TableName = tableName
        };
        
        return (bool)new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect()[0][0];
    }
    
    public List<CatalogMetadata> ListCatalogs(string? patternName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.ListCatalogs = new ListCatalogs();
        if (!string.IsNullOrEmpty(patternName))
        {
            plan.Root.Catalog.ListCatalogs.Pattern = patternName;
        }

        var catalogs = new List<CatalogMetadata>();
        var result = new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect();
        foreach (var catalog in result)
        {
            catalogs.Add(new CatalogMetadata((string)catalog[0], (string)catalog[1]));
        }

        return catalogs;
    }
    
    public List<SparkCatalog.Column> ListColumns(string tableName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.ListColumns = new ListColumns()
        {
            TableName = tableName
        };
        if (!string.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.ListColumns.DbName = dbName;
        }

        var columns = new List<Column>();
        var result = new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect();
        foreach (var column in result)
        {
            columns.Add(new Column((string)column[0], (string)column[1], (string)column[2], (bool)column[3], (bool)column[4], (bool)column[5]));
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

        var databases = new List<Database>();
        var result = new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect();
        foreach (var database in result)
        {
            databases.Add(new Database((string)database[0], (string)database[1], (string)database[2], (string)database[3])); }

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
        
        var functions = new List<Function>();
        var result = new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect();
        foreach (var function in result)
        {
            functions.Add(new Function((string)function[0], (string)function[1], (string[])function[2], (string)function[3], (string)function[4], (bool)function[5]));
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
        var result = new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect();
        foreach (var table in result)
        {
            tables.Add(new Table((string)table[0], (string)table[1], (string)table[2], (string)table[3], (string)table[4], (bool)table[5]));
        }

        return tables;
    }

    public void RecoverPartitions(string tableName)
    {
        var plan = Plan();
        plan.Root.Catalog.RecoverPartitions = new RecoverPartitions()
        {
            TableName = tableName
        };

        GrpcInternal.Exec(_sparkSession, plan);
    }
    
    public void RefreshByPath(string path)
    {
        var plan = Plan();
        plan.Root.Catalog.RefreshByPath = new RefreshByPath()
        {
            Path = path
        };

        GrpcInternal.Exec(_sparkSession, plan);
    }
    
    public void RefreshTable(string tableName)
    {
        var plan = Plan();
        plan.Root.Catalog.RefreshTable = new RefreshTable()
        {
           TableName = tableName
        };

        GrpcInternal.Exec(_sparkSession, plan);
    }

    public void SetCurrentCatalog(string catalogName)
    {
        var plan = Plan();
        plan.Root.Catalog.SetCurrentCatalog = new SetCurrentCatalog()
        {
            CatalogName = catalogName
        };

        GrpcInternal.Exec(_sparkSession, plan);
    }
    
    public void SetCurrentDatabase(string dbName)
    {
        var plan = Plan();
        plan.Root.Catalog.SetCurrentDatabase = new SetCurrentDatabase()
        {
            DbName = dbName
        };

        GrpcInternal.Exec(_sparkSession, plan);
    }
    
    public bool TableExists(string tableName, string? dbName = null)
    {
        var plan = Plan();
        plan.Root.Catalog.TableExists = new TableExists()
        {
            TableName = tableName
        };

        if (!String.IsNullOrEmpty(dbName))
        {
            plan.Root.Catalog.TableExists.DbName = dbName;
        }
        
        return (bool)new DataFrame(_sparkSession, GrpcInternal.Exec(_sparkSession, plan)).Collect()[0][0];
    }
    
    public void UncacheTable(string tableName)
    {
        var plan = Plan();
        plan.Root.Catalog.UncacheTable = new UncacheTable()
        {
            TableName = tableName
        };

        GrpcInternal.Exec(_sparkSession, plan);
    }
    
    private Plan Plan() => new Plan()
    {
        Root = new Relation()
        {
            Catalog = new Catalog()
            {

            },
            Common = new RelationCommon()
            {
                PlanId     = _sparkSession.GetPlanId()
            }
        }
    };

    public record Database(string name, string catalog, string description, string locationUri);
    public record Function(string name, string catalog, string[] namesSpace, string description, string className, bool isTemporary);
    public record Table(string name, string catalog, string nameSpace, string description, string tableType, bool isTemporary);
    public record CatalogMetadata(string name, string description);
    public record Column(string name, string description, string dataType, bool nullable, bool isPartition, bool isBucket);
 
}

