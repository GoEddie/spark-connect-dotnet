using System.Reflection;
using Spark.Connect.Dotnet.Grpc;
using Spark.Connect.Dotnet.Pipelines.Attributes;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.Pipelines;

/// <summary>
/// Finds classes with the `DeclarativePipelineAttribute` attribute and then iterates any methods that have one of the following attributes: `PipelineTableAttribute`
/// </summary>
public class PipelineRunner
{
    public IList<PipelineGraph> Run(SparkSession spark)
    {
        var assembly = Assembly.GetExecutingAssembly();
        return Run(assembly, spark);
    }
    
    public IList<PipelineGraph> Run(Assembly assembly, SparkSession spark)
    {
        var classes = assembly
            .GetTypes()
            .Where(type => type.GetCustomAttribute<DeclarativePipelineAttribute>() != null)
            .ToList();

        return Run(classes, spark);
    }

    private StructType GetSchemaForName(string name, Type type)
    {
        var schemas = type.GetMethods().Where(method => method.GetCustomAttribute<SchemaForAttribute>() != null);
        foreach (var schema in schemas)
        {
            var attribute = schema.GetCustomAttribute<SchemaForAttribute>();
            
            if (attribute.Name != null && attribute.Name == name)
            {
                return schema.Invoke(Activator.CreateInstance(type), null) as StructType;
            }
            
            if (attribute.Names != null && attribute.Names.Contains(name))
            {
                return schema.Invoke(Activator.CreateInstance(type), null) as StructType;
            }
        }

        return null;
    }
    
    private IDictionary<string, string> GetPropertiesForName(string tableName, Type type)
    {
        var properties = type.GetMethods().Where(method => method.GetCustomAttribute<TableOptionsForAttribute>() != null);
        foreach (var propFor in properties)
        {
            var attribute = propFor.GetCustomAttribute<TableOptionsForAttribute>();
            
            if (attribute.Name != null && attribute.Name == tableName)
            {
                return propFor.Invoke(Activator.CreateInstance(type), null) as IDictionary<string, string>;
            }
            
            if (attribute.Names != null && attribute.Names.Contains(tableName))
            {
                return propFor.Invoke(Activator.CreateInstance(type), null) as IDictionary<string, string>;
            }
        }

        return null;
    }
    
    private IDictionary<string, string> GetSqlConfForName(string tableName, Type type)
    {
        var confs = type.GetMethods().Where(method => method.GetCustomAttribute<SqlConfForAttribute>() != null);
        foreach (var propFor in confs)
        {
            var attribute = propFor.GetCustomAttribute<SqlConfForAttribute>();
            
            if (attribute.Name != null && attribute.Name == tableName)
            {
                return propFor.Invoke(Activator.CreateInstance(type), null) as IDictionary<string, string>;
            }
            
            if (attribute.Names != null && attribute.Names.Contains(tableName))
            {
                return propFor.Invoke(Activator.CreateInstance(type), null) as IDictionary<string, string>;
            }
        }

        return null;
    }
    
    public IList<PipelineGraph> Run(List<Type> classes, SparkSession spark)
    {
        var graphs = new List<PipelineGraph>();
        
        foreach (var type in classes)
        {
            var declarativePipelineAttribute = type.GetCustomAttribute<DeclarativePipelineAttribute>();
            var sqlConfsForPipeline = GetSqlConfForName(type.Name, type);
            
            var graph = new PipelineGraph(spark, declarativePipelineAttribute.DefaultCatalog, declarativePipelineAttribute.DefaultDatabase, sqlConfsForPipeline);
            var tables = type.GetMethods().Where(method => method.GetCustomAttribute<PipelineTableAttribute>() != null);
            
            foreach (var table in tables)
            {
                var tableAttribute = table.GetCustomAttribute<PipelineTableAttribute>();
                var schema = GetSchemaForName(tableAttribute.Name ?? table.Name, type);
               
                var properties = GetPropertiesForName(tableAttribute.Name ?? table.Name, type);
                var sqlConfs = GetSqlConfForName(tableAttribute.Name ?? table.Name, type);
                
                graph.AddTable(tableAttribute.Name ?? table.Name, table.Invoke(Activator.CreateInstance(type), [spark]) as Dotnet.Sql.DataFrame, schema, properties, sqlConfs, tableAttribute.Once, tableAttribute.Format, tableAttribute.Comment, tableAttribute.PartitionCols);
            }
            
            
            var matViews = type.GetMethods().Where(method => method.GetCustomAttribute<PipelineMaterializedViewAttribute>() != null);
            
            foreach (var view in matViews)
            {
                var tableAttribute = view.GetCustomAttribute<PipelineMaterializedViewAttribute>();
                var schema = GetSchemaForName(tableAttribute.Name ?? view.Name, type);
                
                
                var properties = GetPropertiesForName(tableAttribute.Name ?? view.Name, type);
                var sqlConfs = GetSqlConfForName(tableAttribute.Name ?? view.Name, type);

                graph.AddMaterializedView(tableAttribute.Name ?? view.Name, view.Invoke(Activator.CreateInstance(type), [spark]) as Dotnet.Sql.DataFrame, schema, properties, sqlConfs, tableAttribute.Once, tableAttribute.Format, tableAttribute.Comment, tableAttribute.PartitionCols);
            }
            
            
            var tempViews = type.GetMethods().Where(method => method.GetCustomAttribute<PipelineTemporaryViewAttribute>() != null);
            
            foreach (var view in tempViews)
            {
                var tableAttribute = view.GetCustomAttribute<PipelineTemporaryViewAttribute>();
                var schema = GetSchemaForName(tableAttribute.Name ?? view.Name, type);
                
                var properties = GetPropertiesForName(tableAttribute.Name ?? view.Name, type);
                var sqlConfs = GetSqlConfForName(tableAttribute.Name ?? view.Name, type);
                
                graph.AddTemporaryView(tableAttribute.Name ?? view.Name, view.Invoke(Activator.CreateInstance(type), [spark]) as Dotnet.Sql.DataFrame, schema, properties, sqlConfs, tableAttribute.Once, tableAttribute.Format, tableAttribute.Comment, tableAttribute.PartitionCols);
            }
            
            graph.StartRun();
            graphs.Add(graph);
        }
        
        
        return graphs;
    }

    
}