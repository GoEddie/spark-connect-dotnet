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
            var tables = type.GetMethods().Where(method => method.GetCustomAttribute<StreamingTableAttribute>() != null);

            foreach (var table in tables)
            {
                var tableAttribute = table.GetCustomAttribute<StreamingTableAttribute>();
                var tableName = tableAttribute.Name ?? table.Name;
                var schema = GetSchemaForName(tableName, type);
                var properties = GetPropertiesForName(tableName, type);
                var sqlConfs = GetSqlConfForName(tableName, type);

                // Get source code location from the method
                var sourceFileName = table.DeclaringType?.Assembly.Location;
                var sourceDefinitionPath = table.DeclaringType?.FullName;

                graph.AddTable(
                    tableName: tableName,
                    source: table.Invoke(Activator.CreateInstance(type), [spark]) as Dotnet.Sql.DataFrame,
                    schema: schema,
                    options: properties,
                    sqlConfs: sqlConfs,
                    format: tableAttribute.Format,
                    comment: tableAttribute.Comment,
                    partitionCols: tableAttribute.PartitionCols,
                    clusteringColumns: tableAttribute.ClusteringColumns,
                    once: tableAttribute.Once ? true : null,
                    sourceCodeFileName: sourceFileName,
                    sourceCodeDefinitionPath: sourceDefinitionPath);
            }

            var matViews = type.GetMethods().Where(method => method.GetCustomAttribute<MaterializedViewAttribute>() != null);

            foreach (var view in matViews)
            {
                var viewAttribute = view.GetCustomAttribute<MaterializedViewAttribute>();
                var viewName = viewAttribute.Name ?? view.Name;
                var schema = GetSchemaForName(viewName, type);
                var properties = GetPropertiesForName(viewName, type);
                var sqlConfs = GetSqlConfForName(viewName, type);

                // Get source code location from the method
                var sourceFileName = view.DeclaringType?.Assembly.Location;
                var sourceDefinitionPath = view.DeclaringType?.FullName;

                graph.AddMaterializedView(
                    viewName: viewName,
                    source: view.Invoke(Activator.CreateInstance(type), [spark]) as Dotnet.Sql.DataFrame,
                    schema: schema,
                    options: properties,
                    sqlConfs: sqlConfs,
                    format: viewAttribute.Format,
                    comment: viewAttribute.Comment,
                    partitionCols: viewAttribute.PartitionCols,
                    clusteringColumns: viewAttribute.ClusteringColumns,
                    once: viewAttribute.Once ? true : null,
                    sourceCodeFileName: sourceFileName,
                    sourceCodeDefinitionPath: sourceDefinitionPath);
            }

            var tempViews = type.GetMethods().Where(method => method.GetCustomAttribute<TemporaryViewAttribute>() != null);

            foreach (var view in tempViews)
            {
                var viewAttribute = view.GetCustomAttribute<TemporaryViewAttribute>();
                var viewName = viewAttribute.Name ?? view.Name;
                var sqlConfs = GetSqlConfForName(viewName, type);

                // Get source code location from the method
                var sourceFileName = view.DeclaringType?.Assembly.Location;
                var sourceDefinitionPath = view.DeclaringType?.FullName;

                graph.AddTemporaryView(
                    viewName,
                    view.Invoke(Activator.CreateInstance(type), [spark]) as Dotnet.Sql.DataFrame,
                    comment: viewAttribute.Comment,
                    sqlConfs: sqlConfs,
                    once: viewAttribute.Once ? true : null,
                    sourceCodeFileName: sourceFileName,
                    sourceCodeDefinitionPath: sourceDefinitionPath);
            }

            var sinks = type.GetMethods().Where(method => method.GetCustomAttribute<SinkAttribute>() != null);

            foreach (var sink in sinks)
            {
                var sinkAttribute = sink.GetCustomAttribute<SinkAttribute>();
                var sinkName = sinkAttribute.Name ?? sink.Name;
                var properties = GetPropertiesForName(sinkName, type);
                var sqlConfs = GetSqlConfForName(sinkName, type);

                // Get source code location from the method
                var sourceFileName = sink.DeclaringType?.Assembly.Location;
                var sourceDefinitionPath = sink.DeclaringType?.FullName;

                graph.AddSink(
                    sinkName,
                    sink.Invoke(Activator.CreateInstance(type), [spark]) as Dotnet.Sql.DataFrame,
                    options: properties,
                    sqlConfs: sqlConfs,
                    format: sinkAttribute.Format,
                    comment: sinkAttribute.Comment,
                    once: sinkAttribute.Once ? true : null,
                    sourceCodeFileName: sourceFileName,
                    sourceCodeDefinitionPath: sourceDefinitionPath);
            }

            graph.StartRun(storage: declarativePipelineAttribute?.Storage);
            graphs.Add(graph);
        }

        return graphs;
    }
}