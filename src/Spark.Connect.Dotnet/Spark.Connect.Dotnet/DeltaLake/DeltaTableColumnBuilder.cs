using Delta.Connect;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.DeltaLake;

public class DeltaTableColumnBuilder
{
    private string _name;
    private string _dataType;
    private string _comment;
    private bool _nullable;
    private string _generatingExpression;
    private bool _allowExplicitInsert = false;
    
    public DeltaTableColumnBuilder(string name, string dataType = "", string comment = "", bool nullable = true, string generatingExpression = "")
    {
        _name = name;
        _dataType = dataType; 
        _comment = comment;
        _nullable = nullable;
        _generatingExpression = generatingExpression;
    }

    public DeltaTableColumnBuilder AllowExplicitInsert(bool allowExplicitInsert)
    {
        _allowExplicitInsert = allowExplicitInsert;
        return this;
    }
    
    public DeltaTableColumnBuilder DataType(string dataType)
    {
        _dataType = dataType;
        return this;
    }
    
    public DeltaTableColumnBuilder Comment(string comment)
    {
        _comment = comment;
        return this;
    }
    
    public DeltaTableColumnBuilder Nullable(bool nullable)
    {
        _nullable = nullable;
        return this;
    }

    public DeltaTableColumnBuilder GeneratedAlwaysAs(string constraint)
    {
        _generatingExpression = constraint;
        return this;
    }

    public CreateDeltaTable.Types.Column Build()
    {
       
        var column = new CreateDeltaTable.Types.Column()
        {
            Name = _name,
            DataType = SparkDataType.FromString(_dataType).ToDataType()
        };

        if (!string.IsNullOrEmpty(_comment))
        {
            column.Comment = _comment;
        }

        if (_nullable)
        {
            column.Nullable = true;
        }

        if (!string.IsNullOrEmpty(_generatingExpression))
        {
            column.GeneratedAlwaysAs = _generatingExpression;
        }
        
        return column;
    }
}