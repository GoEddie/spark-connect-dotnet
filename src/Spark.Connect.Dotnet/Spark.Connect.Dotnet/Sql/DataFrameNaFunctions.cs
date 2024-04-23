namespace Spark.Connect.Dotnet.Sql;

public class DataFrameNaFunctions
{
    private readonly DataFrame _dataFrame;

    public DataFrameNaFunctions(DataFrame dataFrame)
    {
        _dataFrame = dataFrame;
    }

    public DataFrame Drop(string? how = null, int? thresh = null)
    {
        return _dataFrame.DropNa(how, thresh);
    }
    
    public DataFrame Drop(string? how = null, int? thresh = null, params string[] subset)
    {
        return _dataFrame.DropNa(how, thresh, subset);
    }
    public DataFrame Fill(Column value)
    {
        return _dataFrame.FillNa(value);
    }
    public DataFrame Fill(Column value, params string[] subset)
    {
        return _dataFrame.FillNa(value, subset);
    }

    public DataFrame Replace(Column toReplace, Column newValue)
    {
        return _dataFrame.Replace(toReplace, newValue);
    }
    
    public DataFrame Replace(Column toReplace, Column newValue, params string[] subset)
    {
        return _dataFrame.Replace(toReplace, newValue, subset);
    }
}