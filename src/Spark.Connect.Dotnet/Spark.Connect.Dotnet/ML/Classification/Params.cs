using System.Text;
using Spark.Connect.Dotnet.ML.Param;

namespace Spark.Connect.Dotnet.ML.Classification;

public class Params(ParamMap defaultParams)
{
    public ParamMap ParamMap { get; set; } = defaultParams;

    public string ExplainParams()
    {
        var stringBuilder = new StringBuilder();
        
        foreach (var p in ParamMap.GetAll())
        {
            stringBuilder.AppendLine($"{p.Name} = {p.Value}");    
        }
        
        return stringBuilder.ToString();
    }

    public Param.Param? GetDefault(string name)
    {
        return ParamMap.GetDefault(name);
    }
}