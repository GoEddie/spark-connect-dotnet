using System.Xml;
using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Sql;

namespace Spark.Connect.Dotnet.ML.Param;

public class Param(string name, dynamic value)
{
    public string Name { get; } = name;
    public dynamic Value { get; } = value;
}

public class ParamMap
{
    public ParamMap(List<Param> defaultParams)
    {
        _defaultParams = defaultParams;
    }
    
    private List<Param> _defaultParams { get; } = new List<Param>();
    private List<Param> _setParams { get; } = new List<Param>();

    public void Add(string name, dynamic value)
    {
        Add(new Param(name, value));
    }

    public Param? GetDefault(string name)
    {
        return _defaultParams.FirstOrDefault(p => p.Name == name);
    }
    
    public Param? GetDefault(Param param)
    {
        return _defaultParams.FirstOrDefault(p => p.Name == param.Name);
    }

    public bool IsDefined(Param param) => IsDefined(param.Name);
    
    public bool IsDefined(string name)
    {
        return _defaultParams.Any(p => p.Name == name) || _setParams.Any(p => p.Name == name);
    }

    public bool IsSet(Param param) => IsSet(param.Name);
    
    public bool IsSet(string name)
    {
        return _setParams.Any(p => p.Name == name);
    }
    
    public void Add(Param param)
    {
        var defaultParam = GetDefault(param.Name);
        
        if (defaultParam == null)
        {   // I think this will fail later on when they try to use it
            _setParams.Add(param);
            return;
        }
        
        var alreadySet = _setParams.FirstOrDefault(p => p.Name == param.Name);
        
        if (param.Value == defaultParam.Value)
        {
            if (alreadySet != null)
            {
                _setParams.Remove(alreadySet);
            }
        }
        else
        {
            if (alreadySet != null)
            {
                _setParams.Remove(alreadySet);
            }
            
            _setParams.Add(param);
        }
    }

    public Param? Get(string name)
    {
        if (_setParams.Any(p => p.Name == name))
        {
            return _setParams.First(p => p.Name == name);
        }

        if (_defaultParams.Any(p => p.Name == name))
        {
            return _defaultParams.First(p => p.Name == name);
        }

        return null;
    }

    public IList<Param> GetAll()
    {
        var paramDict = _defaultParams.ToDictionary(p => p.Name);
        foreach (var param in _setParams)
        {
            paramDict[param.Name] = param;
        }
        
        return paramDict.Values.ToList();
    }

    public ParamMap Clone()
    {
        var newMap = new ParamMap(_defaultParams);
        foreach (var param in _setParams)
        {
            newMap.Add(param);
        }
        return newMap;
    }

    public ParamMap Update(Dictionary<string, dynamic> updates)
    {
        foreach (var update in updates)
        {
            this.Add(update.Key, update.Value);
        }

        return this;
    }

    public IDictionary<string,Expression.Types.Literal> ToMapField()
    {
        var dict = new Dictionary<string, Expression.Types.Literal>();
        foreach (var param in _setParams)
        {
            dict.Add(param.Name, Functions.Lit(param.Value).Expression.Literal as Expression.Types.Literal);
        }
        return dict;
    }

    public static ParamMap FromMLOperatorParams(MapField<string, Expression.Types.Literal> paramsParams, ParamMap newParams)
    {
        foreach (var oiParam in paramsParams)
        {
            var paramName = oiParam.Key;
            dynamic paramValue = null;
            
            switch (oiParam.Value.LiteralTypeCase)
            {
                case Expression.Types.Literal.LiteralTypeOneofCase.None:
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Null:
                    paramValue = null;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Binary:
                    paramValue = oiParam.Value.Binary.ToByteArray();
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Boolean:
                    paramValue = oiParam.Value.Boolean;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Byte:
                    paramValue = oiParam.Value.Byte;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Short:
                    paramValue = oiParam.Value.Short;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Integer:
                    paramValue = oiParam.Value.Integer;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Long:
                    paramValue = oiParam.Value.Long;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Float:
                    paramValue = oiParam.Value.Float;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Double:
                    paramValue = oiParam.Value.Double;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Decimal:
                    paramValue = oiParam.Value.Decimal;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.String:
                    paramValue = oiParam.Value.String;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Date:
                    paramValue = oiParam.Value.Date;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Timestamp:
                    paramValue = oiParam.Value.Timestamp;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.TimestampNtz:
                    paramValue = oiParam.Value.TimestampNtz;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.CalendarInterval:
                    paramValue = oiParam.Value.CalendarInterval;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.YearMonthInterval:
                    paramValue = oiParam.Value.YearMonthInterval;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.DayTimeInterval:
                    paramValue = oiParam.Value.DayTimeInterval;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Array:
                    paramValue = oiParam.Value.Array;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Map:
                    paramValue = oiParam.Value.Map;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Struct:
                    paramValue = oiParam.Value.Struct;
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.SpecializedArray:
                    paramValue = oiParam.Value.SpecializedArray;
                    break;
                default:
                    paramValue = $"Unknown - do not understand the literal type {oiParam.Value.LiteralTypeCase} - this is a dotnet client lib issue not a spark issue";
                    break;
            }
            
            newParams.Add(new Param(paramName, paramValue));
        }
        return newParams;
    }
}