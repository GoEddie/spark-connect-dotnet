using System.Xml;
using Google.Protobuf.Collections;
using Spark.Connect.Dotnet.Sql;
using Spark.Connect.Dotnet.Sql.Types;

namespace Spark.Connect.Dotnet.ML.Param;

public class Param(string name, dynamic? value)
{
    public string Name { get; } = name;
    public dynamic? Value { get; } = value;
}

/// <summary>
/// This is used to store parameters for `Estimators`, `Transformers` and `Models`. Each object has its own set of default parameters, you can
/// then override the default parameters with your own value. Once you have overriden a value you can either change it again or clear it to go
/// back to the default. There are some objects that have required parameters but there are not default set (this is by spark, not spark-connect-dotnet)
/// </summary>
public class ParamMap
{
    public ParamMap(List<Param> defaultParams)
    {
        _defaultParams = defaultParams;
    }
    
    private List<Param> _defaultParams { get; } = new List<Param>();
    private IDictionary<string, Param> _setParams { get; } = new Dictionary<string, Param>();

    /// <summary>
    /// Add a param, this overrides the default if there is one
    /// </summary>
    /// <param name="name">The name of the parameter</param>
    /// <param name="value">The value, can be any type that can be passed to `Functions.Lit`</param>
    public void Add(string name, dynamic value)
    {
        Add(new Param(name, value));
    }

    /// <summary>
    /// Returns the default `Param` for a given param name
    /// </summary>
    /// <param name="name">Name of the param to get</param>
    /// <returns>`Param` (if it exists) otherwise null</returns>
    public Param? GetDefault(string name)
    {
        return _defaultParams.FirstOrDefault(p => p.Name == name);
    }

    /// <summary>
    /// Returns the default `Param` for a given param name
    /// </summary>
    /// <param name="param">The param that overrides the default to get</param>
    /// <returns>`Param` (if it exists) otherwise null</returns>
    public Param? GetDefault(Param param)
    {
        return _defaultParams.FirstOrDefault(p => p.Name == param.Name);
    }

    /// <summary>
    /// Is the `Param` a default or has it be manually set?
    /// </summary>
    /// <param name="param">`Param` to check if it exists</param>
    /// <returns>`bool` whether it exists or not</returns>
    public bool IsDefined(Param param) => IsDefined(param.Name);

    /// <summary>
    /// Is there a `Param` with a default or has it be manually set to a param of this name?
    /// </summary>
    /// <param name="name">Name of the `Param` to check</param>
    /// <returns>`bool` whether it exists or not</returns>
    public bool IsDefined(string name) => _defaultParams.Any(p => p.Name == name) || _setParams.ContainsKey(name);

    /// <summary>
    /// Has the `Param` been explicitly set on the map, is it different from the default. If it is the same as the
    ///  default then it won't be set.
    /// </summary>
    /// <param name="param">`Param` to check</param>
    /// <returns>`bool` if the param is set and is not the default</returns>
    public bool IsSet(Param param) => IsSet(param.Name);

    /// <summary>
    /// Has the `Param` with the name been explicitly set on the map, is it different from the default. If it is the same as the
    ///  default then it won't be set.
    /// </summary>
    /// <param name="name">Name of the param to check</param>
    /// <returns>`bool` if the param is set and is not the default</returns>
    public bool IsSet(string name)
    {
        return _setParams.ContainsKey(name);
    }

    /// <summary>
    /// If a param with this name has been set then clear it and return it to the default
    /// </summary>
    /// <param name="name">`Param` name to clear</param>
    public void Clear(string name)
    {
        if (IsSet(name))
        {
            _setParams.Remove(name);
        }
    }
    
    /// <summary>
    /// If a param with this name has been set then clear it and return it to the default
    /// </summary>
    /// <param name="param">`Param` clear</param>
    public void Clear(Param param)
    {
        if (IsSet(param))
        {
            _setParams.Remove(param.Name);
        }
    }
    
    /// <summary>
    /// Add a new `Param` which either is the default or overrides the default
    /// </summary>
    /// <param name="param"></param>
    public void Add(Param param)
    {
        var defaultParam = GetDefault(param.Name);
        _setParams[param.Name] = param;
    }

    /// <summary>
    /// Retrieve a `Param` by name
    /// </summary>
    /// <param name="name">Name of the `Param`</param>
    /// <returns>`Param` if it exists or null</returns>
    public Param? Get(string name)
    {
        if (_setParams.ContainsKey(name))
        {
            return _setParams[name];
        }
        
        if (_defaultParams.Any(p => p.Name == name))
        {
            return _defaultParams.First(p => p.Name == name);
        }

        return null;
    }

    /// <summary>
    /// Get a list of all the current `Param`'s including any defaults
    /// </summary>
    /// <returns>`IList` of `Param`</returns>
    public IList<Param> GetAll()
    {
        var paramDict = _defaultParams.ToDictionary(p => p.Name);
        foreach (var param in _setParams)
        {
            paramDict[param.Key] = param.Value;
        }
        
        return paramDict.Values.ToList();
    }

    /// <summary>
    /// Copy this `ParamMap` to an entirely new `ParamMap`, used primarily for `ParamMap.Update`
    /// </summary>
    /// <returns>Copy of the original `ParamMap`</returns>
    public ParamMap Clone()
    {
        var newMap = new ParamMap(_defaultParams);
        foreach (var param in _setParams)
        {
            newMap.Add(param.Value);
        }
        return newMap;
    }

    /// <summary>
    /// Adds or Updates any `Param`'s in the `ParamMap` with the keys in the dictionary, returning a new copy of the `ParamMap`
    /// </summary>
    /// <param name="updates">New parameters to update in the newly created `ParamMap`</param>
    /// <returns>`ParamMap` with `Params` created from the updated dictionary</returns>
    public ParamMap Update(IDictionary<string, dynamic> updates)
    {
        foreach (var update in updates)
        {
            this.Add(update.Key, update.Value);
        }

        return this;
    }

    /// <summary>
    /// Adds or Updates any `Param`'s in the `ParamMap` with the keys in the dictionary, returning a new copy of the `ParamMap`
    /// </summary>
    /// <param name="source">The `ParamMap` to copy from</param>
    /// <returns>`ParamMap` with `Params` created from the updated dictionary</returns>
    public ParamMap Update(ParamMap source)
    {
        foreach (var update in source.GetAll())
        {
            this.Add(update.Name, update.Value);
        }

        return this;
    }

    /// <summary>
    /// Used as a helper to convert a `ParamMap` into the proto types
    /// </summary>
    /// <returns></returns>
    public IDictionary<string,Expression.Types.Literal> ToMapField()
    {
        var dict = new Dictionary<string, Expression.Types.Literal>();
        foreach (var param in _setParams)
        {
            if (param.Value.Value is Array array)
            {
                var value = new Expression.Types.Literal
                {
                    Array = new Expression.Types.Literal.Types.Array()
                    {
                        ElementType = SparkDataType.FromDotNetType(array.GetType().GetElementType()).ToDataType()
                    }
                };

                foreach (var element in array)
                {
                    value.Array.Elements.Add(Functions.Lit(element).Expression.Literal);
                }
                
                dict.Add(param.Value.Name, value);
            }
            else
            {
                dict.Add(param.Value.Name, Functions.Lit(param.Value.Value).Expression.Literal as Expression.Types.Literal);    
            }
            
        }
        return dict;
    }

    /// <summary>
    /// Used as a helper to convert to a `ParamMap` from the proto types
    /// </summary>
    /// <param name="paramsParams"></param>
    /// <param name="newParams"></param>
    /// <returns></returns>
    public static ParamMap FromMLOperatorParams(MapField<string, Expression.Types.Literal> paramsParams, ParamMap newParams)
    {
        foreach (var oiParam in paramsParams)
        {
            var paramName = oiParam.Key;
            dynamic paramValue = GetValueFromLiteral(oiParam.Value);
            newParams.Add(new Param(paramName, paramValue));
        }
        return newParams;
    }

    public static dynamic GetValueFromLiteral(Expression.Types.Literal literal)
    {
        switch (literal.LiteralTypeCase)
            {
                case Expression.Types.Literal.LiteralTypeOneofCase.None:
                    break;
                case Expression.Types.Literal.LiteralTypeOneofCase.Null:
                    return null;
                case Expression.Types.Literal.LiteralTypeOneofCase.Binary:
                    return literal.Binary.ToByteArray();
                case Expression.Types.Literal.LiteralTypeOneofCase.Boolean:
                   return literal.Boolean;
                case Expression.Types.Literal.LiteralTypeOneofCase.Byte:
                    return literal.Byte;
                case Expression.Types.Literal.LiteralTypeOneofCase.Short:
                   return literal.Short;
                case Expression.Types.Literal.LiteralTypeOneofCase.Integer:
                    return literal.Integer;
                case Expression.Types.Literal.LiteralTypeOneofCase.Long:
                    return literal.Long;
                case Expression.Types.Literal.LiteralTypeOneofCase.Float:
                   return literal.Float;
               
                case Expression.Types.Literal.LiteralTypeOneofCase.Double:
                   return literal.Double;
         
                case Expression.Types.Literal.LiteralTypeOneofCase.Decimal:
                   return literal.Decimal;
          
                case Expression.Types.Literal.LiteralTypeOneofCase.String:
                  return literal.String;
       
                case Expression.Types.Literal.LiteralTypeOneofCase.Date:
                 return literal.Date;
      
                case Expression.Types.Literal.LiteralTypeOneofCase.Timestamp:
                  return literal.Timestamp;
         
                case Expression.Types.Literal.LiteralTypeOneofCase.TimestampNtz:
                  return  literal.TimestampNtz;
         
                case Expression.Types.Literal.LiteralTypeOneofCase.CalendarInterval:
                    return literal.CalendarInterval;
                case Expression.Types.Literal.LiteralTypeOneofCase.YearMonthInterval:
                    return literal.YearMonthInterval;
     
                case Expression.Types.Literal.LiteralTypeOneofCase.DayTimeInterval:
                   return literal.DayTimeInterval;
     
                case Expression.Types.Literal.LiteralTypeOneofCase.Array:
                    
                    var array = SparkSession.CreateGenericList(GetValueFromLiteral(literal.Array.Elements.First()).GetType());
                    foreach (var element in literal.Array.Elements)
                    {
                        array.Add(GetValueFromLiteral(element));
                    }

                    return array.ToArray();
                
                case Expression.Types.Literal.LiteralTypeOneofCase.Map:
                    return literal.Map;
                    
                case Expression.Types.Literal.LiteralTypeOneofCase.Struct:
                    return literal.Struct;
                case Expression.Types.Literal.LiteralTypeOneofCase.SpecializedArray:
                    return literal.SpecializedArray;
                default:
                    return $"Unknown - do not understand the literal type {literal.LiteralTypeCase} - this is a dotnet client lib issue not a spark issue";
            }
        
        return null;
    }
}