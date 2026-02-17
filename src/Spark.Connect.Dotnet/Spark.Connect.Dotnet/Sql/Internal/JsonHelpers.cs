
using System.Text.Json;
using System.Text.Json.Nodes;

public static class JsonHelpers
{
    public static Dictionary<string, object>? JsonMapToDictionary(string json)
    {

        if(json == null || json.Length == 0) return null;

        // Parse the JSON into a DOM
        using JsonDocument doc = JsonDocument.Parse(json);
        JsonElement root = doc.RootElement;

        if (root.ValueKind != JsonValueKind.Object)
            throw new ArgumentException("JSON root must be an object (a map).");

        var result = new Dictionary<string, object>();
        foreach (var property in root.EnumerateObject())
        {
            result[property.Name] = ConvertElement(property.Value);
        }

        return result;
    }

    private static object ConvertElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out long l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Array => element.EnumerateArray().Select(ConvertElement).ToList(),
            JsonValueKind.Object => element.EnumerateObject()
                                              .ToDictionary(p => p.Name, p => ConvertElement(p.Value)),
            _ => null
        };
    }


    public static string DictionaryToJson(Dictionary<string, object>? dict)
    {
        var jsonObject = ConvertToJsonNode(dict);
        return jsonObject.ToJsonString(new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }

    private static JsonNode ConvertToJsonNode(object? value)
    {
        switch (value)
        {
            case null:
                return JsonValue.Create((string)null)!;

            case string s:
                return JsonValue.Create(s)!;

            case bool b:
                return JsonValue.Create(b)!;

            case int i:
                return JsonValue.Create(i)!;

            case long l:
                return JsonValue.Create(l)!;

            case double d:
                return JsonValue.Create(d)!;

            case float f:
                return JsonValue.Create(f)!;

            case Dictionary<string, object?> nestedDict:
                var objNode = new JsonObject();
                foreach (var kvp in nestedDict)
                {
                    objNode[kvp.Key] = ConvertToJsonNode(kvp.Value);
                }
                return objNode;

            case IEnumerable<object?> list:
                var arrNode = new JsonArray();
                foreach (var item in list)
                {
                    arrNode.Add(ConvertToJsonNode(item));
                }
                return arrNode;

            default:
                // Fallback: let System.Text.Json try to serialize it
                return JsonSerializer.SerializeToNode(value)!;
        }
    }

}
