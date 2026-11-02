using System.Text.Json;

namespace OutboxKit;

public sealed class JsonMessageSerializer : IMessageSerializer
{
    private readonly JsonSerializerOptions _options;

    public JsonMessageSerializer(JsonSerializerOptions? options = null)
    {
        _options = options ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    public string Serialize(object message, Type type)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(type);
        return JsonSerializer.Serialize(message, type, _options);
    }

    public object Deserialize(string payload, Type type)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(type);

        // a stored "null" would only come from a bug on the write side, so fail loudly here
        return JsonSerializer.Deserialize(payload, type, _options)
            ?? throw new JsonException($"The payload for {type.FullName} deserialized to null.");
    }
}
