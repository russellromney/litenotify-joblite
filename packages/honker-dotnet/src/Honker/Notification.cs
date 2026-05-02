using System.Text.Json;

namespace Honker;

public sealed class Notification
{
    private JsonDocument? _document;
    private object? _decoded;
    private bool _decodedSet;

    internal Notification(long id, string channel, string payloadRaw, long createdAt)
    {
        Id = id;
        Channel = channel;
        PayloadRaw = payloadRaw;
        CreatedAt = createdAt;
    }

    public long Id { get; }
    public string Channel { get; }
    public string PayloadRaw { get; }
    public long CreatedAt { get; }

    public object? Payload
    {
        get
        {
            if (_decodedSet)
            {
                return _decoded;
            }

            _decodedSet = true;
            if (PayloadRaw is "" or "null")
            {
                _decoded = null;
                return null;
            }

            try
            {
                _document = JsonDocument.Parse(PayloadRaw);
                _decoded = _document.RootElement.Clone();
            }
            catch (JsonException)
            {
                _decoded = PayloadRaw;
            }

            return _decoded;
        }
    }
}

public sealed class Event
{
    private JsonDocument? _document;

    internal Event(long offset, string topic, string? key, string payloadRaw, long createdAt)
    {
        Offset = offset;
        Topic = topic;
        Key = key;
        PayloadRaw = payloadRaw;
        CreatedAt = createdAt;
    }

    public long Offset { get; }
    public string Topic { get; }
    public string? Key { get; }
    public string PayloadRaw { get; }
    public long CreatedAt { get; }
    public JsonElement Payload
    {
        get
        {
            _document ??= JsonDocument.Parse(PayloadRaw);
            return _document.RootElement;
        }
    }

    public T? GetPayload<T>()
    {
        return JsonSerializer.Deserialize<T>(PayloadRaw);
    }
}
