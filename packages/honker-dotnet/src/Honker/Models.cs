using System.Text.Json;

namespace Honker;

public sealed record QueueOptions(
    int VisibilityTimeoutSeconds = 300,
    int MaxAttempts = 3
);

public sealed record EnqueueOptions(
    long? RunAtUnix = null,
    double? DelaySeconds = null,
    int Priority = 0,
    int? MaxAttempts = null,
    double? ExpiresSeconds = null
);

public sealed record ScheduledTask(
    string Name,
    string Queue,
    object? Payload = null,
    string? Schedule = null,
    string? Cron = null,
    long Priority = 0,
    long? ExpiresSeconds = null
);

public sealed record ScheduledFire(
    string Name,
    string Queue,
    long FireAt,
    long JobId
);

public sealed record OutboxOptions(
    int VisibilityTimeoutSeconds = 300,
    int MaxAttempts = 3,
    int BaseBackoffSeconds = 30
);

public static class Schedules
{
    public static string EverySeconds(int seconds)
    {
        if (seconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(seconds), "seconds must be positive");
        }

        return $"@every {seconds}s";
    }
}

public sealed class Job
{
    private readonly Queue _queue;
    private JsonDocument? _document;

    internal Job(
        Queue queue,
        long id,
        string queueName,
        string payloadRaw,
        string workerId,
        long attempts,
        long claimExpiresAt,
        string state = "processing")
    {
        _queue = queue;
        Id = id;
        QueueName = queueName;
        PayloadRaw = payloadRaw;
        WorkerId = workerId;
        Attempts = attempts;
        ClaimExpiresAt = claimExpiresAt;
        State = state;
    }

    public long Id { get; }
    public string QueueName { get; }
    public string PayloadRaw { get; }
    public string WorkerId { get; }
    public long Attempts { get; }
    public long ClaimExpiresAt { get; }
    public string State { get; }
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

    public bool Ack() => _queue.Ack(Id, WorkerId);
    public bool Retry(int delaySeconds = 60, string error = "") => _queue.Retry(Id, WorkerId, delaySeconds, error);
    public bool Fail(string error = "") => _queue.Fail(Id, WorkerId, error);
    public bool Heartbeat(int? extendSeconds = null) => _queue.Heartbeat(Id, WorkerId, extendSeconds);
}
