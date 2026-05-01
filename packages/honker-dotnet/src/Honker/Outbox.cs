using System.Text.Json;

namespace Honker;

public sealed class Outbox
{
    private readonly Queue _queue;
    private readonly Func<JsonElement, CancellationToken, Task> _delivery;
    private readonly OutboxOptions _options;

    internal Outbox(
        Database database,
        string name,
        Delegate originalDelivery,
        Func<JsonElement, CancellationToken, Task> delivery,
        OutboxOptions options)
    {
        Database = database;
        Name = name;
        OriginalDelivery = originalDelivery;
        _delivery = delivery;
        _options = options;
        _queue = database.Queue(
            QueueName,
            new QueueOptions(
                VisibilityTimeoutSeconds: options.VisibilityTimeoutSeconds,
                MaxAttempts: options.MaxAttempts
            )
        );
    }

    internal Delegate OriginalDelivery { get; }
    internal Database Database { get; }
    internal OutboxOptions Options => _options;

    public string Name { get; }
    public string QueueName => $"_outbox:{Name}";

    public long Enqueue(object? payload, HonkerTransaction? transaction = null, EnqueueOptions? options = null)
    {
        return _queue.Enqueue(payload, options, transaction);
    }

    public async Task RunWorker(string workerId, CancellationToken cancellationToken = default)
    {
        await foreach (var job in _queue.ClaimAsync(workerId, cancellationToken: cancellationToken).WithCancellation(cancellationToken))
        {
            try
            {
                await _delivery(job.Payload.Clone(), cancellationToken);
                if (!job.Ack())
                {
                    throw new InvalidOperationException($"outbox ack failed for job {job.Id}");
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                var delaySeconds = ComputeRetryDelaySeconds(job.Attempts);
                if (!job.Retry(delaySeconds, ex.Message))
                {
                    throw new InvalidOperationException($"outbox retry failed for job {job.Id}", ex);
                }
            }
        }
    }

    private int ComputeRetryDelaySeconds(long attempts)
    {
        if (_options.BaseBackoffSeconds <= 0)
        {
            return 0;
        }

        var exponent = Math.Max(0, attempts - 1);
        var delay = _options.BaseBackoffSeconds * Math.Pow(2, exponent);
        return Convert.ToInt32(Math.Clamp(Math.Ceiling(delay), 0, int.MaxValue));
    }
}

public sealed class LockHeldException : InvalidOperationException
{
    public LockHeldException(string name)
        : base($"lock '{name}' is already held")
    {
        Name = name;
    }

    public string Name { get; }
}
