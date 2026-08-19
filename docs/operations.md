# Operations

[Back to the user guide](user-guide.md)

ServiceMq exposes queue state, storage health, maintenance controls, and recovery APIs so an application can operate a queue without inspecting implementation details on disk.

## Monitor queue and storage health

```csharp
var health = queue.StorageHealth;

Console.WriteLine($"Inbound state: {queue.StateInbound}");
Console.WriteLine($"Outbound state: {queue.StateOutbound}");
Console.WriteLine($"Storage state: {health.State}");
Console.WriteLine($"Incoming: {health.IncomingMessages}");
Console.WriteLine($"Outgoing: {health.OutgoingMessages}");
Console.WriteLine($"Dead letters: {health.DeadLetterMessages}");
Console.WriteLine($"Corrupt entries: {health.CorruptMessages}");
Console.WriteLine($"Stored bytes: {health.StoredBytes}");

if (health.LastException is not null)
    Console.Error.WriteLine(health.LastException);
```

`StateInbound` and `StateOutbound` report the two independently running sides of the
queue:

| State | Meaning |
|---|---|
| `Running` | No current queue warning or failure is known. |
| `Cautioned` | ServiceMq encountered a recoverable condition that deserves attention. |
| `Failed` | The queue encountered a failure that prevents normal operation. |

Storage health is a snapshot. Poll it at the interval appropriate for your service and publish the values through your existing monitoring system.

Useful alerts include:

- outgoing messages growing continuously;
- an oldest outgoing message older than the expected delivery objective;
- any dead letters or corrupt entries;
- storage approaching `MaxMessages` or `MaxBytes`;
- a non-running queue or unhealthy store.

## Run maintenance

Retention and audit cleanup run automatically while the queue is active. You can also trigger maintenance explicitly:

```csharp
queue.RunStorageMaintenance();
queue.FlushStorage();
```

`RunStorageMaintenance` applies the configured retention rules. `FlushStorage` asks the provider to persist outstanding state. Flush before a coordinated snapshot or before stopping infrastructure around the queue; disposing the queue also closes its storage resources.

## Inspect exceptional records

Dead letters are valid messages that exhausted the delivery policy:

```csharp
foreach (var item in queue.GetDeadLetters())
{
    Console.WriteLine($"{item.Id}: {item.Reason}");
}
```

Corrupt entries could not be decoded or validated by the file provider and are quarantined instead of being silently discarded:

```csharp
foreach (var item in queue.GetCorruptEntries())
{
    Console.WriteLine($"{item.Key}: {item.Length} bytes, modified {item.LastModifiedUtc:u}");
}
```

Exceptional records can contain application payloads. Treat diagnostic output and operator tools with the same confidentiality controls as the messages themselves.

## Back up a queue

For a consistent file-provider backup:

1. Stop producers and allow outstanding delivery to settle.
2. Dispose the `MessageQueue` that owns the store.
3. Copy the complete queue root, including audit, dead-letter, and corrupt areas.
4. Restart the queue.

For SQLite, coordinate the snapshot with the database provider. Stopping and disposing the queue before copying the database is the simplest portable approach.

Do not let two live queue instances use the same file-provider root. The provider takes an exclusive ownership lock to prevent conflicting writers.

## Shut down cleanly

`MessageQueue` is disposable. Keep it for the lifetime of the service and dispose it during application shutdown:

```csharp
using var queue = new MessageQueue(options);

// Run the application until its cancellation token is signaled.
```

In a hosted application, register the queue as a singleton and dispose it with the host. Abrupt termination is recoverable for durable providers, but graceful shutdown reduces replay and makes backups easier to coordinate.

## Clear state deliberately

```csharp
queue.ClearState();
```

Clearing state removes queue data and is intended for tests, development resets, or an explicitly approved administrative workflow. It is not routine maintenance. Back up anything you may need to diagnose or replay first.

## Operational checklist

- Give each queue instance its own durable storage root or SQLite database.
- Put persistent storage on a volume with enough capacity and suitable backup policy.
- Monitor outgoing age, dead letters, corrupt entries, and total bytes.
- Make handlers idempotent because delivery is at least once.
- Test restart, disk-full, peer-unavailable, and dead-letter recovery scenarios.
- Dispose queues during orderly service shutdown.

Next: [Security](security.md) or [Troubleshooting](troubleshooting.md).
