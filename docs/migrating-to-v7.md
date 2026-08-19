# Migrating to ServiceMq 7

[Back to the user guide](user-guide.md)

ServiceMq 7 moves to ServiceWire 7, targets modern .NET, and introduces configurable storage without requiring applications to abandon the original constructor and queue API.

## Package and framework changes

| Area | Earlier ServiceMq | ServiceMq 7 |
|---|---|---|
| ServiceWire dependency | 5.x-era package | `ServiceWire` 7.0 |
| Target frameworks | Older .NET targets | `netstandard2.0` and `net8.0` |
| Core storage | Directory-based only | File, memory, custom provider |
| SQLite | Not available | Optional `ServiceMq.Sqlite` package |
| Delivery failure | Fail log | Retry policy plus dead-letter APIs |

Upgrade the package references together:

```xml
<PackageReference Include="ServiceMq" Version="7.0.0" />
```

Add the companion package only when the application uses SQLite:

```xml
<PackageReference Include="ServiceMq.Sqlite" Version="7.0.0" />
```

## Existing code can start unchanged

The original constructor remains available:

```csharp
using var queue = new MessageQueue(
    "orders",
    new Address("orders-pipe"),
    @"D:\ServiceMq\orders");
```

You can adopt `MessageQueueOptions` incrementally when you need a provider, capacity, retry, retention, visibility, or encryption setting.

## Review the default storage location

When no message directory is supplied, ServiceMq 7 uses an application-local data directory rather than relying on the assembly directory. This is more suitable for installed and hosted applications, but it may differ from the path used by an older deployment.

For an upgrade, set `Storage.RootPath` explicitly until you have verified the intended location:

```csharp
var options = new MessageQueueOptions
{
    Name = "orders",
    Address = new Address("orders-pipe"),
    Storage = new StorageOptions
    {
        RootPath = @"D:\ServiceMq\orders"
    }
};
```

## Existing file queues

The file provider reads legacy `.imq` and `.omq` records. New records use the current validated representation. Existing fail logs remain available for historical diagnosis, while newly exhausted deliveries enter the dead-letter store and can be inspected or replayed through the API.

Before upgrading:

1. Stop all processes using the queue root.
2. Back up the complete directory.
3. Upgrade a copy in a staging environment.
4. Confirm that queued incoming and outgoing records load and deliver.
5. Confirm your dead-letter and retention procedures.

The v7 file provider enforces exclusive ownership of a root. If an older deployment shared one directory between multiple live queue instances, give each instance its own root before upgrading.

## Review delivery behavior

Delivery remains at least once. ServiceMq 7 adds a configurable exponential retry policy and a dead-letter destination. The defaults preserve a bounded delivery age while avoiding a tight retry loop.

Applications should:

- make handlers idempotent;
- alert on dead letters;
- decide who may replay or delete a dead letter;
- choose retry intervals and maximum age for the downstream service objective.

See [Retries and dead letters](retries-and-dead-letters.md).

## Adopt acknowledged receive when needed

`Receive()` still completes a stored message when it is returned. For handlers that
must finish work before the record is removed, migrate them to `Accept()` and call
`Acknowledge(message)` after the side effect succeeds:

```csharp
Message message = queue.Accept(timeoutMs: 5_000);
if (message != null)
{
    HandleOrder(message.To<OrderPlaced>());
    queue.Acknowledge(message);
}
```

Unacknowledged deliveries become visible again after the visibility timeout.

## Move to SQLite only when it helps

SQLite is opt-in; upgrading does not require a provider migration. Prefer it when one process owns a queue and you want transactional, indexed local persistence. A provider change is a data migration, not merely a configuration change: drain or export active work, initialize the new provider, validate it, and keep a rollback copy.

## Upgrade checklist

- [ ] The application targets a framework compatible with `netstandard2.0` or `net8.0`.
- [ ] Package references use ServiceMq 7 and no conflicting direct ServiceWire version.
- [ ] The storage root is explicit and backed up.
- [ ] Each live file queue has a unique root.
- [ ] Restart and legacy-record loading were tested.
- [ ] Handlers tolerate duplicate delivery.
- [ ] Retry, capacity, retention, and visibility defaults were reviewed.
- [ ] Dead-letter and corrupt-record alerts have an owner.
- [ ] TCP deployments have an appropriate secure network boundary.

For ServiceWire-specific compatibility details, see the upstream [ServiceWire migration guide](https://github.com/tylerjensen/ServiceWire/blob/master/docs/migrating-to-v7.md).

Next: [Storage](storage.md) or [Troubleshooting](troubleshooting.md).
