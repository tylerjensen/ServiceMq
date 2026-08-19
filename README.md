# ServiceMq

### A durable, store-and-forward message queue for .NET

ServiceMq moves typed objects, text, or bytes between .NET processes over named pipes
or TCP. A sender writes each message to durable storage before delivery. If the
destination is unavailable, ServiceMq retries without making the application manage a
retry loop. A receiver can consume immediately or explicitly acknowledge work.

![A message is stored at both ends of its trip](docs/images/architecture.svg)

## Documentation

The [ServiceMq User Guide](docs/user-guide.md) is the main documentation. Start with:

| If you want to… | Read |
| --- | --- |
| Send and receive a message in ten minutes | [Getting started](docs/getting-started.md) |
| Choose named pipes, TCP, or both | [Addresses and transports](docs/addresses-and-transports.md) |
| Understand `Receive`, `Accept`, and delivery guarantees | [Messages and delivery](docs/messages-and-delivery.md) |
| Choose file, memory, SQLite, or custom storage | [Storage](docs/storage.md) |
| Configure retries and recover failed messages | [Retries and dead letters](docs/retries-and-dead-letters.md) |
| Monitor disk use and repair bad records | [Operations](docs/operations.md) |
| Protect data and deploy safely | [Security](docs/security.md) |
| Upgrade an older ServiceMq application | [Migrating to 7.0](docs/migrating-to-v7.md) |
| Diagnose a problem | [Troubleshooting](docs/troubleshooting.md) |

## Install

```shell
dotnet add package ServiceMq --version 7.0.0
```

For SQLite storage:

```shell
dotnet add package ServiceMq.Sqlite --version 7.0.0
```

Both packages target `netstandard2.0` and `net8.0`. ServiceMq 7 uses ServiceWire 7.0.

## Quick start

Create one address and queue per process. This example uses named pipes because both
queues are on the same machine:

```csharp
using ServiceMq;

var ordersAddress = new Address("orders-pipe");

using var orders = new MessageQueue(
    name: "orders",
    address: ordersAddress,
    msgDir: @"C:\service-data\orders");

using var checkout = new MessageQueue(
    name: "checkout",
    address: new Address("checkout-pipe"),
    msgDir: @"C:\service-data\checkout");

Guid id = checkout.Send(ordersAddress, new OrderPlaced
{
    OrderId = 42,
    Total = 19.95m
});

Message message = orders.Receive(timeoutMs: 5_000);
if (message != null)
{
    OrderPlaced order = message.To<OrderPlaced>();
    Console.WriteLine($"Received {message.Id}: order {order.OrderId}");
}

public sealed class OrderPlaced
{
    public int OrderId { get; set; }
    public decimal Total { get; set; }
}
```

`Send` returns after the outgoing message is stored, not necessarily delivered.
`Receive` removes the incoming record before returning it. Use `Accept` followed by
`Acknowledge` when work must remain recoverable until processing finishes:

```csharp
Message message = orders.Accept(timeoutMs: 5_000);
if (message != null)
{
    try
    {
        await HandleOrder(message.To<OrderPlaced>());
        orders.Acknowledge(message);
    }
    catch
    {
        orders.ReEnqueue(message);
        throw;
    }
}
```

## Production configuration

The original constructor remains supported. `MessageQueueOptions` exposes the complete
configuration surface:

```csharp
var queue = new MessageQueue(new MessageQueueOptions
{
    Name = "orders",
    Address = new Address("orders-pipe"),
    VisibilityTimeout = TimeSpan.FromMinutes(1),
    Storage = new StorageOptions
    {
        RootPath = @"D:\service-data\orders",
        Durability = DurabilityMode.FlushToDisk,
        MaxBytes = 10L * 1024 * 1024 * 1024,
        MaxMessages = 1_000_000,
        FullBehavior = QueueFullBehavior.Reject,
        SentRetention = TimeSpan.FromDays(2),
        ReadRetention = TimeSpan.FromHours(12),
        DeadLetterRetention = TimeSpan.FromDays(30),
        SentAuditPayload = AuditPayloadMode.MetadataOnly,
        ReadAuditPayload = AuditPayloadMode.MetadataOnly
    },
    Delivery = new DeliveryOptions
    {
        MaxConcurrentDestinations = 8,
        MaxAttempts = 100,
        MaxAge = TimeSpan.FromDays(1),
        InitialRetryDelay = TimeSpan.FromSeconds(1),
        MaximumRetryDelay = TimeSpan.FromMinutes(1),
        RetryBackoffFactor = 1.5
    }
});
```

See [Storage](docs/storage.md) for every option and provider.

## What ServiceMq guarantees

- Outgoing and incoming records are stored before their respective RPC calls return.
- Delivery is **at least once**. A crash in the final acknowledgment window can produce
  a duplicate, so consumers should treat `Message.Id` as an idempotency key.
- Messages sent by one `MessageQueue` are delivered FIFO per destination, including
  after an outage or restart. Concurrent sends are ordered when they enter the durable
  outbound queue; ordering does not span separate sender processes.
- File records use atomic replacement and malformed records are quarantined.
- Legacy `.omq` and `.imq` records remain readable.

ServiceMq is an embedded queue library, not a clustered broker. It does not provide
distributed consensus, competing-consumer coordination across several processes, or
exactly-once side effects.

## Storage providers

| Provider | Package | Best for |
| --- | --- | --- |
| `FileMessageStore` | `ServiceMq` | Durable queues with minimal infrastructure |
| `MemoryMessageStore` | `ServiceMq` | Tests and deliberately transient queues |
| `SqliteMessageStore` | `ServiceMq.Sqlite` | Indexed, transactional storage in one database file |
| `IMessageStore` | Your assembly | Application-specific storage engines |

![How to choose a storage provider](docs/images/provider-choice.svg)

## Project status

ServiceMq 7.0 targets .NET Standard 2.0 and .NET 8.0 and uses ServiceWire 7.0. The test
suite covers named pipes, TCP, restart compatibility, capacity policies, visibility
timeouts, dead-letter replay, encryption, corruption quarantine, and all built-in
storage providers.

Licensed under the [Apache License 2.0](License.txt).
