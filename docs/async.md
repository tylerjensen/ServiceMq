# Async API

ServiceMq 7.3 adds an async surface that mirrors the synchronous API. Every async method
returns a `Task` and never blocks the caller's thread on storage I/O or on the receive wait,
so it composes cleanly with `async`/`await` in ASP.NET, console workers, and other
concurrency-heavy hosts.

## What is async

Two layers are async:

- **`IAsyncMessageStore`** — the storage contract. All built-in providers implement it:
  - `FileMessageStore` uses the real async file APIs for reads and writes.
  - `SqliteMessageStore` serializes on the shared connection and offloads the work.
  - `MemoryMessageStore` completes synchronously (it is in-memory) and returns a completed task.
  - `SharpCoreDbMessageStore` offloads its fast point operations to the thread pool.
  - `ConfiguredMessageStore` (the decorator ServiceMq builds around your provider) delegates to
    the inner store's async surface when it implements `IAsyncMessageStore`, and falls back to
    offloading otherwise.
- **`MessageQueue`** — the public facade. Async methods prefer the async store when the
  configured provider supports it and fall back to the synchronous store otherwise, so the
  async API works with any `IMessageStore`.

## Send and broadcast

```csharp
Guid id = await queue.SendAsync(destination, new OrderPlaced { OrderId = 42 });
await queue.SendAsync(destination, "order.placed", "serialized payload");
await queue.SendBytesAsync(destination, bytes, "application/octet-stream");

Guid broadcastId = await queue.BroadcastAsync(new[] { r1, r2 }, "hello everyone");
```

## Receive and acknowledge

`ReceiveAsync`, `ReceiveBulkAsync`, `AcceptAsync`, and `AcceptBulkAsync` wait without holding
a thread: the wait is a polling `Task.Delay` loop, and the brief store I/O is offloaded. A
timeout returns `null` (or an empty list) just like the synchronous methods.

```csharp
Message message = await queue.AcceptAsync(timeoutMs: 5_000);
if (message != null)
{
    var order = message.To<OrderPlaced>();
    await queue.AcknowledgeAsync(message);   // or ReEnqueueAsync(message) to retry later
}
```

`AcceptAsync`/`AcceptBulkAsync` lease the message under the visibility-timeout policy; complete
it with `AcknowledgeAsync` or return it with `ReEnqueueAsync`.

## Cancellation

Every async method accepts an optional `CancellationToken`. Cancelling a receive aborts the
wait with `OperationCanceledException`. Starting with 7.4, once a receive has dequeued
messages, it completes storage updates and returns those messages even if cancellation
is requested. This also applies to the entire batch returned by `ReceiveBulkAsync`:
cancellation cannot discard a partially completed batch.

`AcknowledgeAsync` checks cancellation before starting completion. Once started, it
finishes the acknowledgement even if the token is canceled. The visibility lease is
retained and prevented from expiring during completion; if storage fails, the lease
can expire and redeliver the message. A token canceled before acknowledgement starts
leaves the message and lease unchanged.

`ReceiveAsync` and `ReceiveBulkAsync` remove messages before returning them. For work
that must remain recoverable until processing succeeds, use `AcceptAsync` or
`AcceptBulkAsync`, followed by acknowledgement.

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
Message message = await queue.ReceiveAsync(-1, cts.Token);
```

## Inspection and maintenance

```csharp
IReadOnlyList<DeadLetter> deadLetters = await queue.GetDeadLettersAsync();
await queue.ReplayDeadLetterAsync(deadLetter.Key);
await queue.DeleteDeadLetterAsync(deadLetter.Key);
await queue.PurgeDeadLettersAsync();

IReadOnlyList<StorageEntry> corrupt = await queue.GetCorruptEntriesAsync();
await queue.DeleteCorruptEntryAsync(key);

QueueStorageHealth health = await queue.GetStorageHealthAsync();
await queue.FlushStorageAsync();
await queue.RunStorageMaintenanceAsync();
```

## Custom providers

To expose real async I/O in a custom store, implement `IAsyncMessageStore` alongside
`IMessageStore`. The queue internals detect the interface and prefer it; otherwise every async
call falls back to offloading the synchronous implementation. `IAsyncMessageStore` mirrors
`IMessageStore` one method at a time with a trailing `CancellationToken`.
