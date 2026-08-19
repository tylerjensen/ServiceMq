# Retries and dead letters

The outbound worker owns delivery. The application calls `Send` once; ServiceMq stores
the record, connects to the destination, retries failures, and either deletes the
record after success or moves it to the dead-letter area.

![Retry and dead-letter transitions](images/delivery-lifecycle.svg)

---

## Configure retry policy

```csharp
Delivery = new DeliveryOptions
{
    MaxAttempts = 100,
    MaxAge = TimeSpan.FromHours(24),
    InitialRetryDelay = TimeSpan.FromSeconds(1),
    MaximumRetryDelay = TimeSpan.FromMinutes(1),
    RetryBackoffFactor = 1.5
}
```

After a failure, delay grows approximately as:

```text
min(MaximumRetryDelay,
    InitialRetryDelay × RetryBackoffFactor ^ (attempt - 1))
```

The default attempt limit is effectively unlimited, while the default maximum age is
24 hours. A message enters the dead-letter store when either limit is reached.

Attempt count and last-attempt time are stored in version 7 outgoing records, so a
process restart does not reset the retry policy.

---

## Ordering while a destination is down

When one message fails, later messages for the same destination join its retry queue.
ServiceMq does not intentionally skip ahead for that destination. Different
destinations can make progress independently.

This ordering applies within one `MessageQueue` instance and its durable store. It is
not distributed ordering across several sender processes.

---

## Inspect failures

```csharp
foreach (DeadLetter dead in queue.GetDeadLetters())
{
    Console.WriteLine(
        $"{dead.Key}: {dead.MessageId} to {dead.Destination}, " +
        $"{dead.Attempts} attempts, {dead.Reason}");
}
```

A `DeadLetter` exposes:

- Storage key
- Original `MessageId`
- Destination
- Original sent time
- Attempt count
- Message type
- Last failure reason

The payload remains inside the protected stored record and is not copied into the
summary object.

---

## Replay

Fix the destination or configuration first, then replay:

```csharp
DeadLetter dead = queue.GetDeadLetters().First();

if (queue.ReplayDeadLetter(dead.Key))
{
    Console.WriteLine($"Requeued {dead.MessageId}");
}
```

Replay preserves the message `Id`, resets attempt timing, writes a new outgoing record,
and removes the dead-letter record only after the new record is stored.

To remove one record without replay:

```csharp
queue.DeleteDeadLetter(dead.Key);
```

To remove every current dead letter:

```csharp
queue.PurgeDeadLetters();
```

Automatic dead-letter retention is controlled by `StorageOptions.DeadLetterRetention`.

---

## Legacy failure logs

Older ServiceMq versions wrote minute-bucketed `fail-*.log` files. Version 7 leaves
those files in place and retention can remove them, but `GetDeadLetters()` only returns
new replayable `.dlq` records. Archive or migrate legacy logs separately if they must be
replayed.

---

## Avoiding duplicates during recovery

Replay deliberately preserves `Message.Id`. Consumers should use it as their
idempotency key:

```csharp
Message message = queue.Accept(5_000);
if (message != null)
{
    if (!processedIds.Contains(message.Id))
    {
        ApplyBusinessChange(message);
        processedIds.Add(message.Id);
    }
    queue.Acknowledge(message);
}
```

For transactional business changes, store the processed ID in the same database
transaction as the change.

---

[← Back to the user guide](user-guide.md)
