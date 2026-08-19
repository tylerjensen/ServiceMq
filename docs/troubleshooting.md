# Troubleshooting

[Back to the user guide](user-guide.md)

Start with the queue state and storage snapshot:

```csharp
Console.WriteLine(queue.StateInbound);
Console.WriteLine(queue.StateOutbound);
Console.WriteLine(queue.StorageHealth.State);
Console.WriteLine(queue.StorageHealth.LastException);
```

Then check outgoing age, dead letters, corrupt records, storage capacity, and whether the remote ServiceWire endpoint is reachable.

## The storage root is already in use

The file provider allows one live owner for a queue root. This prevents two processes from changing the same records concurrently.

- Confirm another process or duplicate application instance is not using the path.
- Confirm two queue registrations did not accidentally receive the same `RootPath`.
- Dispose the owning queue cleanly before starting its replacement.
- Do not remove lock artifacts while a possible owner is still running.

If multiple replicas need durable local queues, assign a separate root or database to every replica.

## Access is denied or the store cannot be created

- Use an absolute path on a persistent, writable volume.
- Grant the application identity read/write/create/delete permission on that queue root.
- Do not use an installation directory that becomes read-only after deployment.
- Verify antivirus or backup software is not holding records indefinitely.

When no path is specified, log the configured root during startup so operators know where data lives.

## Outgoing messages keep growing

This normally means the peer is unavailable or rejecting delivery.

1. Verify the peer is running and listening on the expected named-pipe or TCP address.
2. Verify both sides use the same transport and TCP port.
3. Check firewall, DNS, routing, and service-account boundaries.
4. Inspect `OldestOutgoingUtc` and the current retry policy.
5. Inspect dead letters after the maximum attempts or age is reached.

ServiceMq retains durable outgoing records while retrying; growth during an outage is expected until capacity limits apply backpressure.

## A capacity exception occurs

`MaxMessages` or `MaxBytes` has been reached. Capacity protects the host from
unbounded accumulation.

- Restore the downstream consumer instead of simply raising every limit.
- Clear acknowledged audit records through retention, if they are consuming the byte budget applicable to your provider.
- Inspect dead letters and corrupt records.
- Increase capacity only after confirming the volume and operational objectives can support it.

## A message is delivered more than once

ServiceMq provides at-least-once delivery. A sender or receiver can fail after the peer performed work but before completion was durably recorded.

Use a stable business-operation ID and make the handler idempotent. Store processed IDs or apply a unique database constraint in the same transaction as the handler's side effect.

## An accepted message returns later

An `Accept()` message remains pending until `Acknowledge(message)` succeeds. If it is
not acknowledged before the visibility timeout, it becomes eligible again.

- Acknowledge only after application work completes.
- Choose a visibility timeout longer than normal handler duration.
- Renew or redesign long-running work rather than relying on a very short timeout.
- Expect replay after process termination.

Use `Receive<T>()` when removing the record as it is returned is the intended behavior.

## Messages entered the dead-letter store

Inspect each record's reason and attempts:

```csharp
foreach (var item in queue.GetDeadLetters())
    Console.WriteLine($"{item.Id} attempts={item.Attempts}: {item.Reason}");
```

Correct the peer, contract, or configuration first. Then replay selected entries. Blindly replaying all records can repeat poison-message failures and overload the recovered peer.

See [Retries and dead letters](retries-and-dead-letters.md).

## Corrupt entries are reported

The file provider quarantines data it cannot safely decode. Common causes include partial external copies, disk corruption, manual file edits, or using the wrong encryption key.

- Preserve the corrupt record and a store backup for investigation.
- Check `StorageHealth.LastException` and `GetCorruptEntries()`.
- Verify the configured protector and key.
- Restore from a known-good backup when necessary.
- Delete a quarantined entry only after an approved recovery decision.

## Protected records will not load

Verify that the same protector configuration and key used to write the records are available at startup. Base64 decoding success does not prove the key is correct; authenticated decryption will reject a different or damaged key.

Do not switch protection on or off for a populated store without a planned migration.

## SQLite fails to initialize

- Reference `ServiceMq.Sqlite` in addition to the core package.
- Configure the SQLite provider through the companion package API.
- Ensure the database directory is writable and persistent.
- Confirm only the intended process owns the local database.
- Include the package's native/runtime assets in trimmed or custom deployments.

If none of the SQLite benefits are required, the core file provider is the simplest durable fallback.

## TCP cannot bind or connect

- Use an explicit port and confirm it is not already occupied.
- Confirm the host has a usable non-loopback IPv4 address for the convenience constructor.
- Verify the peer resolves the intended address rather than a container- or host-only address.
- Keep both peers on compatible ServiceWire versions and message contracts.
- Remember that ServiceMq does not add TCP TLS or authentication; verify the secure tunnel or private network too.

For one-machine communication, named pipes avoid network addressing and firewall configuration.

## The process does not shut down cleanly

Keep one long-lived queue instance, stop application work, and dispose the queue during host shutdown. Avoid constructing undisposed queues per message or per request.

## Diagnostic information to collect

When reporting a reproducible issue, include:

- ServiceMq, ServiceWire, and target-framework versions;
- transport and endpoint shape, with secrets and public addresses redacted;
- storage provider and non-secret option values;
- queue and storage health snapshots;
- counts and oldest timestamps, not message bodies unless necessary;
- exception type, message, and stack trace;
- whether the issue survives a clean restart;
- a minimal sender/receiver contract that reproduces it.

Return to [Getting started](getting-started.md) or the [User guide](user-guide.md).
