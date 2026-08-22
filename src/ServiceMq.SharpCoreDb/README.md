# ServiceMq.SharpCoreDb

A [SharpCoreDB](https://github.com/MPCoreDeveloper/SharpCoreDB) storage provider for [ServiceMq](https://github.com/tylerjensen/ServiceMq).

ServiceMq stores queue state behind `IMessageStore`, and ships several implementations: files (the default), memory, SQLite, and this one. They are interchangeable — pick whichever fits your deployment. This package exists so SharpCoreDB is available as a choice; it is not a recommended replacement for the SQLite or file providers.

> **Requires .NET 10.0.** The SharpCoreDB NuGet package (currently `1.9.3`) targets `net10.0` (C# 14) only. Use `ServiceMq.Sqlite` or the built-in file store for `netstandard2.0` / `net8.0` consumers.

## Usage

```csharp
using var store = new SharpCoreDbMessageStore("queue-data", "your-master-password");

var queue = new MessageQueue(new MessageQueueOptions
{
    Name = "receiver",
    Address = new Address("receiver"),
    Storage = new StorageOptions
    {
        Durability = DurabilityMode.FlushToDisk,
        Provider = store
    }
});
```

The master password is required and has no default. It opens the SharpCoreDB database and derives the payload encryption key, so the same password must be supplied to reopen an existing store.

## What is in the store directory

| File | Purpose |
|---|---|
| `servicemq-store.json` | Store manifest: format version, key-derivation parameters, the per-store salt, and the active table generation. **Losing it loses every payload** — back it up with the data. |
| `servicemq.lock` | Ownership lock, held open for the life of the store. |
| `queue_items*.dat`, `meta.dat`, `.salt` | SharpCoreDB's own files. |

A store directory has **one owner at a time**. Opening a second `SharpCoreDbMessageStore` on the same directory — in the same process or another — throws `IOException` naming the directory. This matches how `MessageQueue` owns `StorageOptions.Provider`, and it is what lets the provider keep an in-memory key index that is authoritative for `Contains`, `GetKeys`, and `GetStatistics`. The lock is `FileShare.None`: mandatory on Windows, an advisory `flock` on Unix that .NET processes honor among themselves.

### Format and compatibility

Format **v1** is the first shipped format (7.1.0). A directory that holds rows but no manifest was created by the unreleased pre-manifest commit; it is rejected with `InvalidDataException` rather than silently re-keyed. Recreate it. A manifest with an unknown `formatVersion` is rejected with `NotSupportedException`.

## Encryption

SharpCoreDB 1.9.3 encrypts database metadata with the master password but writes table payloads to `*.dat` in the clear (`Storage.AppendBytes` ignores `EnableBatchEncryption` there). This provider therefore seals every payload itself:

- **AES-256-GCM**, envelope `version(1) | nonce(12) | tag(16) | ciphertext`, Base64-encoded.
- Key derived with **PBKDF2-HMAC-SHA256** (210,000 iterations; the count is recorded in the manifest so it can be raised for new stores later) over a random 16-byte per-store salt.
- The row's composite `area:key` is authenticated as associated data, so a ciphertext cannot be relocated to another row or storage area undetected.
- A wrong password, a tampered payload, or an unrecognized envelope raises `CryptographicException` (or its `AuthenticationTagMismatchException` subclass) rather than returning data, and is recorded in `LastException`.

This is independent of `StorageOptions.Protector`. ServiceMq's `AesStorageProtector` works with every provider and takes a real key rather than a password; if you configure both, payloads are simply encrypted twice.

## How deletes work (and why)

SharpCoreDB's directory-mode storage is append-oriented: inserts and updates are written through to the table data file, and in the current release (`1.9.3`) row removals are applied to the in-memory table but are not yet recorded in that file. In our testing, a row removed with `ITable.DeleteByPrimaryKey`, `IDatabase.DeleteByPrimaryKey`, SQL `DELETE`, or `Table.Delete(where)` was present again after the database was reopened, whether or not `Flush()`, `ForceSave()`, or `VacuumAsync()` had been called in between. `Table.CompactStorage()` does rewrite the file, but its behaviour on a table that had been written to in the same session was not yet consistent enough in our runs for a message queue to rely on, so the provider does not call it.

Since a queue cannot let a received, delivered, or purged message reappear after a restart, the provider keeps deletion durable on its own terms:

- **Deletes are tombstones.** `Delete`, `Purge`, and the source side of `Move` rewrite the row with `area = -1` and an empty value. That is an update, which SharpCoreDB persists, and tombstoned rows are invisible to every read API. Re-writing a tombstoned key updates the row in place (the primary key is still held, so an insert would be rejected).
- **Space is reclaimed by generational compaction.** Once tombstones number at least 4,096 *and* outnumber live rows, live rows are copied into a fresh table (`queue_items_g1`, `_g2`, …), the copy is flushed, the manifest's `table` is switched (the commit point), the old generation's file handle is released and its data file deleted. Because a table that has been written to in the current session is best left for the next open to drop, its catalogue entry is removed then. A crash at any point leaves a readable store: before the commit the new generation is an orphan, after it the old one is, and either is cleaned up at the next open.

This is a provider-level convention, not a change to the on-disk format SharpCoreDB owns, and it is intended to be temporary. Looking ahead, once SharpCoreDB records removals durably — for instance by appending delete markers to the table log and honouring them on load, or by offering a compaction step that is safe to invoke on an open table — the provider can return to physical deletes behind the same manifest version, recognising existing `area = -1` rows during a one-time compaction so no data migration is needed. We intend to share these observations with the SharpCoreDB maintainers; the engine is under active development and this is the kind of refinement a later release is well placed to make.

## Benchmark: SQLite vs SharpCoreDB

In-process `IMessageStore` numbers from the `ServiceMq.Benchmarks` console app, both providers measured sequentially in one process. Each operation is the **median of 3 samples** on fixtures rebuilt per sample, so no sample benefits from an earlier one having emptied the table or created the rows it touches: appends hit rows that already exist, each purge removes 500 freshly written rows, each move and delete sample has its own key range. Treat them as shape, not spec — they vary by hardware and disk.

**SharpCoreDB encrypts every payload here and SQLite does not**, so the write-side rows are not comparing equivalent work.

Environment for the table below: Windows 11 (10.0.26200) x64, .NET 10.0.11, 12 logical processors; Release build; 5,000 writes / 5,000 reads / 1,000 appends / 500 moves / 500 deletes / 500-row purges.

| Operation | SQLite (net8.0, unencrypted) | SharpCoreDB (net10.0, AES-256-GCM) |
|---|---|---|
| Write (FlushToDisk) | 1,374 ops/sec | 119 ops/sec |
| Write (Buffered + one Flush) | 1,361 ops/sec | 1,539 ops/sec |
| Append to existing row (FlushToDisk) | 1,430 ops/sec | 119 ops/sec |
| Read | 50,801 ops/sec | 74,296 ops/sec |
| Contains | 70,135 ops/sec | 2,712,379 ops/sec |
| GetStatistics | 235 ops/sec | 3,641 ops/sec |
| Move (Outgoing → DeadLetter) | 1,252 ops/sec | 60 ops/sec |
| Delete | 1,407 ops/sec | 127 ops/sec |
| Purge (500 rows, per row) | 247,463 rows/sec | 2,120 rows/sec |

What the numbers say:

- **Durable point writes are roughly an order of magnitude slower** (119 vs 1,374 ops/sec). SharpCoreDB flushes per operation and this provider encrypts per payload. If your queue does thousands of `FlushToDisk` writes per second, SQLite is the better fit.
- **Buffered writes erase that gap.** With `DurabilityMode.Buffered` and one flush at the end, SharpCoreDB's 1,539 ops/sec edges out SQLite's 1,361 — the SQLite provider runs `synchronous=FULL` per statement regardless of durability mode, so it gains nothing from buffering. The usual durability trade-off applies.
- **Reads, key lookups, and statistics are faster.** Point reads go through the direct `ITable` B-tree rather than the SQL parser; `Contains` and `GetStatistics` are served from the provider's in-memory index and never touch the table.
- **Deletes, moves, and purges are much slower.** Each is a tombstone rewrite (an encrypted-row update) plus a flush; a move also flushes the destination before touching the source, so it pays two. SQLite's one-statement `DELETE ... WHERE` purge is two orders of magnitude faster per row. These are the dead-letter, cleanup, and retention paths, not the hot path, but a queue with very high churn will feel it.

## Implementation notes

- A single table holds all six storage areas; the primary key is namespaced `"<area>:<key>"`.
- Because the area is only a key prefix, the provider maintains an in-memory per-area index of key → (length, created, modified). This supplies the ordinal `GetKeys` ordering that `IMessageStore` implementations are expected to provide (`SqliteMessageStore` gets it from `ORDER BY key`, the file and memory stores from an ordinal sort), keeps `Purge`/`GetStatistics` off the table scan path, and decides insert-vs-update. It is rebuilt by one full scan at open, updated after every successful mutation, and re-probed from the table for the affected key when a mutation fails part-way.
- SharpCoreDB exposes no multi-statement transaction, so `Move` writes and **flushes** the destination row before tombstoning the source. A crash in between leaves a recoverable duplicate rather than a lost message, regardless of how the engine orders unflushed writes.
- `Database.Load()` does not repopulate `DefaultExpressions` / `ColumnCheckExpressions` after deserialization, so a reopened table throws from `Table.Insert`. `RepairTableSchemaLists` normalizes those per-column lists at open; it can be removed once fixed upstream.

## Building and testing

The projects are `net10.0`-only, so the .NET 10 SDK is required to build `src/ServiceMq.slnx`. It still compiles the `netstandard2.0` / `net8.0` targets of ServiceMq and ServiceMq.Sqlite normally — shipped package targets are unchanged.

```
dotnet build src/ServiceMq.slnx -c Release
dotnet test src/ServiceMq.Tests.SharpCoreDb -c Release
dotnet run --project src/ServiceMq.Benchmarks -c Release
```

Both test projects use xunit.v3 through the VSTest adapter, so plain `dotnet test` works. That is deliberate: xunit.v3 defaults to the Microsoft Testing Platform runner, and on the .NET 10 SDK MTP requires a repository-wide `global.json` opt-in that applies to every test project at once. Setting `IsTestingPlatformApplication` to `false` in each test `.csproj` keeps the choice per project and leaves the .NET 10 requirement scoped to this package.

The SharpCoreDB test project includes fault-injection tests: an internal `IQueueTable` seam (visible via `InternalsVisibleTo`) lets tests throw at exact points inside `Move`, `Delete`, `Purge`, and `Write`, tamper with stored envelopes, and force compaction on small tables.

## Links

- **SharpCoreDB repository:** <https://github.com/MPCoreDeveloper/SharpCoreDB>
- **NuGet package:** [`SharpCoreDB`](https://www.nuget.org/packages/SharpCoreDB) — pinned to `1.9.3`. Update the version in the `.csproj` when a newer release ships.
- **Engine issues found during implementation:** [`docs/sharpcoredb-known-issues.md`](https://github.com/MPCoreDeveloper/SharpCoreDB/blob/main/docs/sharpcoredb-known-issues.md)
