# ServiceMq.SharpCoreDb

A [SharpCoreDB](https://github.com/MPCoreDeveloper/SharpCoreDB) storage provider for [ServiceMq](https://github.com/tylerjensen/ServiceMq).

> **Requires .NET 10.0.** The SharpCoreDB NuGet package (currently `1.9.3`) targets `net10.0` (C# 14) only. The SQLite provider (`ServiceMq.Sqlite`) remains the option for `netstandard2.0` / `net8.0` consumers.

## Why SharpCoreDB instead of SQLite?

SharpCoreDB is a lightweight, encrypted, file-based embedded database with SQL support, AES-256-GCM encryption, and modern C#. Compared to SQLite it offers:

| Capability | SharpCoreDB | SQLite |
|---|---|---|
| Built-in AES-256-GCM at-rest encryption | ✅ (see note below) | ❌ (third-party extension) |
| Single NuGet package, no native deps | ✅ | ⚠️ (native `e_sqlite3`) |
| .NET 10 / C# 14 native codebase | ✅ | C (via P/Invoke) |
| Direct B-tree table API (no SQL parse) | ✅ fast point reads | — |
| `netstandard2.0` / `net8.0` support | ❌ | ✅ |

### Encryption note

SharpCoreDB 1.9.3 encrypts the database metadata (with the master password) but **not** the table payload files by default (`Storage.AppendBytes` writes plaintext to `*.dat` — `EnableBatchEncryption` is a no-op there). This provider therefore encrypts every payload value itself with **AES-256-GCM** (key derived from the master password) before storing it, so queue messages are protected at rest. The same master password must be supplied again to reopen the store; a configurable default is provided (`SharpCoreDbMessageStore.DefaultMasterPassword`).

```csharp
// Default password (configurable constant)
var store = new SharpCoreDbMessageStore("queue-data");

// Or supply your own master password
var store = new SharpCoreDbMessageStore("queue-data", "my-super-secret-password");
```

## Benchmark: SQLite vs SharpCoreDB

The following in-process numbers were produced by the `ServiceMq.Benchmarks` console app (Release, .NET 10.0, 5,000 writes / 5,000 reads / 1,000 appends / 500 moves / 500 deletes). **SharpCoreDB includes provider-level AES-256-GCM payload encryption** in every write; SQLite does not encrypt.

| Operation | SQLite (net8.0) | SharpCoreDB (net10.0) | Winner |
|---|---|---|---|
| Write (FlushToDisk) | 1,672 ops/sec | 131 ops/sec | SQLite ⚡ |
| Append (FlushToDisk) | 1,739 ops/sec | 130 ops/sec | SQLite ⚡ |
| Read | 65,127 ops/sec | 98,424 ops/sec | SharpCoreDB ⚡ |
| Contains | 78,017 ops/sec | 109,108 ops/sec | SharpCoreDB ⚡ |
| Move | 1,694 ops/sec | 543 ops/sec | SQLite ⚡ |
| Delete | 1,740 ops/sec | 304 ops/sec | SQLite ⚡ |
| Purge | 9,504 ops/sec | 99 ops/sec | SQLite ⚡ |
| GetStatistics | 1,433 ops/sec | 76 ops/sec | SQLite ⚡ |

**Takeaways (consistent with [SharpCoreDB's own comparative benchmark](https://github.com/MPCoreDeveloper/SharpCoreDB/blob/main/docs/benchmarks/SHARPCOREDB_COMPARATIVE_BENCHMARKS.md)):**

- **SharpCoreDB wins point reads** — its direct B-tree `ITable` lookups (used by this provider) avoid SQL parser overhead.
- **SQLite wins heavy point writes** — SharpCoreDB's per-row durability flush and per-payload AES encryption are expensive per operation. For workloads with thousands of separate `FlushToDisk` writes (e.g. a busy queue), SQLite is the better fit.
- **SharpCoreDB's sweet spot** is bulk/batch ingestion plus built-in encryption. If you use `DurabilityMode.Buffered`/`MemoryOnly` and flush periodically (batching), the SharpCoreDB write gap narrows substantially and its read advantage + encryption-at-rest become the deciding factors.

### Adding the source

This repository is a fork/downstream usage of the SharpCoreDB engine. The SharpCoreDB source is maintained separately at:

- **SharpCoreDB repository:** <https://github.com/MPCoreDeveloper/SharpCoreDB>
- **NuGet package:** `SharpCoreDB` (https://www.nuget.org/packages/SharpCoreDB) — pinned to `1.9.3` in this project. Update the version when a newer release ships.

## Project layout

- `SharpCoreDbMessageStore.cs` — `IMessageStore` implementation (single `queue_items` table, area-prefixed primary keys, direct `ITable` API, AES-256-GCM payload encryption).
- `ServiceMq.SharpCoreDb.csproj` — net10.0 library package.
- Tests live in `../ServiceMq.Tests.SharpCoreDb` (net10.0, xunit.v3 + Microsoft Testing Platform).
- The benchmark lives in `../ServiceMq.Benchmarks` (net10.0 console app).
- SharpCoreDB engine issues found during implementation are tracked in the SharpCoreDB repository at `docs/sharpcoredb-known-issues.md` (https://github.com/MPCoreDeveloper/SharpCoreDB/blob/main/docs/sharpcoredb-known-issues.md).
