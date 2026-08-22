#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using SharpCoreDB;
using SharpCoreDB.Interfaces;

namespace ServiceMq
{
    /// <summary>
    /// SharpCoreDB storage provider for ServiceMq. Requires .NET 10.0 because the
    /// SharpCoreDB NuGet package (1.9.3) targets net10.0 only.
    /// <para>
    /// Uses SharpCoreDB <b>directory mode</b> with the direct <c>ITable</c> API. A single
    /// table holds all storage areas; the primary key is namespaced
    /// <c>"&lt;area&gt;:&lt;key&gt;"</c> so the same logical key can exist in several areas
    /// (incoming, outgoing, dead-letter, ...).
    /// </para>
    /// <para>
    /// <b>Why directory mode and the direct ITable API?</b> Per the SharpCoreDB benchmark
    /// documentation, the direct <c>ITable</c> primary-key lookups (B-tree) are the fastest
    /// path and avoid the SQL parser overhead; single-file (.scdb) tables however use a
    /// no-op B-tree index, so directory mode is required for correct point operations.
    /// </para>
    /// <para>
    /// <b>Deletes are tombstones.</b> SharpCoreDB 1.9.3's append-oriented storage writes
    /// inserts and updates through to the table data file, but row removals (via
    /// <c>DeleteByPrimaryKey</c> or SQL <c>DELETE</c>, with or without <c>Flush</c>,
    /// <c>ForceSave</c>, or <c>Vacuum</c>) are applied in memory and are present again on the
    /// next open; <c>Table.CompactStorage()</c> was not yet consistent enough in our runs to
    /// rely on. The provider therefore marks a deleted row with <c>area = -1</c> and an empty
    /// value, which is durable, and reclaims space by periodically rewriting live rows into a
    /// fresh generation of the table (see <see cref="Compact"/>). Tombstoned rows are invisible
    /// to every read API. This is intended to give way to physical deletes once a SharpCoreDB
    /// release records removals durably; see the package README.
    /// </para>
    /// <para>
    /// <b>Ownership.</b> A store directory has exactly one open <see cref="SharpCoreDbMessageStore"/>
    /// at a time, enforced with an exclusive lock file. The provider keeps an in-memory per-area
    /// key index that is authoritative for <see cref="Contains"/>, <see cref="GetKeys"/>, and
    /// <see cref="GetStatistics"/>; a second instance would not see this one's writes. This
    /// matches how <see cref="MessageQueue"/> owns <see cref="StorageOptions.Provider"/>.
    /// </para>
    /// <para>
    /// <b>Payload encryption.</b> SharpCoreDB 1.9.3 writes table data unencrypted to
    /// <c>*.dat</c>, so this provider encrypts every payload itself with AES-256-GCM. The key
    /// is derived with PBKDF2-HMAC-SHA256 from the caller-supplied master password and a
    /// random per-store salt recorded in the store manifest (<c>servicemq-store.json</c>)
    /// alongside the data. This is independent of, and composes with,
    /// <see cref="StorageOptions.Protector"/>: configuring an <see cref="IStorageProtector"/>
    /// as well simply encrypts the payload twice.
    /// </para>
    /// </summary>
    public sealed class SharpCoreDbMessageStore : IMessageStore
    {
        private const string DefaultTableName = "queue_items";
        private const string GenerationSuffix = "_g";
        private const string ManifestFileName = "servicemq-store.json";
        private const string LockFileName = "servicemq.lock";
        private const int CurrentFormatVersion = 1;
        private const string KeyDerivationFunction = "PBKDF2-HMAC-SHA256";
        private const int SaltLength = 16;
        private const int DefaultKeyDerivationIterations = 210000;
        private const byte EnvelopeVersion = 1;
        private const int NonceLength = 12;
        private const int TagLength = 16;
        private const int TombstoneArea = -1;

        private static readonly JsonSerializerOptions ManifestJson = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            WriteIndented = true
        };

        private readonly Lock syncRoot = new();
        private readonly FileStream lockStream;
        private readonly ServiceProvider serviceProvider;
        private readonly IDatabase database;
        private readonly AesGcm payloadCipher;
        private readonly Func<IQueueTable, IQueueTable>? tableDecorator;
        private readonly StoreManifest manifest;

        // Swapped by Compact(), which moves live rows into a new generation of the table.
        // rawTable is kept so the old generation's file handle can be released before it is
        // dropped (SharpCoreDB holds a table's .dat open once it has been written to).
        private IQueueTable table;
        private ITable rawTable;
        private string tableName;

        /// <summary>
        /// Per-area key metadata, ordinally sorted. The area is only a primary-key prefix in
        /// SharpCoreDB, so without this index every key listing, purge, and statistics call
        /// would scan and materialize the whole table across all six areas. Keeping it also
        /// satisfies the <see cref="IMessageStore"/> ordering contract that
        /// <see cref="GetKeys"/> returns keys in ordinal order. It is updated immediately after
        /// every successful table mutation and re-probed from the table when one fails.
        /// </summary>
        private readonly Dictionary<StorageArea, SortedDictionary<string, RowMetadata>> index = new();

        /// <summary>
        /// Composite keys of rows that physically exist in the table as tombstones. A write to
        /// one of these must update rather than insert (the primary key is still taken).
        /// </summary>
        private readonly HashSet<string> tombstones = new(StringComparer.Ordinal);

        private Exception? lastException;
        private bool disposed;

        public Exception LastException { get { return lastException!; } }
        public string DatabasePath { get; private set; } = string.Empty;

        /// <summary>
        /// Tombstones are rewritten away once at least this many exist and they outnumber
        /// live rows, so compaction cost stays proportional to live data. Internal so tests
        /// can force compaction on a small table.
        /// </summary>
        internal int CompactionMinimumTombstones { get; set; } = 4096;

        /// <param name="databasePath">Directory that holds the SharpCoreDB database.</param>
        /// <param name="masterPassword">
        /// Master password. It opens the SharpCoreDB database and, via PBKDF2, derives the
        /// payload encryption key. There is deliberately no default: a shipped constant would
        /// be public knowledge and the resulting encryption at rest would protect nothing.
        /// The same password must be supplied to reopen an existing store.
        /// </param>
        public SharpCoreDbMessageStore(string databasePath, string masterPassword)
            : this(databasePath, masterPassword, null, null)
        {
        }

        public SharpCoreDbMessageStore(string databasePath, string masterPassword, DatabaseConfig? config)
            : this(databasePath, masterPassword, config, null)
        {
        }

        /// <summary>
        /// Test seam. <paramref name="tableDecorator"/> wraps the production
        /// <see cref="IQueueTable"/> so tests can inject faults or tampered rows.
        /// </summary>
        internal SharpCoreDbMessageStore(string databasePath, string masterPassword, DatabaseConfig? config,
            Func<IQueueTable, IQueueTable>? tableDecorator)
        {
            if (string.IsNullOrWhiteSpace(databasePath)) throw new ArgumentException("A database path is required.", nameof(databasePath));
            if (string.IsNullOrWhiteSpace(masterPassword)) throw new ArgumentException("A master password is required for SharpCoreDB encryption.", nameof(masterPassword));

            DatabasePath = Path.GetFullPath(databasePath);
            Directory.CreateDirectory(DatabasePath);
            this.tableDecorator = tableDecorator;

            // Everything owned is built into locals and only promoted to fields once the whole
            // open succeeds, so a failure part-way (wrong password, unsupported manifest, a
            // SharpCoreDB fault) can release exactly what was acquired, in reverse order.
            FileStream? lockFile = null;
            ServiceProvider? provider = null;
            IDatabase? db = null;
            AesGcm? cipher = null;
            try
            {
                lockFile = AcquireLock(DatabasePath);
                var loaded = TryLoadManifest(DatabasePath);

                provider = new ServiceCollection()
                    .AddSharpCoreDB()
                    .BuildServiceProvider();

                var factory = new DatabaseFactory(provider);
                // Directory mode. EnableBatchEncryption MUST be true: SharpCoreDB append
                // storage only encrypts table data files when this flag is set
                // (Storage.Append.cs). Payloads are additionally sealed by this provider.
                db = factory.Create(DatabasePath, masterPassword, false, WithBatchEncryption(config));

                tableName = loaded?.Table ?? DefaultTableName;
                DropLeftoverGenerations(db, DatabasePath, tableName);
                table = OpenTable(db, tableName, out rawTable);

                // The index needs only keys and metadata, not the cipher, so it is built before
                // the manifest decision: an unversioned directory is only rejected if it holds rows.
                RebuildIndex();
                manifest = ResolveManifest(DatabasePath, loaded, IndexedRowCount() + tombstones.Count);

                var payloadKey = DerivePayloadKey(masterPassword, manifest);
                cipher = new AesGcm(payloadKey, TagLength);
                CryptographicOperations.ZeroMemory(payloadKey);

                table.Flush();

                lockStream = lockFile;
                serviceProvider = provider;
                database = db;
                payloadCipher = cipher;
            }
            catch (Exception ex)
            {
                try { db?.DisposeAsync().AsTask().GetAwaiter().GetResult(); } catch { }
                try { cipher?.Dispose(); } catch { }
                try { provider?.Dispose(); } catch { }
                try { lockFile?.Dispose(); } catch { }
                lastException = ex;
                throw;
            }

            // Reclaim space left by a previous session if it is due. A failure here leaves a
            // consistent store (see Compact) and must not fail the open.
            try { MaybeCompact(); }
            catch (Exception ex) { lastException = ex; }
        }

        public IReadOnlyList<string> GetKeys(StorageArea area)
        {
            lock (syncRoot)
            {
                try { return new List<string>(index[area].Keys); }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public bool Contains(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                try { return index[area].ContainsKey(key); }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public StorageEntry Read(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                try
                {
                    var rowId = CompositeKey(area, key);
                    var row = table.FindByPrimaryKey(rowId);
                    if (row == null || IsTombstone(row)) throw new KeyNotFoundException(key);

                    var metadata = index[area].TryGetValue(key, out var known)
                        ? known
                        : RowMetadata.FromRow(row);

                    return new StorageEntry
                    {
                        Key = key,
                        Value = Unprotect(GetValue(row, "value") ?? string.Empty, rowId),
                        Length = metadata.Length,
                        CreatedUtc = new DateTime(metadata.CreatedTicks, DateTimeKind.Utc),
                        LastModifiedUtc = new DateTime(metadata.ModifiedTicks, DateTimeKind.Utc)
                    };
                }
                // A missing key is ordinary control flow, not a storage fault. Recording it in
                // LastException would leave the owning queue permanently Cautioned.
                catch (KeyNotFoundException) { throw; }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void Write(StorageArea area, string key, string value, DurabilityMode durability)
        {
            lock (syncRoot)
            {
                try
                {
                    var now = DateTime.UtcNow.Ticks;
                    var normalized = value ?? string.Empty;
                    var length = (long)Encoding.UTF8.GetByteCount(normalized);
                    var rowId = CompositeKey(area, key);
                    var areaIndex = index[area];
                    var stored = Protect(normalized, rowId);

                    if (areaIndex.TryGetValue(key, out var existing))
                    {
                        table.UpdateByPrimaryKey(rowId, new()
                        {
                            ["area"] = (int)area,
                            ["value"] = stored,
                            ["length"] = length,
                            ["modified_ticks"] = now
                        });
                        areaIndex[key] = new RowMetadata(length, existing.CreatedTicks, now);
                    }
                    else
                    {
                        Upsert(rowId, area, stored, length, now, now);
                        areaIndex[key] = new RowMetadata(length, now, now);
                    }
                    if (durability == DurabilityMode.FlushToDisk) table.Flush();
                }
                catch (Exception ex) { ResyncIndex(area, key); lastException = ex; throw; }
            }
        }

        public void Append(StorageArea area, string key, string value, DurabilityMode durability)
        {
            lock (syncRoot)
            {
                try
                {
                    var normalized = value ?? string.Empty;
                    var areaIndex = index[area];
                    var rowId = CompositeKey(area, key);

                    Dictionary<string, object>? existingRow = null;
                    if (areaIndex.TryGetValue(key, out var existing)) existingRow = table.FindByPrimaryKey(rowId);
                    if (existingRow == null || IsTombstone(existingRow))
                    {
                        Write(area, key, normalized, durability);
                        return;
                    }

                    var now = DateTime.UtcNow.Ticks;
                    var combined = Unprotect(GetValue(existingRow, "value") ?? string.Empty, rowId) +
                        Environment.NewLine + normalized;
                    var length = (long)Encoding.UTF8.GetByteCount(combined);

                    table.UpdateByPrimaryKey(rowId, new()
                    {
                        ["area"] = (int)area,
                        ["value"] = Protect(combined, rowId),
                        ["length"] = length,
                        ["modified_ticks"] = now
                    });
                    areaIndex[key] = new RowMetadata(length, existing.CreatedTicks, now);
                    if (durability == DurabilityMode.FlushToDisk) table.Flush();
                }
                catch (Exception ex) { ResyncIndex(area, key); lastException = ex; throw; }
            }
        }

        public void Delete(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                try
                {
                    if (!index[area].ContainsKey(key)) return;
                    Tombstone(area, key, DateTime.UtcNow.Ticks);
                    table.Flush();
                    MaybeCompact();
                }
                catch (Exception ex) { ResyncIndex(area, key); lastException = ex; throw; }
            }
        }

        public void Move(StorageArea source, StorageArea destination, string key)
        {
            lock (syncRoot)
            {
                try
                {
                    if (source == destination) return;
                    var sourceRowId = CompositeKey(source, key);
                    var destinationRowId = CompositeKey(destination, key);
                    var row = table.FindByPrimaryKey(sourceRowId);
                    if (row == null || IsTombstone(row)) return;

                    var metadata = index[source].TryGetValue(key, out var known)
                        ? known
                        : RowMetadata.FromRow(row);
                    var now = DateTime.UtcNow.Ticks;

                    // The composite key is authenticated into the ciphertext, so a moved
                    // payload has to be re-sealed under the destination row id.
                    var payload = Unprotect(GetValue(row, "value") ?? string.Empty, sourceRowId);

                    // Mirror the SQLite provider semantics: any pre-existing destination entry
                    // is replaced. SharpCoreDB exposes no multi-statement transaction, so the
                    // destination is written and flushed before the source is removed: a crash
                    // anywhere in between leaves a recoverable duplicate, never a lost message,
                    // regardless of how SharpCoreDB orders unflushed writes. The index is
                    // updated after each step so a failure part-way leaves it matching the table.
                    if (index[destination].ContainsKey(key))
                    {
                        table.UpdateByPrimaryKey(destinationRowId, new()
                        {
                            ["area"] = (int)destination,
                            ["value"] = Protect(payload, destinationRowId),
                            ["length"] = metadata.Length,
                            ["created_ticks"] = metadata.CreatedTicks,
                            ["modified_ticks"] = now
                        });
                    }
                    else
                    {
                        Upsert(destinationRowId, destination, Protect(payload, destinationRowId),
                            metadata.Length, metadata.CreatedTicks, now);
                    }
                    index[destination][key] = new RowMetadata(metadata.Length, metadata.CreatedTicks, now);
                    table.Flush();

                    Tombstone(source, key, now);
                    table.Flush();
                    MaybeCompact();
                }
                catch (Exception ex)
                {
                    ResyncIndex(source, key);
                    ResyncIndex(destination, key);
                    lastException = ex;
                    throw;
                }
            }
        }

        public void Purge(StorageArea area, DateTime olderThanUtc)
        {
            lock (syncRoot)
            {
                try
                {
                    var areaIndex = index[area];
                    List<string> expired = [];
                    foreach (var pair in areaIndex)
                    {
                        if (pair.Value.ModifiedTicks < olderThanUtc.Ticks) expired.Add(pair.Key);
                    }
                    if (expired.Count == 0) return;

                    var now = DateTime.UtcNow.Ticks;
                    foreach (var key in expired) Tombstone(area, key, now);
                    table.Flush();
                    MaybeCompact();
                }
                catch (Exception ex)
                {
                    // Many keys may have been touched; re-derive the whole index from the table.
                    try { RebuildIndex(); } catch { }
                    lastException = ex;
                    throw;
                }
            }
        }

        public StorageAreaStatistics GetStatistics(StorageArea area)
        {
            lock (syncRoot)
            {
                try
                {
                    var areaIndex = index[area];
                    if (areaIndex.Count == 0) return new StorageAreaStatistics { Count = 0, Bytes = 0, OldestUtc = null };

                    long bytes = 0;
                    var oldest = long.MaxValue;
                    foreach (var metadata in areaIndex.Values)
                    {
                        bytes += metadata.Length;
                        if (metadata.CreatedTicks < oldest) oldest = metadata.CreatedTicks;
                    }

                    return new StorageAreaStatistics
                    {
                        Count = areaIndex.Count,
                        Bytes = bytes,
                        OldestUtc = new DateTime(oldest, DateTimeKind.Utc)
                    };
                }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void Flush()
        {
            lock (syncRoot)
            {
                try { table.Flush(); }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void ClearException() { lastException = null; }

        public void Dispose()
        {
            lock (syncRoot)
            {
                if (disposed) return;
                disposed = true;
                // Each owned resource is released even if an earlier one throws; the lock file
                // goes last so the directory is only handed back once everything else is closed.
                try
                {
                    try { database.Flush(); } catch { }
                    database.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
                finally
                {
                    try { payloadCipher.Dispose(); }
                    finally
                    {
                        try { serviceProvider.Dispose(); }
                        finally { lockStream.Dispose(); }
                    }
                }
            }
        }

        // ----- rows, tombstones, compaction ----------------------------------------------

        /// <summary>
        /// Inserts a row, or updates it if the primary key is held by a tombstone (SharpCoreDB
        /// raises a primary key violation on insert even though the row is logically gone).
        /// </summary>
        private void Upsert(string rowId, StorageArea area, string storedValue, long length, long createdTicks, long modifiedTicks)
        {
            if (tombstones.Remove(rowId))
            {
                table.UpdateByPrimaryKey(rowId, new()
                {
                    ["area"] = (int)area,
                    ["value"] = storedValue,
                    ["length"] = length,
                    ["created_ticks"] = createdTicks,
                    ["modified_ticks"] = modifiedTicks
                });
            }
            else
            {
                table.Insert(new()
                {
                    ["key"] = rowId,
                    ["area"] = (int)area,
                    ["value"] = storedValue,
                    ["length"] = length,
                    ["created_ticks"] = createdTicks,
                    ["modified_ticks"] = modifiedTicks
                });
            }
        }

        /// <summary>
        /// Logically deletes a row by rewriting it as a tombstone, which SharpCoreDB does
        /// persist, then drops it from the index.
        /// </summary>
        private void Tombstone(StorageArea area, string key, long now)
        {
            var rowId = CompositeKey(area, key);
            table.UpdateByPrimaryKey(rowId, new()
            {
                ["area"] = TombstoneArea,
                ["value"] = string.Empty,
                ["length"] = 0L,
                ["modified_ticks"] = now
            });
            index[area].Remove(key);
            tombstones.Add(rowId);
        }

        private static bool IsTombstone(Dictionary<string, object> row) => GetInt64(row, "area") == TombstoneArea;

        private void MaybeCompact()
        {
            if (tombstones.Count < CompactionMinimumTombstones || tombstones.Count < IndexedRowCount()) return;
            Compact();
        }

        /// <summary>
        /// Reclaims the space held by tombstones by copying live rows into a new generation of
        /// the table and dropping the old one. Each step is durable before the next, and the
        /// manifest update is the commit point, so a crash at any point leaves a readable store:
        /// before the commit the new generation is an orphan that the next open drops; after it
        /// the old generation is. Payloads are copied sealed, so the cipher is not involved.
        /// </summary>
        private void Compact()
        {
            var nextName = NextGenerationName(tableName);
            var next = OpenTable(database, nextName, out var nextRaw);
            try
            {
                foreach (var row in table.Select())
                {
                    if (IsTombstone(row)) continue;
                    var composite = GetValue(row, "key");
                    if (composite == null) continue;
                    next.Insert(new()
                    {
                        ["key"] = composite,
                        ["area"] = (int)GetInt64(row, "area"),
                        ["value"] = GetValue(row, "value") ?? string.Empty,
                        ["length"] = GetInt64(row, "length"),
                        ["created_ticks"] = GetInt64(row, "created_ticks"),
                        ["modified_ticks"] = GetInt64(row, "modified_ticks")
                    });
                }
                next.Flush();
            }
            catch
            {
                try { DropTable(database, nextName); } catch { }
                throw;
            }

            var previousName = tableName;
            var previousRaw = rawTable;
            manifest.Table = nextName;
            WriteManifest(DatabasePath, manifest);   // commit point

            table = next;
            rawTable = nextRaw;
            tableName = nextName;
            tombstones.Clear();

            // The previous generation cannot be dropped in this session: SharpCoreDB keeps a
            // written table's .dat open, DROP TABLE against that open file de-catalogues the
            // table but orphans the file, and DROP after disposing the table throws. So the
            // handle is released and the data file deleted here to reclaim the space now; the
            // catalogue entry is removed by DropLeftoverGenerations at the next open, where a
            // fileless table drops cleanly. The store is already consistent either way.
            try
            {
                (previousRaw as IDisposable)?.Dispose();
                File.Delete(Path.Combine(DatabasePath, previousName + ".dat"));
            }
            catch { }
        }

        private IQueueTable OpenTable(IDatabase db, string name, out ITable raw)
        {
            EnsureSchema(db, name);
            if (!db.TryGetTable(name, out raw))
                throw new InvalidOperationException("Failed to initialize the " + name + " table in SharpCoreDB.");
            RepairTableSchemaLists(raw);
            IQueueTable queueTable = new SharpCoreDbQueueTable(db, raw);
            return tableDecorator == null ? queueTable : tableDecorator(queueTable);
        }

        /// <summary>
        /// Drops table generations other than the active one, and deletes generation data files
        /// that no catalogued table owns. Either can only exist if a previous compaction was
        /// interrupted, or its final DROP failed, on either side of the manifest commit. Nothing
        /// here has been written in this session, so the engine holds no handle on it yet.
        /// </summary>
        private static void DropLeftoverGenerations(IDatabase db, string databasePath, string activeName)
        {
            var catalogued = new HashSet<string>(StringComparer.Ordinal);
            List<string> leftovers = [];
            foreach (var info in db.GetTables())
            {
                var name = info.Name;
                catalogued.Add(name);
                if (string.Equals(name, activeName, StringComparison.Ordinal)) continue;
                if (IsGenerationName(name)) leftovers.Add(name);
            }
            foreach (var name in leftovers) DropTable(db, name);
            if (leftovers.Count > 0) db.Flush();

            foreach (var file in Directory.GetFiles(databasePath, DefaultTableName + "*.dat"))
            {
                var stem = Path.GetFileNameWithoutExtension(file);
                if (!IsGenerationName(stem) || catalogued.Contains(stem)) continue;
                try { File.Delete(file); } catch (IOException) { }
            }
        }

        private static bool IsGenerationName(string name) =>
            string.Equals(name, DefaultTableName, StringComparison.Ordinal) ||
            name.StartsWith(DefaultTableName + GenerationSuffix, StringComparison.Ordinal);

        private static void DropTable(IDatabase db, string name) => db.ExecuteSQL("DROP TABLE " + name);

        private static string NextGenerationName(string current)
        {
            var generation = 0;
            var marker = current.LastIndexOf(GenerationSuffix, StringComparison.Ordinal);
            if (marker >= 0) int.TryParse(current.AsSpan(marker + GenerationSuffix.Length), NumberStyles.Integer, CultureInfo.InvariantCulture, out generation);
            return DefaultTableName + GenerationSuffix + (generation + 1).ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Returns a copy of <paramref name="source"/> with
        /// <see cref="DatabaseConfig.EnableBatchEncryption"/> forced on, which SharpCoreDB
        /// requires in order to encrypt table data files at rest. The copy is reflective so
        /// options added by a future SharpCoreDB release carry over instead of being silently
        /// reset to their defaults.
        /// </summary>
        private static DatabaseConfig WithBatchEncryption(DatabaseConfig? source)
        {
            var clone = new DatabaseConfig { EnableBatchEncryption = true };
            if (source == null) return clone;

            // DatabaseConfig properties are init-only, so the copy goes through reflection
            // (PropertyInfo.SetValue invokes an init accessor the same way a serializer does).
            // EnableBatchEncryption is deliberately not copied: it is forced on above.
            foreach (var property in typeof(DatabaseConfig).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!property.CanRead || !property.CanWrite || property.GetIndexParameters().Length != 0) continue;
                if (property.Name == nameof(DatabaseConfig.EnableBatchEncryption)) continue;
                property.SetValue(clone, property.GetValue(source));
            }
            return clone;
        }

        private static void EnsureSchema(IDatabase db, string name)
        {
            // NOTE: BIGINT (DataType.Long/Int64) is required for length/created_ticks/
            // modified_ticks because DateTime.Ticks (~6.4e17) overflows DataType.Integer
            // (Int32). area stays INTEGER (Int32); -1 marks a tombstone.
            db.ExecuteSQL("CREATE TABLE IF NOT EXISTS " + name + " (" +
                "key TEXT PRIMARY KEY, " +
                "area INTEGER NOT NULL, " +
                "value TEXT NOT NULL, " +
                "length BIGINT NOT NULL, " +
                "created_ticks BIGINT NOT NULL, " +
                "modified_ticks BIGINT NOT NULL)");
        }

        /// <summary>
        /// SharpCoreDB <c>Database.Load()</c> fills IsAuto/IsNotNull/DefaultValues/
        /// ColumnCollations after deserialization but NOT DefaultExpressions or
        /// ColumnCheckExpressions. A reopened table then throws ArgumentOutOfRangeException
        /// from Table.Insert/Update. This normalizes all per-column lists to the column count
        /// (mirroring the pattern SharpCoreDB uses itself).
        /// </summary>
        private static void RepairTableSchemaLists(ITable table)
        {
            if (table is not SharpCoreDB.DataStructures.Table concrete) return;

            var count = concrete.Columns.Count;
            while (concrete.IsAuto.Count < count) concrete.IsAuto.Add(false);
            while (concrete.IsNotNull.Count < count) concrete.IsNotNull.Add(false);
            while (concrete.DefaultValues.Count < count) concrete.DefaultValues.Add(null);
            while (concrete.DefaultExpressions.Count < count) concrete.DefaultExpressions.Add(null);
            while (concrete.ColumnCheckExpressions.Count < count) concrete.ColumnCheckExpressions.Add(null);
            while (concrete.ColumnCollations.Count < count) concrete.ColumnCollations.Add(CollationType.Binary);
        }

        // ----- index ------------------------------------------------------------------------

        /// <summary>
        /// Reads every row and rebuilds the per-area key index and tombstone set from scratch.
        /// Called once at open and again if a multi-key mutation (<see cref="Purge"/>) fails
        /// part-way.
        /// </summary>
        private void RebuildIndex()
        {
            foreach (StorageArea area in Enum.GetValues<StorageArea>())
                index[area] = new SortedDictionary<string, RowMetadata>(StringComparer.Ordinal);
            tombstones.Clear();

            foreach (var row in table.Select())
            {
                var composite = GetValue(row, "key");
                if (composite == null) continue;
                if (IsTombstone(row)) { tombstones.Add(composite); continue; }
                if (!TrySplitCompositeKey(composite, out var area, out var key)) continue;
                index[area][key] = RowMetadata.FromRow(row);
            }
        }

        /// <summary>
        /// Re-derives one index entry from the table after a mutation failed, so the index
        /// reflects whatever the table actually holds. Never throws: it runs inside catch
        /// blocks and must not mask the original failure.
        /// </summary>
        private void ResyncIndex(StorageArea area, string key)
        {
            try
            {
                var rowId = CompositeKey(area, key);
                var row = table.FindByPrimaryKey(rowId);
                if (row == null)
                {
                    index[area].Remove(key);
                    tombstones.Remove(rowId);
                }
                else if (IsTombstone(row))
                {
                    index[area].Remove(key);
                    tombstones.Add(rowId);
                }
                else
                {
                    index[area][key] = RowMetadata.FromRow(row);
                    tombstones.Remove(rowId);
                }
            }
            catch { }
        }

        private long IndexedRowCount()
        {
            long count = 0;
            foreach (var areaIndex in index.Values) count += areaIndex.Count;
            return count;
        }

        private static string CompositeKey(StorageArea area, string key) =>
            ((int)area).ToString(CultureInfo.InvariantCulture) + ":" + key;

        private static bool TrySplitCompositeKey(string composite, out StorageArea area, out string key)
        {
            area = default;
            key = string.Empty;
            var separator = composite.IndexOf(':');
            if (separator <= 0) return false;
            if (!int.TryParse(composite.AsSpan(0, separator), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)) return false;
            if (!Enum.IsDefined(typeof(StorageArea), value)) return false;
            area = (StorageArea)value;
            key = composite.Substring(separator + 1);
            return true;
        }

        // ----- lock and manifest ------------------------------------------------------------

        /// <summary>
        /// Takes the exclusive ownership lock for the store directory. Acquired before
        /// SharpCoreDB opens so a second opener fails here with a clear message rather than
        /// somewhere inside the engine. <c>FileShare.None</c> is a mandatory lock on Windows
        /// and an advisory <c>flock</c> on Unix, which .NET processes honor among themselves.
        /// </summary>
        private static FileStream AcquireLock(string databasePath)
        {
            var lockPath = Path.Combine(databasePath, LockFileName);
            try
            {
                return new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException ex)
            {
                throw new IOException(
                    "The SharpCoreDB store at '" + databasePath + "' is already open in this or another process. " +
                    "SharpCoreDbMessageStore requires exclusive ownership of its directory.", ex);
            }
        }

        /// <summary>
        /// Loads <c>servicemq-store.json</c> if present. Returns null when the directory has no
        /// manifest yet; the caller decides whether that means "new store" or "unsupported".
        /// </summary>
        private static StoreManifest? TryLoadManifest(string databasePath)
        {
            var manifestPath = Path.Combine(databasePath, ManifestFileName);
            if (!File.Exists(manifestPath)) return null;

            StoreManifest? manifest;
            try { manifest = JsonSerializer.Deserialize<StoreManifest>(File.ReadAllBytes(manifestPath), ManifestJson); }
            catch (JsonException ex)
            {
                throw new InvalidDataException("The ServiceMq store manifest at " + manifestPath + " is not valid JSON.", ex);
            }
            if (manifest == null)
                throw new InvalidDataException("The ServiceMq store manifest at " + manifestPath + " is empty.");
            return manifest;
        }

        /// <summary>
        /// Validates an existing manifest, or creates one for a directory that has no rows yet.
        /// A directory with rows but no manifest predates the manifest (the unreleased
        /// pre-7.1.0 layout) and is rejected rather than silently re-keyed: every payload in it
        /// would fail authentication under a freshly generated salt.
        /// </summary>
        private static StoreManifest ResolveManifest(string databasePath, StoreManifest? existing, long rowCount)
        {
            var manifestPath = Path.Combine(databasePath, ManifestFileName);
            if (existing != null)
            {
                if (existing.FormatVersion != CurrentFormatVersion)
                    throw new NotSupportedException(
                        "The ServiceMq store at '" + databasePath + "' uses formatVersion " + existing.FormatVersion +
                        ", which is not supported by this version of ServiceMq.SharpCoreDb (supports " + CurrentFormatVersion + ").");
                if (!string.Equals(existing.Kdf, KeyDerivationFunction, StringComparison.Ordinal))
                    throw new NotSupportedException(
                        "The ServiceMq store at '" + databasePath + "' uses key derivation '" + existing.Kdf +
                        "', which is not supported by this version of ServiceMq.SharpCoreDb.");
                if (existing.Iterations <= 0)
                    throw new InvalidDataException("The ServiceMq store manifest at " + manifestPath + " has an invalid iteration count.");
                if (existing.DecodeSalt().Length != SaltLength)
                    throw new InvalidDataException("The ServiceMq store manifest at " + manifestPath + " has an invalid salt; expected " + SaltLength + " bytes.");
                existing.Table ??= DefaultTableName;
                return existing;
            }

            if (rowCount > 0)
                throw new InvalidDataException(
                    "The ServiceMq store at '" + databasePath + "' holds " + rowCount + " row(s) but has no " + ManifestFileName + ". " +
                    "It was created by a pre-release layout that this version cannot read; there is no migration. " +
                    "Recreate the store in a new directory.");

            var manifest = new StoreManifest
            {
                FormatVersion = CurrentFormatVersion,
                Kdf = KeyDerivationFunction,
                Iterations = DefaultKeyDerivationIterations,
                EnvelopeVersion = EnvelopeVersion,
                Salt = Convert.ToBase64String(RandomNumberGenerator.GetBytes(SaltLength)),
                Table = DefaultTableName
            };
            WriteManifest(databasePath, manifest);
            return manifest;
        }

        /// <summary>
        /// Writes the manifest atomically (temp file, flush to disk, rename over). Losing this
        /// file makes every stored payload undecryptable, so a crash mid-write must leave the
        /// previous copy intact. The exclusive lock is already held, so there is no writer race.
        /// </summary>
        private static void WriteManifest(string databasePath, StoreManifest manifest)
        {
            var manifestPath = Path.Combine(databasePath, ManifestFileName);
            var temporaryPath = manifestPath + ".tmp";
            using (var stream = new FileStream(temporaryPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                stream.Write(JsonSerializer.SerializeToUtf8Bytes(manifest, ManifestJson));
                stream.Flush(true);
            }
            File.Move(temporaryPath, manifestPath, overwrite: true);
        }

        private static byte[] DerivePayloadKey(string masterPassword, StoreManifest manifest) =>
            Rfc2898DeriveBytes.Pbkdf2(Encoding.UTF8.GetBytes(masterPassword), manifest.DecodeSalt(),
                manifest.Iterations, HashAlgorithmName.SHA256, 32);

        // ----- envelope ---------------------------------------------------------------------

        /// <summary>
        /// Seals a payload with AES-256-GCM. Envelope: version(1) | nonce(12) | tag(16) |
        /// ciphertext, Base64-encoded. The composite row key is authenticated as associated
        /// data, so a ciphertext cannot be moved between rows or areas undetected.
        /// </summary>
        private string Protect(string plaintext, string associatedData)
        {
            var plain = Encoding.UTF8.GetBytes(plaintext);
            var nonce = RandomNumberGenerator.GetBytes(NonceLength);
            var cipher = new byte[plain.Length];
            var tag = new byte[TagLength];
            payloadCipher.Encrypt(nonce, plain, cipher, tag, Encoding.UTF8.GetBytes(associatedData));

            var buffer = new byte[1 + NonceLength + TagLength + cipher.Length];
            buffer[0] = EnvelopeVersion;
            Buffer.BlockCopy(nonce, 0, buffer, 1, NonceLength);
            Buffer.BlockCopy(tag, 0, buffer, 1 + NonceLength, TagLength);
            Buffer.BlockCopy(cipher, 0, buffer, 1 + NonceLength + TagLength, cipher.Length);
            return Convert.ToBase64String(buffer);
        }

        /// <summary>
        /// Opens an envelope produced by <see cref="Protect"/>. Throws
        /// <see cref="CryptographicException"/> on a wrong master password, a tampered
        /// payload, or an unrecognized envelope.
        /// </summary>
        private string Unprotect(string storedValue, string associatedData)
        {
            byte[] buffer;
            try { buffer = Convert.FromBase64String(storedValue); }
            catch (FormatException ex)
            {
                throw new CryptographicException("The stored ServiceMq payload is not a valid encrypted envelope.", ex);
            }

            if (buffer.Length < 1 + NonceLength + TagLength || buffer[0] != EnvelopeVersion)
                throw new CryptographicException("The stored ServiceMq payload is not a recognized encrypted envelope.");

            var cipher = buffer.AsSpan(1 + NonceLength + TagLength);
            var plain = new byte[cipher.Length];
            payloadCipher.Decrypt(buffer.AsSpan(1, NonceLength), cipher,
                buffer.AsSpan(1 + NonceLength, TagLength), plain, Encoding.UTF8.GetBytes(associatedData));
            return Encoding.UTF8.GetString(plain);
        }

        // ----- row helpers ------------------------------------------------------------------

        private static string? GetValue(Dictionary<string, object> row, string name)
        {
            foreach (var kvp in row)
            {
                if (string.Equals(kvp.Key, name, StringComparison.OrdinalIgnoreCase))
                    return kvp.Value?.ToString();
            }
            return null;
        }

        private static long GetInt64(Dictionary<string, object> row, string name)
        {
            foreach (var kvp in row)
            {
                if (string.Equals(kvp.Key, name, StringComparison.OrdinalIgnoreCase))
                {
                    if (kvp.Value == null) return 0;
                    if (kvp.Value is long l) return l;
                    if (kvp.Value is int i) return i;
                    if (kvp.Value is double d) return (long)d;
                    if (kvp.Value is decimal m) return (long)m;
                    return Convert.ToInt64(kvp.Value);
                }
            }
            return 0;
        }

        private readonly struct RowMetadata(long length, long createdTicks, long modifiedTicks)
        {
            public long Length { get; } = length;
            public long CreatedTicks { get; } = createdTicks;
            public long ModifiedTicks { get; } = modifiedTicks;

            public static RowMetadata FromRow(Dictionary<string, object> row) =>
                new(GetInt64(row, "length"), GetInt64(row, "created_ticks"), GetInt64(row, "modified_ticks"));
        }

        /// <summary>
        /// On-disk description of how payloads in this store are protected and which table
        /// generation is live. Versioned so a later release can recognise and migrate older
        /// layouts instead of failing authentication.
        /// </summary>
        private sealed class StoreManifest
        {
            public int FormatVersion { get; set; }
            public string Kdf { get; set; } = string.Empty;
            public int Iterations { get; set; }
            public int EnvelopeVersion { get; set; }
            public string Salt { get; set; } = string.Empty;
            /// <summary>Active table name; compaction advances it through generations.</summary>
            public string? Table { get; set; }

            public byte[] DecodeSalt()
            {
                try { return Convert.FromBase64String(Salt); }
                catch (FormatException) { return []; }
            }
        }
    }
}
