#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
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
    /// Uses SharpCoreDB <b>directory mode</b> with encryption enabled (AES-256-GCM,
    /// key derived from the master password). A single <c>queue_items</c> table holds
    /// all storage areas. The message key is the primary key namespaced
    /// <c>"<area>:<key>"</c> so the same logical key can exist in multiple
    /// areas (incoming, outgoing, dead-letter, ...).
    /// </para>
    /// <para>
    /// <b>Why directory mode and the direct ITable API?</b> Per the SharpCoreDB benchmark
    /// documentation, the direct <c>ITable</c> primary-key lookups (B-tree) are the fastest
    /// path and avoid the SQL parser overhead; single-file (.scdb) tables however use a
    /// no-op B-tree index, so directory mode is required for correct point operations.
    /// </para>
    /// </summary>
    public sealed class SharpCoreDbMessageStore : IMessageStore
    {
        private const string TableName = "queue_items";

        /// <summary>
        /// Default master password used when the caller does not supply one.
        /// The AES-256-GCM-encryption key is derived from this password, so it must be
        /// supplied again (or the same default kept) to reopen the store.
        /// </summary>
        public const string DefaultMasterPassword = "ServiceMq-SharpCoreDb-Default-2026!";

        private readonly Lock syncRoot = new();
        private readonly IServiceProvider serviceProvider;
        private readonly IDatabase database;
        private readonly ITable table;
        private readonly byte[] payloadKey;
        private readonly string masterPassword;
        private Exception? lastException;

        public Exception LastException { get { return lastException!; } }
        public string DatabasePath { get; private set; } = string.Empty;
        public string MasterPassword => masterPassword;

        public SharpCoreDbMessageStore(string databasePath)
            : this(databasePath, DefaultMasterPassword, null)
        {
        }

        public SharpCoreDbMessageStore(string databasePath, string masterPassword)
            : this(databasePath, masterPassword, null)
        {
        }

        public SharpCoreDbMessageStore(string databasePath, string masterPassword, DatabaseConfig? config)
        {
            if (string.IsNullOrWhiteSpace(databasePath)) throw new ArgumentException("A database path is required.", "databasePath");
            if (string.IsNullOrWhiteSpace(masterPassword)) throw new ArgumentException("A master password is required for SharpCoreDB encryption.", "masterPassword");

            DatabasePath = Path.GetFullPath(databasePath);
            this.masterPassword = masterPassword;
            // Payload encryption key: AES-256-GCM key derived from the master password.
            // Storage.AppendBytes in SharpCoreDB 1.9.3 writes table data as plaintext to
            // the .dat file (EnableBatchEncryption is a no-op there), so the provider
            // encrypts payload values itself to guarantee encryption at rest.
            payloadKey = SHA256.HashData(Encoding.UTF8.GetBytes(masterPassword));
            Directory.CreateDirectory(DatabasePath);

            try
            {
                serviceProvider = new ServiceCollection()
                    .AddSharpCoreDB()
                    .BuildServiceProvider();

                var factory = new DatabaseFactory(serviceProvider);
                // Directory mode: encryption stays ON (NoEncryptMode=false by default in
                // DatabaseConfig.Default) with AES-256-GCM derived from the master password.
                // EnableBatchEncryption MUST be true: SharpCoreDB's append storage only encrypts
                // table data files when this flag is set (Storage.Append.cs), otherwise the
                // payload column is written as plaintext to queue_items.dat.
                // DatabaseConfig uses init-only properties, so a clone is required to
                // enable batch encryption without mutating the caller's config instance.
                database = factory.Create(DatabasePath, masterPassword, false, WithBatchEncryption(config));

                EnsureSchema();

                if (!database.TryGetTable(TableName, out var t))
                    throw new InvalidOperationException("Failed to initialize the queue_items table in SharpCoreDB.");
                table = t;
                RepairTableSchemaLists();

                Flush();
            }
            catch (Exception ex)
            {
                lastException = ex;
                throw;
            }
        }

        public IReadOnlyList<string> GetKeys(StorageArea area)
        {
            lock (syncRoot)
            {
                try
                {
                    List<string> result = [];
                    foreach (var row in SelectArea(area))
                    {
                        var composite = GetValue(row, "key");
                        if (composite != null) result.Add(StripAreaPrefix(composite));
                    }
                    return result;
                }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public bool Contains(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                try { return table.FindByPrimaryKey(CompositeKey(area, key)) != null; }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public StorageEntry Read(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                try
                {
                    var row = table.FindByPrimaryKey(CompositeKey(area, key));
                    if (row == null) throw new KeyNotFoundException(key);
                    return new StorageEntry
                    {
                        Key = key,
                        Value = Unprotect(GetValue(row, "value") ?? string.Empty),
                        Length = GetInt64(row, "length"),
                        CreatedUtc = new DateTime(GetInt64(row, "created_ticks"), DateTimeKind.Utc),
                        LastModifiedUtc = new DateTime(GetInt64(row, "modified_ticks"), DateTimeKind.Utc)
                    };
                }
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

                    if (table.FindByPrimaryKey(rowId) == null)
                    {
                        table.Insert(new()
                        {
                            ["key"] = rowId,
                            ["area"] = (int)area,
                            ["value"] = Protect(normalized),
                            ["length"] = length,
                            ["created_ticks"] = now,
                            ["modified_ticks"] = now
                        });
                    }
                    else
                    {
                        table.UpdateByPrimaryKey(rowId, new()
                        {
                            ["area"] = (int)area,
                            ["value"] = Protect(normalized),
                            ["length"] = length,
                            ["modified_ticks"] = now
                        });
                    }
                    if (durability == DurabilityMode.FlushToDisk) Flush();
                }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void Append(StorageArea area, string key, string value, DurabilityMode durability)
        {
            lock (syncRoot)
            {
                try
                {
                    var now = DateTime.UtcNow.Ticks;
                    var normalized = value ?? string.Empty;
                    var rowId = CompositeKey(area, key);
                    var existing = table.FindByPrimaryKey(rowId);
                    if (existing == null)
                    {
                        Write(area, key, normalized, durability);
                        return;
                    }

                    var previous = Unprotect(GetValue(existing, "value") ?? string.Empty);
                    var combined = previous + Environment.NewLine + normalized;
                    table.UpdateByPrimaryKey(rowId, new()
                    {
                        ["area"] = (int)area,
                        ["value"] = Protect(combined),
                        ["length"] = (long)Encoding.UTF8.GetByteCount(combined),
                        ["modified_ticks"] = now
                    });
                    if (durability == DurabilityMode.FlushToDisk) Flush();
                }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void Delete(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                try
                {
                    table.DeleteByPrimaryKey(CompositeKey(area, key));
                    Flush();
                }
                catch (Exception ex) { lastException = ex; throw; }
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
                    if (row == null) return;

                    // Mirror the SQLite provider semantics: delete any pre-existing
                    // destination entry, then move by removing the source row and
                    // inserting the row under the destination area key.
                    // Preserve the raw CLR values from the found row so that Table.Insert
                    // can run its own type coercion instead of re-parsing through strings.
                    table.DeleteByPrimaryKey(destinationRowId);
                    table.DeleteByPrimaryKey(sourceRowId);
                    Dictionary<string, object> moved = new(StringComparer.OrdinalIgnoreCase)
                    {
                        ["key"] = destinationRowId,
                        ["area"] = (int)destination,
                        ["modified_ticks"] = DateTime.UtcNow.Ticks
                    };
                    moved["value"] = Protect(Unprotect(GetValue(row, "value") ?? string.Empty));
                    CopyRowValue(row, "length", moved);
                    CopyRowValue(row, "created_ticks", moved);
                    table.Insert(moved);
                    Flush();
                }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void Purge(StorageArea area, DateTime olderThanUtc)
        {
            lock (syncRoot)
            {
                try
                {
                    foreach (var row in SelectArea(area))
                    {
                        if (GetInt64(row, "modified_ticks") < olderThanUtc.Ticks)
                        {
                            var composite = GetValue(row, "key");
                            if (composite != null) table.DeleteByPrimaryKey(composite);
                        }
                    }
                    Flush();
                }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public StorageAreaStatistics GetStatistics(StorageArea area)
        {
            lock (syncRoot)
            {
                try
                {
                    var rows = SelectArea(area);
                    if (rows.Count == 0) return new StorageAreaStatistics { Count = 0, Bytes = 0, OldestUtc = null };

                    long bytes = 0;
                    long oldest = long.MaxValue;
                    foreach (var row in rows)
                    {
                        bytes += GetInt64(row, "length");
                        var created = GetInt64(row, "created_ticks");
                        if (created < oldest) oldest = created;
                    }

                    return new StorageAreaStatistics
                    {
                        Count = rows.Count,
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
                try { database.Flush(); }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void ClearException() { lastException = null; }

        public void Dispose()
        {
            lock (syncRoot)
            {
                try { Flush(); } catch { }
                database.DisposeAsync().AsTask().GetAwaiter().GetResult();
                (serviceProvider as IDisposable)?.Dispose();
            }
        }

        /// <summary>
        /// Clones a <see cref="DatabaseConfig"/> (or creates a default) with
        /// <see cref="DatabaseConfig.EnableBatchEncryption"/> forced to true, which is
        /// required for SharpCoreDB to encrypt table data files at rest.
        /// </summary>
        private static DatabaseConfig WithBatchEncryption(DatabaseConfig? source)
        {
            var src = source ?? new DatabaseConfig();
            return new DatabaseConfig
            {
                NoEncryptMode = src.NoEncryptMode,
                EnableBatchEncryption = true,
                BatchEncryptionSizeKB = src.BatchEncryptionSizeKB,
                HighSpeedInsertMode = src.HighSpeedInsertMode,
                UseOptimizedInsertPath = src.UseOptimizedInsertPath,
                ToggleEncryptionDuringBulk = src.ToggleEncryptionDuringBulk,
                EncryptionBufferSizeKB = src.EncryptionBufferSizeKB,
                EnableQueryCache = src.EnableQueryCache,
                QueryCacheSize = src.QueryCacheSize,
                WalBufferSize = src.WalBufferSize,
                BufferPoolSize = src.BufferPoolSize,
                EnableHashIndexes = src.EnableHashIndexes,
                UseBufferedIO = src.UseBufferedIO,
                UseMemoryMapping = src.UseMemoryMapping,
                CollectGCAfterBatches = src.CollectGCAfterBatches,
                EnablePageCache = src.EnablePageCache,
                PageCacheCapacity = src.PageCacheCapacity,
                PageSize = src.PageSize,
                WalDurabilityMode = src.WalDurabilityMode,
                WalMaxBatchSize = src.WalMaxBatchSize,
                GroupCommitSize = src.GroupCommitSize,
                WalMaxBatchDelayMs = src.WalMaxBatchDelayMs,
                UseGroupCommitWal = src.UseGroupCommitWal,
                EnableAdaptiveWalBatching = src.EnableAdaptiveWalBatching,
                WalBatchMultiplier = src.WalBatchMultiplier,
                SqlValidationMode = src.SqlValidationMode,
                StrictParameterValidation = src.StrictParameterValidation,
                EnableCompiledPlanCache = src.EnableCompiledPlanCache,
                CompiledPlanCacheCapacity = src.CompiledPlanCacheCapacity,
                NormalizeSqlForPlanCache = src.NormalizeSqlForPlanCache,
                EnableSimdAndProjectionPushdown = src.EnableSimdAndProjectionPushdown,
                EnableBTreeSelection = src.EnableBTreeSelection,
                EnableUnsafeEqualityIndex = src.EnableUnsafeEqualityIndex,
                EnableDeltaUpdates = src.EnableDeltaUpdates,
                StorageEngineType = src.StorageEngineType,
                WorkloadHint = src.WorkloadHint,
                ColumnarAutoCompactionThreshold = src.ColumnarAutoCompactionThreshold,
                WalBufferSizePages = src.WalBufferSizePages
            };
        }

        private void EnsureSchema()
        {
            // NOTE: BIGINT (DataType.Long/Int64) is required for length/created_ticks/
            // modified_ticks because DateTime.Ticks (~6.4e17) overflows DataType.Integer
            // (Int32). area stays INTEGER (Int32).
            database.ExecuteSQL("CREATE TABLE IF NOT EXISTS " + TableName + " (" +
                "key TEXT PRIMARY KEY, " +
                "area INTEGER NOT NULL, " +
                "value TEXT NOT NULL, " +
                "length BIGINT NOT NULL, " +
                "created_ticks BIGINT NOT NULL, " +
                "modified_ticks BIGINT NOT NULL)");
        }

        /// <summary>
        /// SharpCoreDB's <c>Database.Load()</c> fills IsAuto/IsNotNull/DefaultValues/
        /// ColumnCollations after deserialization but NOT DefaultExpressions or
        /// ColumnCheckExpressions. A reopened table then throws
        /// ArgumentOutOfRangeException from Table.Insert/Update. This normalizes all
        /// per-column lists to the column count (mirroring SharpCoreDB's own pattern).
        /// </summary>
        private void RepairTableSchemaLists()
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

        /// <summary>
        /// Returns all rows belonging to the given storage area.
        /// The area is part of the primary key prefix, so this filters in memory over
        /// <c>ITable.Select()</c>. These scans are rare (GetKeys/Purge/GetStatistics).
        /// </summary>
        private List<Dictionary<string, object>> SelectArea(StorageArea area)
        {
            database.Flush();
            List<Dictionary<string, object>> result = [];
            foreach (var row in table.Select())
            {
                if (GetInt64(row, "area") == (int)area) result.Add(row);
            }
            return result;
        }

        private static string CompositeKey(StorageArea area, string key) => (int)area + ":" + key;

        private static string StripAreaPrefix(string compositeKey)
        {
            var idx = compositeKey.IndexOf(':');
            return idx >= 0 ? compositeKey.Substring(idx + 1) : compositeKey;
        }

        /// <summary>
        /// Encrypts a payload value with AES-256-GCM using the key derived from the
        /// master password. Format: nonce(12) | tag(16) | ciphertext. Base64-encoded.
        /// Because SharpCoreDB 1.9.3 stores table data unencrypted in the .dat file,
        /// this provider-level encryption guarantees queue payloads are protected at rest.
        /// </summary>
        private string Protect(string plaintext)
        {
            var plain = Encoding.UTF8.GetBytes(plaintext);
            var nonce = new byte[12];
            RandomNumberGenerator.Fill(nonce);
            var cipher = new byte[plain.Length];
            var tag = new byte[16];
            using var aes = new AesGcm(payloadKey, 16);
            aes.Encrypt(nonce, plain, cipher, tag);
            var buffer = new byte[nonce.Length + tag.Length + cipher.Length];
            Buffer.BlockCopy(nonce, 0, buffer, 0, nonce.Length);
            Buffer.BlockCopy(tag, 0, buffer, nonce.Length, tag.Length);
            Buffer.BlockCopy(cipher, 0, buffer, nonce.Length + tag.Length, cipher.Length);
            return Convert.ToBase64String(buffer);
        }

        /// <summary>
        /// Decrypts a value produced by <see cref="Protect"/>. Throws on tampering
        /// (AES-GCM integrity check).
        /// </summary>
        private string Unprotect(string protectedValue)
        {
            var buffer = Convert.FromBase64String(protectedValue);
            if (buffer.Length < 12 + 16) return protectedValue; // not protected (legacy/plaintext)
            var nonce = buffer.AsSpan(0, 12);
            var tag = buffer.AsSpan(12, 16);
            var cipher = buffer.AsSpan(12 + 16);
            var plain = new byte[cipher.Length];
            using var aes = new AesGcm(payloadKey, 16);
            aes.Decrypt(nonce, cipher, tag, plain);
            return Encoding.UTF8.GetString(plain);
        }

        private static void CopyRowValue(Dictionary<string, object> source, string name, Dictionary<string, object> target)
        {
            foreach (var kvp in source)
            {
                if (string.Equals(kvp.Key, name, StringComparison.OrdinalIgnoreCase))
                {
                    target[kvp.Key] = kvp.Value ?? string.Empty;
                    return;
                }
            }
        }

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
    }
}