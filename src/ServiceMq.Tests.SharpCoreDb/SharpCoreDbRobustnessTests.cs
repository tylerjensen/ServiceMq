#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using SharpCoreDB;
using Xunit;

namespace ServiceMq.Tests.SharpCoreDb
{
    /// <summary>
    /// Failure-path behaviour: the store manifest, the exclusive ownership lock, index
    /// consistency when a mutation fails part-way, constructor cleanup, and envelope
    /// tampering. Faults are injected through the internal <see cref="IQueueTable"/> seam.
    /// </summary>
    public class SharpCoreDbRobustnessTests
    {
        private const string Password = SharpCoreDbStorageTests.Password;
        private const string ManifestFile = "servicemq-store.json";
        private const string LockFile = "servicemq.lock";

        // StorageArea ordinals as they appear in composite row keys ("<area>:<key>").
        private const string OutgoingPrefix = "1:";
        private const string DeadLetterPrefix = "4:";

        private static SharpCoreDbMessageStore Open(string root) => new(root, Password);

        private static SharpCoreDbMessageStore OpenFaulting(string root, out FaultingTable faults)
        {
            FaultingTable? captured = null;
            var store = new SharpCoreDbMessageStore(root, Password, null, inner => captured = new FaultingTable(inner));
            faults = captured!;
            return store;
        }

        // ----- Manifest -------------------------------------------------------------------

        [Fact]
        public void Manifest_IsCreatedOnFirstOpen_AndReusedOnReopen()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                    store.Write(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);

                var manifestPath = Path.Combine(root, ManifestFile);
                Assert.True(File.Exists(manifestPath));
                var json = JsonNode.Parse(File.ReadAllText(manifestPath))!;
                Assert.Equal(1, (int)json["formatVersion"]!);
                Assert.Equal("PBKDF2-HMAC-SHA256", (string)json["kdf"]!);
                Assert.Equal(16, Convert.FromBase64String((string)json["salt"]!).Length);
                var firstSalt = (string)json["salt"]!;

                using (var store = Open(root))
                    Assert.Equal("payload", store.Read(StorageArea.Outgoing, "a.omq").Value);

                // Reopen must not regenerate the salt.
                Assert.Equal(firstSalt, (string)JsonNode.Parse(File.ReadAllText(manifestPath))!["salt"]!);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Manifest_TamperedSalt_FailsPayloadAuthentication()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                    store.Write(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);

                EditManifest(root, json => json["salt"] = Convert.ToBase64String(RandomNumberGenerator.GetBytes(16)));

                using var reopened = Open(root);
                Assert.ThrowsAny<CryptographicException>(() => reopened.Read(StorageArea.Outgoing, "a.omq"));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Manifest_MissingButRowsPresent_IsRejected()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                    store.Write(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);

                File.Delete(Path.Combine(root, ManifestFile));

                var ex = Assert.Throws<InvalidDataException>(() => Open(root));
                Assert.Contains(ManifestFile, ex.Message);
                Assert.Contains("no migration", ex.Message, StringComparison.OrdinalIgnoreCase);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Manifest_UnknownFormatVersion_IsRejected()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                    store.Write(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);

                EditManifest(root, json => json["formatVersion"] = 99);

                var ex = Assert.Throws<NotSupportedException>(() => Open(root));
                Assert.Contains("99", ex.Message);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Manifest_StaleSaltFileWithoutRows_IsIgnored()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                Directory.CreateDirectory(root);
                // Layout of the unreleased pre-manifest commit: a bare salt file and no rows.
                File.WriteAllBytes(Path.Combine(root, "servicemq-payload.salt"), RandomNumberGenerator.GetBytes(16));

                using (var store = Open(root))
                    store.Write(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);

                Assert.True(File.Exists(Path.Combine(root, ManifestFile)));
                using (var store = Open(root))
                    Assert.Equal("payload", store.Read(StorageArea.Outgoing, "a.omq").Value);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        // ----- Exclusive ownership --------------------------------------------------------

        [Fact]
        public void SecondInstanceOnSameDirectory_Throws_FirstKeepsWorking()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var first = Open(root))
                {
                    first.Write(StorageArea.Outgoing, "a.omq", "one", DurabilityMode.FlushToDisk);

                    var ex = Assert.Throws<IOException>(() => Open(root));
                    Assert.Contains(root, ex.Message);
                    Assert.Contains("exclusive", ex.Message, StringComparison.OrdinalIgnoreCase);

                    first.Write(StorageArea.Outgoing, "b.omq", "two", DurabilityMode.FlushToDisk);
                    Assert.Equal("two", first.Read(StorageArea.Outgoing, "b.omq").Value);
                }

                // Dispose releases the lock.
                using var third = Open(root);
                Assert.Equal(new[] { "a.omq", "b.omq" }, third.GetKeys(StorageArea.Outgoing).ToArray());
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void DoubleDispose_DoesNotThrow()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                var store = Open(root);
                store.Dispose();
                store.Dispose();
                using var reopened = Open(root);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        // ----- Move fault injection -------------------------------------------------------

        [Fact]
        public void Move_FailsOnDestinationInsert_IndexMatchesTable()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = OpenFaulting(root, out var faults);
                store.Write(StorageArea.Outgoing, "m.omq", "payload", DurabilityMode.FlushToDisk);

                faults.Before = (op, key) => { if (op == "Insert" && Key(key).StartsWith(DeadLetterPrefix)) throw new IOException("injected"); };
                Assert.Throws<IOException>(() => store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq"));
                faults.Before = null;

                Assert.True(store.Contains(StorageArea.Outgoing, "m.omq"));
                Assert.False(store.Contains(StorageArea.DeadLetter, "m.omq"));
                Assert.Equal("payload", store.Read(StorageArea.Outgoing, "m.omq").Value);
                Assert.Equal(1, store.GetStatistics(StorageArea.Outgoing).Count);
                Assert.Equal(0, store.GetStatistics(StorageArea.DeadLetter).Count);
                Assert.IsType<IOException>(store.LastException);

                store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq");
                Assert.False(store.Contains(StorageArea.Outgoing, "m.omq"));
                Assert.Equal("payload", store.Read(StorageArea.DeadLetter, "m.omq").Value);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Move_FailsOnSourceTombstone_DuplicateIsVisibleAndRecoverable()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = OpenFaulting(root, out var faults);
                store.Write(StorageArea.Outgoing, "m.omq", "payload", DurabilityMode.FlushToDisk);

                faults.Before = (op, key) => { if (op == "Update" && Key(key).StartsWith(OutgoingPrefix)) throw new IOException("injected"); };
                Assert.Throws<IOException>(() => store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq"));
                faults.Before = null;

                // Destination was written and flushed; source tombstone failed: both are present.
                Assert.True(store.Contains(StorageArea.Outgoing, "m.omq"));
                Assert.True(store.Contains(StorageArea.DeadLetter, "m.omq"));
                Assert.Equal("payload", store.Read(StorageArea.Outgoing, "m.omq").Value);
                Assert.Equal("payload", store.Read(StorageArea.DeadLetter, "m.omq").Value);

                // Retrying completes the move.
                store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq");
                Assert.False(store.Contains(StorageArea.Outgoing, "m.omq"));
                Assert.True(store.Contains(StorageArea.DeadLetter, "m.omq"));
                Assert.Equal(1, store.GetStatistics(StorageArea.DeadLetter).Count);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Move_FailsOnFlushAfterDestinationInsert_BothRowsVisible()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = OpenFaulting(root, out var faults);
                store.Write(StorageArea.Outgoing, "m.omq", "payload", DurabilityMode.FlushToDisk);

                var flushes = 0;
                faults.Before = (op, _) => { if (op == "Flush" && ++flushes == 1) throw new IOException("injected"); };
                Assert.Throws<IOException>(() => store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq"));
                faults.Before = null;

                Assert.True(store.Contains(StorageArea.Outgoing, "m.omq"));
                Assert.True(store.Contains(StorageArea.DeadLetter, "m.omq"));
                Assert.Equal("payload", store.Read(StorageArea.DeadLetter, "m.omq").Value);

                store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq");
                Assert.False(store.Contains(StorageArea.Outgoing, "m.omq"));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Move_FailsOnFinalFlush_MoveIsComplete()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = OpenFaulting(root, out var faults);
                store.Write(StorageArea.Outgoing, "m.omq", "payload", DurabilityMode.FlushToDisk);

                var flushes = 0;
                faults.Before = (op, _) => { if (op == "Flush" && ++flushes == 2) throw new IOException("injected"); };
                Assert.Throws<IOException>(() => store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq"));
                faults.Before = null;

                Assert.False(store.Contains(StorageArea.Outgoing, "m.omq"));
                Assert.True(store.Contains(StorageArea.DeadLetter, "m.omq"));
                Assert.Equal("payload", store.Read(StorageArea.DeadLetter, "m.omq").Value);
                Assert.Equal(0, store.GetStatistics(StorageArea.Outgoing).Count);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        // ----- Other mutation faults ------------------------------------------------------

        [Fact]
        public void Write_FailsOnInsert_KeyIsNotIndexed()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = OpenFaulting(root, out var faults);
                faults.Before = (op, _) => { if (op == "Insert") throw new IOException("injected"); };
                Assert.Throws<IOException>(() => store.Write(StorageArea.Outgoing, "w.omq", "payload", DurabilityMode.FlushToDisk));
                faults.Before = null;

                Assert.False(store.Contains(StorageArea.Outgoing, "w.omq"));
                Assert.Empty(store.GetKeys(StorageArea.Outgoing));
                Assert.Equal(0, store.GetStatistics(StorageArea.Outgoing).Count);
                Assert.Throws<KeyNotFoundException>(() => store.Read(StorageArea.Outgoing, "w.omq"));

                store.Write(StorageArea.Outgoing, "w.omq", "payload", DurabilityMode.FlushToDisk);
                Assert.True(store.Contains(StorageArea.Outgoing, "w.omq"));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Delete_FailsOnTombstone_KeyStaysIndexed()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = OpenFaulting(root, out var faults);
                store.Write(StorageArea.Outgoing, "d.omq", "payload", DurabilityMode.FlushToDisk);

                faults.Before = (op, _) => { if (op == "Update") throw new IOException("injected"); };
                Assert.Throws<IOException>(() => store.Delete(StorageArea.Outgoing, "d.omq"));
                faults.Before = null;

                Assert.True(store.Contains(StorageArea.Outgoing, "d.omq"));
                Assert.Equal("payload", store.Read(StorageArea.Outgoing, "d.omq").Value);

                store.Delete(StorageArea.Outgoing, "d.omq");
                Assert.False(store.Contains(StorageArea.Outgoing, "d.omq"));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Purge_FailsMidway_IndexIsRebuiltFromTable()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                IReadOnlyList<string> keysAfterFailure;
                using (var store = OpenFaulting(root, out var faults))
                {
                    for (var i = 0; i < 5; i++)
                        store.Write(StorageArea.Sent, $"s{i}.log", "audit", DurabilityMode.FlushToDisk);

                    var updates = 0;
                    faults.Before = (op, _) => { if (op == "Update" && ++updates == 3) throw new IOException("injected"); };
                    Assert.Throws<IOException>(() => store.Purge(StorageArea.Sent, DateTime.UtcNow.AddDays(1)));
                    faults.Before = null;

                    keysAfterFailure = store.GetKeys(StorageArea.Sent);
                    Assert.Equal(3, keysAfterFailure.Count);
                    Assert.Equal(3, store.GetStatistics(StorageArea.Sent).Count);
                }

                // A fresh open rebuilds the index from the table; it must agree with the
                // resynced index of the failed instance.
                using var reopened = Open(root);
                var reopenedKeys = reopened.GetKeys(StorageArea.Sent).ToArray();
                Assert.True(reopenedKeys.SequenceEqual(keysAfterFailure),
                    "after failure: [" + string.Join(",", keysAfterFailure) + "]  reopened: [" + string.Join(",", reopenedKeys) + "]");
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        // ----- Constructor cleanup --------------------------------------------------------

        [Fact]
        public void WrongPasswordRepeatedly_ThenCorrectPassword_Opens()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                    store.Write(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);

                // Whether SharpCoreDB rejects the password at open or the payload fails
                // authentication at read, no lock or handle may survive the attempt.
                for (var attempt = 0; attempt < 3; attempt++)
                {
                    Assert.ThrowsAny<Exception>(() =>
                    {
                        using var wrong = new SharpCoreDbMessageStore(root, "not-the-password-" + attempt);
                        return wrong.Read(StorageArea.Outgoing, "a.omq").Value;
                    });
                }

                using var correct = Open(root);
                Assert.Equal("payload", correct.Read(StorageArea.Outgoing, "a.omq").Value);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void ConstructorFault_ReleasesLockAndResources()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                var ex = Assert.Throws<IOException>(() => new SharpCoreDbMessageStore(root, Password, null, inner =>
                {
                    var faulting = new FaultingTable(inner);
                    faulting.Before = (op, _) => { if (op == "Select") throw new IOException("injected during index build"); };
                    return faulting;
                }));
                Assert.Contains("index build", ex.Message);

                // The lock file is released, so the directory opens normally.
                using var store = Open(root);
                store.Write(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);
                Assert.Equal("payload", store.Read(StorageArea.Outgoing, "a.omq").Value);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        // ----- Envelope tampering ---------------------------------------------------------

        [Fact]
        public void TamperedEnvelopes_AreRejected_AndRecordedAsStorageFaults()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = OpenFaulting(root, out var faults);
                store.Write(StorageArea.Outgoing, "a.omq", "alpha", DurabilityMode.FlushToDisk);
                store.Write(StorageArea.Outgoing, "b.omq", "bravo", DurabilityMode.FlushToDisk);

                var sealedB = GetStoredValue(faults.Inner, OutgoingPrefix + "b.omq");

                string? replacement = null;
                faults.OnFind = (key, row) =>
                    row != null && replacement != null && Key(key) == OutgoingPrefix + "a.omq"
                        ? WithValue(row, replacement)
                        : row;

                void ExpectRejected(string tampered)
                {
                    replacement = tampered;
                    store.ClearException();
                    Assert.ThrowsAny<CryptographicException>(() => store.Read(StorageArea.Outgoing, "a.omq"));
                    Assert.IsAssignableFrom<CryptographicException>(store.LastException);
                }

                var sealedA = GetStoredValue(faults.Inner, OutgoingPrefix + "a.omq");
                ExpectRejected(sealedA.Substring(0, sealedA.Length / 2));   // truncated
                ExpectRejected(sealedB);                                     // valid envelope, wrong row (AAD)
                ExpectRejected("hello");                                     // plaintext
                ExpectRejected(string.Empty);                                // empty
                ExpectRejected(Convert.ToBase64String(new byte[] { 2, 0, 0 })); // unknown envelope version

                replacement = null;
                store.ClearException();
                Assert.Equal("alpha", store.Read(StorageArea.Outgoing, "a.omq").Value);
                Assert.Null(store.LastException);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        // ----- tombstones and compaction ------------------------------------------------------

        [Fact]
        public void MoveAndPurge_SurviveReopen()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                {
                    store.Write(StorageArea.Outgoing, "m.omq", "payload", DurabilityMode.FlushToDisk);
                    store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq");
                    store.Write(StorageArea.Sent, "old.log", "audit", DurabilityMode.FlushToDisk);
                    store.Purge(StorageArea.Sent, DateTime.UtcNow.AddDays(1));
                }
                using (var store = Open(root))
                {
                    Assert.False(store.Contains(StorageArea.Outgoing, "m.omq"));
                    Assert.Throws<KeyNotFoundException>(() => store.Read(StorageArea.Outgoing, "m.omq"));
                    Assert.Equal("payload", store.Read(StorageArea.DeadLetter, "m.omq").Value);
                    Assert.Empty(store.GetKeys(StorageArea.Sent));
                    Assert.Equal(0, store.GetStatistics(StorageArea.Sent).Count);
                }
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void DeleteThenRewriteSameKey_SurvivesReopen()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                {
                    store.Write(StorageArea.Outgoing, "a.omq", "first", DurabilityMode.FlushToDisk);
                    store.Delete(StorageArea.Outgoing, "a.omq");
                    Assert.False(store.Contains(StorageArea.Outgoing, "a.omq"));
                    // The primary key is still held by the tombstone; the write must reuse it.
                    store.Write(StorageArea.Outgoing, "a.omq", "second", DurabilityMode.FlushToDisk);
                    Assert.Equal("second", store.Read(StorageArea.Outgoing, "a.omq").Value);
                    Assert.Equal(1, store.GetStatistics(StorageArea.Outgoing).Count);
                }
                using (var store = Open(root))
                {
                    Assert.Equal("second", store.Read(StorageArea.Outgoing, "a.omq").Value);
                    Assert.Equal(new[] { "a.omq" }, store.GetKeys(StorageArea.Outgoing).ToArray());
                }
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Compaction_RewritesLiveRowsIntoNewGeneration()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                {
                    store.CompactionMinimumTombstones = 8;
                    for (var i = 0; i < 20; i++)
                        store.Write(StorageArea.Outgoing, $"k{i:D2}.omq", $"v{i}", DurabilityMode.Buffered);
                    store.Flush();

                    // 12 tombstones >= 8 and >= 8 live rows: compaction runs on the 12th delete.
                    for (var i = 0; i < 12; i++)
                        store.Delete(StorageArea.Outgoing, $"k{i:D2}.omq");

                    Assert.Equal("queue_items_g1", ActiveTable(root));
                    Assert.Equal(8, store.GetStatistics(StorageArea.Outgoing).Count);
                    Assert.Equal("v12", store.Read(StorageArea.Outgoing, "k12.omq").Value);

                    // Live rows keep working in the new generation, including deletes and
                    // re-writes of compacted-away keys (no stale tombstone blocks the insert).
                    store.Write(StorageArea.Outgoing, "k00.omq", "again", DurabilityMode.FlushToDisk);
                    store.Delete(StorageArea.Outgoing, "k19.omq");
                    Assert.Equal(8, store.GetStatistics(StorageArea.Outgoing).Count);
                }
                using (var store = Open(root))
                {
                    Assert.Equal(8, store.GetStatistics(StorageArea.Outgoing).Count);
                    Assert.Equal("again", store.Read(StorageArea.Outgoing, "k00.omq").Value);
                    Assert.Equal("v18", store.Read(StorageArea.Outgoing, "k18.omq").Value);
                    Assert.False(store.Contains(StorageArea.Outgoing, "k19.omq"));
                    Assert.Equal(new[] { "queue_items_g1" }, TableNames(root));
                }
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Compaction_CanRunRepeatedly()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                store.CompactionMinimumTombstones = 2;
                for (var round = 1; round <= 3; round++)
                {
                    store.Write(StorageArea.Outgoing, "a.omq", "a" + round, DurabilityMode.Buffered);
                    store.Write(StorageArea.Outgoing, "b.omq", "b" + round, DurabilityMode.Buffered);
                    store.Write(StorageArea.Outgoing, "keep.omq", "keep" + round, DurabilityMode.Buffered);
                    store.Delete(StorageArea.Outgoing, "a.omq");
                    store.Delete(StorageArea.Outgoing, "b.omq");   // 2 tombstones >= 2 and >= 1 live
                    Assert.Equal("queue_items_g" + round, ActiveTable(root));
                    Assert.Equal("keep" + round, store.Read(StorageArea.Outgoing, "keep.omq").Value);
                    Assert.Equal(1, store.GetStatistics(StorageArea.Outgoing).Count);
                }
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void LeftoverGenerationTables_AreDroppedOnOpen()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                    store.Write(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);

                // Simulate a compaction interrupted before its manifest commit: an orphan
                // generation exists next to the active table.
                CreateRawTable(root, "queue_items_g7");
                Assert.Contains("queue_items_g7", TableNames(root));
                // And a data file whose table is no longer catalogued (a DROP that failed
                // part-way leaves exactly this behind).
                var orphan = Path.Combine(root, "queue_items_g3.dat");
                File.WriteAllBytes(orphan, new byte[] { 1, 2, 3 });

                using (var store = Open(root))
                    Assert.Equal("payload", store.Read(StorageArea.Outgoing, "a.omq").Value);

                Assert.Equal(new[] { "queue_items" }, TableNames(root));
                Assert.False(File.Exists(orphan));
                Assert.False(File.Exists(Path.Combine(root, "queue_items_g7.dat")));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        // ----- helpers ---------------------------------------------------------------------

        private static string ActiveTable(string root) =>
            (string)JsonNode.Parse(File.ReadAllText(Path.Combine(root, ManifestFile)))!["table"]!;

        private static string[] TableNames(string root)
        {
            var (db, provider) = OpenRaw(root);
            try
            {
                return db.GetTables().Select(t => t.Name)
                    .Where(n => n.StartsWith("queue_items", StringComparison.Ordinal))
                    .OrderBy(n => n, StringComparer.Ordinal).ToArray();
            }
            finally
            {
                db.DisposeAsync().AsTask().GetAwaiter().GetResult();
                provider.Dispose();
            }
        }

        private static void CreateRawTable(string root, string name)
        {
            var (db, provider) = OpenRaw(root);
            try
            {
                db.ExecuteSQL("CREATE TABLE IF NOT EXISTS " + name + " (key TEXT PRIMARY KEY, area INTEGER NOT NULL, value TEXT NOT NULL, length BIGINT NOT NULL, created_ticks BIGINT NOT NULL, modified_ticks BIGINT NOT NULL)");
                db.Flush();
            }
            finally
            {
                db.DisposeAsync().AsTask().GetAwaiter().GetResult();
                provider.Dispose();
            }
        }

        private static (SharpCoreDB.Interfaces.IDatabase db, Microsoft.Extensions.DependencyInjection.ServiceProvider provider) OpenRaw(string root)
        {
            var provider = new Microsoft.Extensions.DependencyInjection.ServiceCollection()
                .AddSharpCoreDB()
                .BuildServiceProvider();
            var db = new SharpCoreDB.DatabaseFactory(provider)
                .Create(root, Password, false, new SharpCoreDB.DatabaseConfig { EnableBatchEncryption = true });
            return (db, provider);
        }


        private static string Key(object? key) => key?.ToString() ?? string.Empty;

        private static void EditManifest(string root, Action<JsonNode> edit)
        {
            var path = Path.Combine(root, ManifestFile);
            var json = JsonNode.Parse(File.ReadAllText(path))!;
            edit(json);
            File.WriteAllText(path, json.ToJsonString());
        }

        private static string GetStoredValue(IQueueTable table, string compositeKey)
        {
            var row = table.FindByPrimaryKey(compositeKey)!;
            return row.First(kvp => string.Equals(kvp.Key, "value", StringComparison.OrdinalIgnoreCase)).Value.ToString()!;
        }

        private static Dictionary<string, object> WithValue(Dictionary<string, object> row, string value)
        {
            var copy = new Dictionary<string, object>(row, row.Comparer);
            var valueKey = row.Keys.First(k => string.Equals(k, "value", StringComparison.OrdinalIgnoreCase));
            copy[valueKey] = value;
            return copy;
        }

        /// <summary>
        /// Pass-through <see cref="IQueueTable"/> with a pre-operation hook for throwing and a
        /// result hook for tampering with what <c>FindByPrimaryKey</c> returns.
        /// </summary>
        internal sealed class FaultingTable(IQueueTable inner) : IQueueTable
        {
            public IQueueTable Inner { get; } = inner;

            /// <summary>Invoked as (operation, key) before delegating; throw to inject a fault.</summary>
            public Action<string, object?>? Before { get; set; }

            /// <summary>Invoked as (key, row) on every lookup; return a replacement row to tamper.</summary>
            public Func<object, Dictionary<string, object>?, Dictionary<string, object>?>? OnFind { get; set; }

            public Dictionary<string, object>? FindByPrimaryKey(object key)
            {
                Before?.Invoke("Find", key);
                var row = Inner.FindByPrimaryKey(key);
                return OnFind == null ? row : OnFind(key, row);
            }

            public void Insert(Dictionary<string, object> row)
            {
                Before?.Invoke("Insert", row.TryGetValue("key", out var k) ? k : null);
                Inner.Insert(row);
            }

            public bool UpdateByPrimaryKey(object key, Dictionary<string, object> updates)
            {
                Before?.Invoke("Update", key);
                return Inner.UpdateByPrimaryKey(key, updates);
            }

            public bool DeleteByPrimaryKey(object key)
            {
                Before?.Invoke("Delete", key);
                return Inner.DeleteByPrimaryKey(key);
            }

            public List<Dictionary<string, object>> Select()
            {
                Before?.Invoke("Select", null);
                return Inner.Select();
            }

            public void Flush()
            {
                Before?.Invoke("Flush", null);
                Inner.Flush();
            }
        }
    }
}
