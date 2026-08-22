using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ServiceMq.Tests.SharpCoreDb
{
    /// <summary>
    /// Behaviours every IMessageStore is expected to provide. SqliteMessageStore gets these
    /// from SQL (ORDER BY key, transactional Move); FileMessageStore and MemoryMessageStore
    /// get them from an ordinal sort and a SortedDictionary. The SharpCoreDB provider has to
    /// supply them itself, so they are pinned here.
    /// </summary>
    public class SharpCoreDbContractTests
    {
        private const string Password = SharpCoreDbStorageTests.Password;

        private static SharpCoreDbMessageStore Open(string root) =>
            new SharpCoreDbMessageStore(root, Password);

        [Fact]
        public void GetKeys_IsOrdinallySorted()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                var inserted = new[] { "zzz.omq", "aaa.omq", "mmm.omq", "bbb.omq", "yyy.omq" };
                foreach (var key in inserted)
                    store.Write(StorageArea.Outgoing, key, "v", DurabilityMode.Buffered);

                Assert.Equal(
                    inserted.OrderBy(x => x, StringComparer.Ordinal).ToArray(),
                    store.GetKeys(StorageArea.Outgoing).ToArray());
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        /// <summary>
        /// ConfiguredMessageStore.DropOldest() takes GetKeys(area).FirstOrDefault() and treats
        /// it as the oldest record, so ordering has to survive delete/reinsert churn rather
        /// than reflecting physical row order.
        /// </summary>
        [Fact]
        public void GetKeys_FirstKeyIsOldestAfterChurn()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                const string oldest = "20260101000000-0000000001.imq";
                store.Write(StorageArea.Incoming, oldest, "old", DurabilityMode.Buffered);
                store.Write(StorageArea.Incoming, "20260101000001-0000000002.imq", "mid", DurabilityMode.Buffered);
                store.Write(StorageArea.Incoming, "20260101000002-0000000003.imq", "new", DurabilityMode.Buffered);

                store.Delete(StorageArea.Incoming, oldest);
                store.Write(StorageArea.Incoming, oldest, "old-again", DurabilityMode.Buffered);

                Assert.Equal(oldest, store.GetKeys(StorageArea.Incoming).First());
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void GetKeys_IsScopedToArea()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                store.Write(StorageArea.Incoming, "shared.key", "in", DurabilityMode.Buffered);
                store.Write(StorageArea.Outgoing, "shared.key", "out", DurabilityMode.Buffered);

                Assert.Equal(new[] { "shared.key" }, store.GetKeys(StorageArea.Incoming).ToArray());
                Assert.Equal(new[] { "shared.key" }, store.GetKeys(StorageArea.Outgoing).ToArray());
                Assert.Equal("in", store.Read(StorageArea.Incoming, "shared.key").Value);
                Assert.Equal("out", store.Read(StorageArea.Outgoing, "shared.key").Value);
                Assert.Empty(store.GetKeys(StorageArea.DeadLetter));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void GetKeys_SurvivesReopen()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                {
                    foreach (var key in new[] { "c.omq", "a.omq", "b.omq" })
                        store.Write(StorageArea.Outgoing, key, key, DurabilityMode.FlushToDisk);
                }
                using (var store = Open(root))
                {
                    Assert.Equal(new[] { "a.omq", "b.omq", "c.omq" }, store.GetKeys(StorageArea.Outgoing).ToArray());
                }
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Delete_SurvivesReopen()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                {
                    store.Write(StorageArea.Sent, "s0.log", "a", DurabilityMode.FlushToDisk);
                    store.Write(StorageArea.Sent, "s1.log", "b", DurabilityMode.FlushToDisk);
                    store.Write(StorageArea.Sent, "s2.log", "c", DurabilityMode.FlushToDisk);
                    store.Delete(StorageArea.Sent, "s0.log");
                    Assert.Equal(new[] { "s1.log", "s2.log" }, store.GetKeys(StorageArea.Sent).ToArray());
                }
                using (var store = Open(root))
                {
                    Assert.Equal(new[] { "s1.log", "s2.log" }, store.GetKeys(StorageArea.Sent).ToArray());
                    Assert.False(store.Contains(StorageArea.Sent, "s0.log"));
                    Assert.Throws<KeyNotFoundException>(() => store.Read(StorageArea.Sent, "s0.log"));
                }
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void EmptyValue_RoundTrips()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                store.Write(StorageArea.Outgoing, "empty.omq", string.Empty, DurabilityMode.FlushToDisk);
                var entry = store.Read(StorageArea.Outgoing, "empty.omq");
                Assert.Equal(string.Empty, entry.Value);
                Assert.Equal(0, entry.Length);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Move_PreservesCreatedAndLength()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                store.Write(StorageArea.Outgoing, "m.omq", "payload", DurabilityMode.FlushToDisk);
                var before = store.Read(StorageArea.Outgoing, "m.omq");

                store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "m.omq");
                var after = store.Read(StorageArea.DeadLetter, "m.omq");

                Assert.Equal("payload", after.Value);
                Assert.Equal(before.CreatedUtc, after.CreatedUtc);
                Assert.Equal(before.Length, after.Length);
                Assert.False(store.Contains(StorageArea.Outgoing, "m.omq"));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Move_ReplacesExistingDestination()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                store.Write(StorageArea.Outgoing, "dup.omq", "source", DurabilityMode.FlushToDisk);
                store.Write(StorageArea.DeadLetter, "dup.omq", "stale-destination", DurabilityMode.FlushToDisk);

                store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "dup.omq");

                Assert.Equal("source", store.Read(StorageArea.DeadLetter, "dup.omq").Value);
                Assert.Equal(1, store.GetStatistics(StorageArea.DeadLetter).Count);
                Assert.Equal(0, store.GetStatistics(StorageArea.Outgoing).Count);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Read_MissingKey_ThrowsWithoutFaultingTheStore()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                Assert.Throws<KeyNotFoundException>(() => store.Read(StorageArea.Outgoing, "nope.omq"));
                // A missing key must not land in LastException: MessageQueue surfaces that as
                // QueueState.Cautioned for the life of the queue.
                Assert.Null(store.LastException);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Purge_RemovesOnlyEntriesOlderThanTheCutoff()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                store.Write(StorageArea.Sent, "a.log", "a", DurabilityMode.FlushToDisk);
                store.Write(StorageArea.Sent, "b.log", "b", DurabilityMode.FlushToDisk);

                store.Purge(StorageArea.Sent, DateTime.UtcNow.AddDays(-1));
                Assert.Equal(2, store.GetStatistics(StorageArea.Sent).Count);

                store.Purge(StorageArea.Sent, DateTime.UtcNow.AddDays(1));
                Assert.Equal(0, store.GetStatistics(StorageArea.Sent).Count);
                Assert.Empty(store.GetKeys(StorageArea.Sent));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void GetStatistics_CountsPlaintextBytes()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                var payloads = new[] { "one", "second-payload", "third" };
                for (var i = 0; i < payloads.Length; i++)
                    store.Write(StorageArea.Outgoing, $"k{i}.omq", payloads[i], DurabilityMode.Buffered);

                var statistics = store.GetStatistics(StorageArea.Outgoing);
                Assert.Equal(payloads.Length, statistics.Count);
                Assert.Equal(payloads.Sum(x => (long)Encoding.UTF8.GetByteCount(x)), statistics.Bytes);
                Assert.NotNull(statistics.OldestUtc);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void ConcurrentWrites_AllPersist()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using var store = Open(root);
                Parallel.For(0, 200, i =>
                    store.Write(StorageArea.Outgoing, $"k-{i:D4}.omq", $"payload-{i}", DurabilityMode.Buffered));

                Assert.Equal(200, store.GetStatistics(StorageArea.Outgoing).Count);
                for (var i = 0; i < 200; i++)
                    Assert.Equal($"payload-{i}", store.Read(StorageArea.Outgoing, $"k-{i:D4}.omq").Value);
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void Constructor_RequiresAMasterPassword()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                Assert.Throws<ArgumentException>(() => new SharpCoreDbMessageStore(root, null!));
                Assert.Throws<ArgumentException>(() => new SharpCoreDbMessageStore(root, "   "));
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }

        [Fact]
        public void WrongPassword_NeverYieldsPlaintext()
        {
            var root = SharpCoreDbStorageTests.NewRoot();
            try
            {
                using (var store = Open(root))
                    store.Write(StorageArea.Outgoing, "s.omq", "classified", DurabilityMode.FlushToDisk);

                // Opening or reading must fail. What must never happen is a successful read.
                Assert.ThrowsAny<Exception>(() =>
                {
                    using var wrong = new SharpCoreDbMessageStore(root, "not-the-master-password");
                    return wrong.Read(StorageArea.Outgoing, "s.omq").Value;
                });
            }
            finally { SharpCoreDbStorageTests.Cleanup(root); }
        }
    }
}
