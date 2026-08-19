using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ServiceMq.Tests
{
    public class PerformanceTests
    {
        [Fact]
        public void MemoryStore_AppendAccumulatesWithoutChangingStoredLengthSemantics()
        {
            using (var store = new MemoryMessageStore())
            {
                AssertAppendBehavior(store);
            }
        }

        [Fact]
        public void SqliteStore_AppendUsesIncrementalUpsert()
        {
            var path = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", Guid.NewGuid().ToString("N"), "append.db");
            using (var store = new SqliteMessageStore(path))
            {
                AssertAppendBehavior(store);
            }
        }

        [Fact]
        public void FileStore_AllowsConcurrentAuditAppends()
        {
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", Guid.NewGuid().ToString("N"), "append-files");
            using (var store = new FileMessageStore(root))
            {
                Parallel.For(0, 100, i =>
                    store.Append(StorageArea.Sent, "audit.log", "audit-" + i, DurabilityMode.Buffered));

                var lines = store.Read(StorageArea.Sent, "audit.log").Value
                    .Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);
                Assert.Equal(100, lines.Length);
                Assert.Equal(Enumerable.Range(0, 100).Select(x => "audit-" + x).OrderBy(x => x),
                    lines.OrderBy(x => x));
            }
        }

        [Fact]
        public void CapacityCounters_AreInitializedOnceAndDoNotRescanPerWrite()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var store = new CountingStore();
            var options = MemoryOptions("counter-sender", new Address("counter-sender-" + suffix), store);
            options.Storage.MaxMessages = 1000;
            options.ConnectTimeOutMs = 10;

            using (var sender = new MessageQueue(options))
            {
                for (var i = 0; i < 50; i++) sender.Send(new Address("counter-missing-" + suffix), i);
            }

            Assert.Equal(2, store.GetStatisticsCalls);
        }

        [Fact]
        public async Task BlockCapacity_WakesWaitingProducerWhenSpaceIsReleased()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("block-sender-" + suffix);
            var receiverAddress = new Address("block-receiver-" + suffix);
            var options = MemoryOptions("block-sender", senderAddress);
            options.Storage.MaxMessages = 1;
            options.Storage.FullBehavior = QueueFullBehavior.Block;
            options.Storage.FullWaitTimeout = TimeSpan.FromSeconds(5);

            using (var sender = new MessageQueue(options))
            {
                sender.Send(receiverAddress, 1);
                var secondSend = Task.Run(() => sender.Send(receiverAddress, 2));
                await Task.Delay(100);
                Assert.False(secondSend.IsCompleted);

                using (var receiver = new MessageQueue(MemoryOptions("block-receiver", receiverAddress)))
                {
                    Assert.Equal(1, receiver.Receive(3000).To<int>());
                    var completed = await Task.WhenAny(secondSend, Task.Delay(3000));
                    Assert.Same(secondSend, completed);
                    await secondSend;
                    Assert.Equal(2, receiver.Receive(3000).To<int>());
                }
            }
        }

        [Fact]
        public void SlowDestination_DoesNotDelayAnotherDestination()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("parallel-sender-" + suffix);
            var fastAddress = new Address("parallel-fast-" + suffix);
            var slowAddress = new Address("parallel-slow-" + suffix);
            var options = MemoryOptions("parallel-sender", senderAddress);
            options.Delivery.MaxConcurrentDestinations = 2;

            using (var slowStore = new BlockingIncomingStore())
            using (var slow = new MessageQueue(MemoryOptions("parallel-slow", slowAddress, slowStore)))
            using (var fast = new MessageQueue(MemoryOptions("parallel-fast", fastAddress)))
            using (var sender = new MessageQueue(options))
            {
                sender.Send(slowAddress, "slow");
                Assert.True(slowStore.WriteStarted.Wait(3000));
                try
                {
                    var stopwatch = Stopwatch.StartNew();
                    sender.Send(fastAddress, "fast");
                    var received = fast.Receive(750);
                    stopwatch.Stop();

                    Assert.NotNull(received);
                    Assert.Equal("fast", received.To<string>());
                    Assert.True(stopwatch.Elapsed < TimeSpan.FromMilliseconds(750));
                }
                finally { slowStore.AllowWrite.Set(); }
            }
        }

        [Fact]
        public void MaxConcurrentDestinations_MustBePositive()
        {
            var options = MemoryOptions("invalid", new Address("invalid-" + Guid.NewGuid().ToString("N")));
            options.Delivery.MaxConcurrentDestinations = 0;
            Assert.Throws<ArgumentOutOfRangeException>(() => new MessageQueue(options));
        }

        [Fact]
        public void MaxConcurrentDestinations_BoundsActiveDestinationDeliveries()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("bound-sender-" + suffix);
            var fastAddress = new Address("bound-fast-" + suffix);
            var slowAddress = new Address("bound-slow-" + suffix);
            var options = MemoryOptions("bound-sender", senderAddress);
            options.Delivery.MaxConcurrentDestinations = 1;

            using (var slowStore = new BlockingIncomingStore())
            using (var slow = new MessageQueue(MemoryOptions("bound-slow", slowAddress, slowStore)))
            using (var fast = new MessageQueue(MemoryOptions("bound-fast", fastAddress)))
            using (var sender = new MessageQueue(options))
            {
                sender.Send(slowAddress, "slow");
                Assert.True(slowStore.WriteStarted.Wait(3000));
                sender.Send(fastAddress, "fast");
                Assert.Null(fast.Receive(100));

                slowStore.AllowWrite.Set();
                var received = fast.Receive(3000);
                Assert.NotNull(received);
                Assert.Equal("fast", received.To<string>());
            }
        }

        private static void AssertAppendBehavior(IMessageStore store)
        {
            var values = Enumerable.Range(0, 100).Select(x => "audit-" + x).ToArray();
            foreach (var value in values) store.Append(StorageArea.Sent, "audit.log", value, DurabilityMode.FlushToDisk);

            var expected = string.Join(Environment.NewLine, values);
            var entry = store.Read(StorageArea.Sent, "audit.log");
            Assert.Equal(expected, entry.Value);
            Assert.Equal(Encoding.UTF8.GetByteCount(expected), entry.Length);
            Assert.Equal(entry.Length, store.GetStatistics(StorageArea.Sent).Bytes);
        }

        private static MessageQueueOptions MemoryOptions(string name, Address address, IMessageStore store = null)
        {
            return new MessageQueueOptions
            {
                Name = name,
                Address = address,
                ConnectTimeOutMs = 25,
                Storage = new StorageOptions
                {
                    Durability = DurabilityMode.MemoryOnly,
                    Provider = store ?? new MemoryMessageStore(),
                    DisposeProvider = false,
                    SentAuditPayload = AuditPayloadMode.None,
                    ReadAuditPayload = AuditPayloadMode.None
                },
                Delivery = new DeliveryOptions
                {
                    MaxAge = TimeSpan.FromHours(1),
                    InitialRetryDelay = TimeSpan.FromMilliseconds(10),
                    MaximumRetryDelay = TimeSpan.FromMilliseconds(10)
                }
            };
        }

        private sealed class CountingStore : IMessageStore
        {
            private readonly MemoryMessageStore inner = new MemoryMessageStore();

            public int GetStatisticsCalls { get; private set; }
            public Exception LastException { get { return inner.LastException; } }
            public IReadOnlyList<string> GetKeys(StorageArea area) { return inner.GetKeys(area); }
            public bool Contains(StorageArea area, string key) { return inner.Contains(area, key); }
            public StorageEntry Read(StorageArea area, string key) { return inner.Read(area, key); }
            public void Write(StorageArea area, string key, string value, DurabilityMode durability) { inner.Write(area, key, value, durability); }
            public void Append(StorageArea area, string key, string value, DurabilityMode durability) { inner.Append(area, key, value, durability); }
            public void Delete(StorageArea area, string key) { inner.Delete(area, key); }
            public void Move(StorageArea source, StorageArea destination, string key) { inner.Move(source, destination, key); }
            public void Purge(StorageArea area, DateTime olderThanUtc) { inner.Purge(area, olderThanUtc); }

            public StorageAreaStatistics GetStatistics(StorageArea area)
            {
                GetStatisticsCalls++;
                return inner.GetStatistics(area);
            }

            public void ClearException() { inner.ClearException(); }
            public void Flush() { inner.Flush(); }
            public void Dispose() { inner.Dispose(); }
        }

        private sealed class BlockingIncomingStore : IMessageStore
        {
            private readonly MemoryMessageStore inner = new MemoryMessageStore();

            public readonly ManualResetEventSlim WriteStarted = new ManualResetEventSlim(false);
            public readonly ManualResetEventSlim AllowWrite = new ManualResetEventSlim(false);
            public Exception LastException { get { return inner.LastException; } }
            public IReadOnlyList<string> GetKeys(StorageArea area) { return inner.GetKeys(area); }
            public bool Contains(StorageArea area, string key) { return inner.Contains(area, key); }
            public StorageEntry Read(StorageArea area, string key) { return inner.Read(area, key); }

            public void Write(StorageArea area, string key, string value, DurabilityMode durability)
            {
                if (area == StorageArea.Incoming)
                {
                    WriteStarted.Set();
                    if (!AllowWrite.Wait(5000)) throw new TimeoutException("The test did not release the blocked write.");
                }
                inner.Write(area, key, value, durability);
            }

            public void Append(StorageArea area, string key, string value, DurabilityMode durability) { inner.Append(area, key, value, durability); }
            public void Delete(StorageArea area, string key) { inner.Delete(area, key); }
            public void Move(StorageArea source, StorageArea destination, string key) { inner.Move(source, destination, key); }
            public void Purge(StorageArea area, DateTime olderThanUtc) { inner.Purge(area, olderThanUtc); }
            public StorageAreaStatistics GetStatistics(StorageArea area) { return inner.GetStatistics(area); }
            public void ClearException() { inner.ClearException(); }
            public void Flush() { inner.Flush(); }

            public void Dispose()
            {
                AllowWrite.Set();
                WriteStarted.Dispose();
                AllowWrite.Dispose();
                inner.Dispose();
            }
        }
    }
}
