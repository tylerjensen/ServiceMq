using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace ServiceMq.Tests
{
    public class OrderingTests
    {
        [Fact]
        public void ReceiveBulk_PreservesFifo_WhenInboundQueueSpillsPastMemoryCache()
        {
            const int count = 100;
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("spill-sender-" + suffix);
            var receiverAddress = new Address("spill-receiver-" + suffix);

            using (var receiver = new MessageQueue(MemoryOptions("spill-receiver", receiverAddress)))
            using (var sender = new MessageQueue(MemoryOptions("spill-sender", senderAddress)))
            {
                for (var i = 0; i < count; i++) sender.Send(receiverAddress, i);

                Assert.True(WaitUntil(() => receiver.CountInbound == count, 5000));
                var received = receiver.ReceiveBulk(count, 1000);

                Assert.Equal(count, received.Count);
                Assert.Equal(Enumerable.Range(0, count), received.Select(x => x.To<int>()));
            }
        }

        [Fact]
        public void Delivery_PreservesFifo_AcrossOutageRestartAndUnorderedProviderKeys()
        {
            const int count = 75;
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("restart-sender-" + suffix);
            var receiverAddress = new Address("restart-receiver-" + suffix);
            var senderStore = new ReverseKeyStore();

            using (var sender = new MessageQueue(MemoryOptions("restart-sender", senderAddress, senderStore)))
            {
                for (var i = 0; i < count; i++) sender.Send(receiverAddress, i);
            }

            using (var receiver = new MessageQueue(MemoryOptions("restart-receiver", receiverAddress)))
            using (var restartedSender = new MessageQueue(MemoryOptions("restart-sender", senderAddress, senderStore)))
            {
                Assert.True(WaitUntil(() => receiver.CountInbound == count, 5000));
                var received = receiver.ReceiveBulk(count, 1000);

                Assert.Equal(count, received.Count);
                Assert.Equal(Enumerable.Range(0, count), received.Select(x => x.To<int>()));
            }
        }

        [Fact]
        public void NewDurableKey_SortsAfterExistingKey_WhenClockMovesBackward()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("clock-sender-" + suffix);
            var receiverAddress = new Address("clock-receiver-" + suffix);
            var senderStore = new MemoryMessageStore();
            var options = MemoryOptions("clock-sender", senderAddress, senderStore);

            using (var sender = new MessageQueue(options)) sender.Send(receiverAddress, 1);

            var originalKey = senderStore.GetKeys(StorageArea.Outgoing).Single();
            var original = senderStore.Read(StorageArea.Outgoing, originalKey);
            var futureKey = "209912312359599999999-0000000001-" + Guid.NewGuid().ToString("N") + ".omq";
            senderStore.Write(StorageArea.Outgoing, futureKey, original.Value, DurabilityMode.MemoryOnly);
            senderStore.Delete(StorageArea.Outgoing, originalKey);

            using (var sender = new MessageQueue(options)) sender.Send(receiverAddress, 2);

            Assert.Equal(2, senderStore.GetKeys(StorageArea.Outgoing).Count);
            Assert.True(string.CompareOrdinal(senderStore.GetKeys(StorageArea.Outgoing)[0], futureKey) >= 0);

            using (var receiver = new MessageQueue(MemoryOptions("clock-receiver", receiverAddress)))
            using (var sender = new MessageQueue(options))
            {
                Assert.True(WaitUntil(() => receiver.CountInbound == 2, 5000));
                var received = receiver.ReceiveBulk(2, 1000);
                Assert.Equal(new[] { 1, 2 }, received.Select(x => x.To<int>()).ToArray());
            }
        }

        [Fact]
        public void DefaultDequeue_DoesNotPerformPerMessageExistenceReads()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("fast-sender-" + suffix);
            var receiverAddress = new Address("fast-receiver-" + suffix);
            var senderStore = new ReverseKeyStore();
            var receiverStore = new ReverseKeyStore();

            using (var receiver = new MessageQueue(MemoryOptions("fast-receiver", receiverAddress, receiverStore)))
            using (var sender = new MessageQueue(MemoryOptions("fast-sender", senderAddress, senderStore)))
            {
                for (var i = 0; i < 20; i++) sender.Send(receiverAddress, i);
                Assert.True(WaitUntil(() => receiver.CountInbound == 20, 5000));
                Assert.Equal(20, receiver.ReceiveBulk(20, 1000).Count);
            }

            Assert.Equal(0, senderStore.ContainsCalls);
            Assert.Equal(0, receiverStore.ContainsCalls);
        }

        [Fact]
        public void DropOldest_WithSpilledAndRoutedMessages_DeliversOnlyNewestInFifoOrder()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("drop-spill-sender-" + suffix);
            var receiverAddress = new Address("drop-spill-receiver-" + suffix);
            var options = MemoryOptions("drop-spill-sender", senderAddress);
            options.MaxMessagesInMemory = 1;
            options.ReorderLevel = 1;
            options.Storage.MaxMessages = 3;
            options.Storage.FullBehavior = QueueFullBehavior.DropOldest;
            options.Delivery.InitialRetryDelay = TimeSpan.FromSeconds(1);
            options.Delivery.MaximumRetryDelay = TimeSpan.FromSeconds(1);

            using (var sender = new MessageQueue(options))
            {
                for (var i = 0; i < 10; i++) sender.Send(receiverAddress, i);
                Thread.Sleep(100); // let the first unavailable attempt finish before bringing the receiver up

                using (var receiver = new MessageQueue(MemoryOptions("drop-spill-receiver", receiverAddress)))
                {
                    Assert.True(WaitUntil(() => receiver.CountInbound == 3, 5000),
                        "Inbound=" + receiver.CountInbound + ", outbound=" + sender.CountOutbound +
                        ", state=" + sender.StateOutbound + ", error=" + sender.StateExceptionOutbound);
                    var received = receiver.ReceiveBulk(3, 1000);
                    Assert.Equal(new[] { 7, 8, 9 }, received.Select(x => x.To<int>()).ToArray());
                    Assert.True(WaitUntil(() => sender.CountOutbound == 0, 1000));
                }
            }
        }

        private static MessageQueueOptions MemoryOptions(string name, Address address, IMessageStore store = null)
        {
            return new MessageQueueOptions
            {
                Name = name,
                Address = address,
                ConnectTimeOutMs = 25,
                MaxMessagesInMemory = 4,
                ReorderLevel = 2,
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

        private static bool WaitUntil(Func<bool> condition, int timeoutMs)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (condition()) return true;
                Thread.Sleep(10);
            }
            return condition();
        }

        private sealed class ReverseKeyStore : IMessageStore
        {
            private readonly MemoryMessageStore inner = new MemoryMessageStore();

            public int ContainsCalls { get; private set; }
            public Exception LastException { get { return inner.LastException; } }

            public IReadOnlyList<string> GetKeys(StorageArea area)
            {
                return inner.GetKeys(area).Reverse().ToArray();
            }

            public bool Contains(StorageArea area, string key)
            {
                ContainsCalls++;
                return inner.Contains(area, key);
            }

            public StorageEntry Read(StorageArea area, string key) { return inner.Read(area, key); }
            public void Write(StorageArea area, string key, string value, DurabilityMode durability) { inner.Write(area, key, value, durability); }
            public void Append(StorageArea area, string key, string value, DurabilityMode durability) { inner.Append(area, key, value, durability); }
            public void Delete(StorageArea area, string key) { inner.Delete(area, key); }
            public void Move(StorageArea source, StorageArea destination, string key) { inner.Move(source, destination, key); }
            public void Purge(StorageArea area, DateTime olderThanUtc) { inner.Purge(area, olderThanUtc); }
            public StorageAreaStatistics GetStatistics(StorageArea area) { return inner.GetStatistics(area); }
            public void ClearException() { inner.ClearException(); }
            public void Flush() { inner.Flush(); }
            public void Dispose() { inner.Dispose(); }
        }
    }
}
