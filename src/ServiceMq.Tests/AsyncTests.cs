using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ServiceMq.Tests
{
    public class AsyncTests
    {
        [Fact]
        public async Task SendAsync_ReceiveAsync_EndToEnd()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("async-sender-" + suffix);
            var receiverAddress = new Address("async-receiver-" + suffix);
            using (var receiver = CreateMemoryQueue("async-receiver", receiverAddress))
            using (var sender = CreateMemoryQueue("async-sender", senderAddress))
            {
                var id = await sender.SendAsync(receiverAddress, "hello async");
                var message = await receiver.ReceiveAsync(3000);
                Assert.NotNull(message);
                Assert.Equal(id, message.Id);
                Assert.Equal("hello async", message.To<string>());
            }
        }

        [Fact]
        public async Task SendBytesAsync_ReceiveAsync_RoundTripsBytes()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("async-bytes-sender-" + suffix);
            var receiverAddress = new Address("async-bytes-receiver-" + suffix);
            var payload = new byte[] { 1, 2, 3, 4, 5, 250, 251, 252 };
            using (var receiver = CreateMemoryQueue("async-bytes-receiver", receiverAddress))
            using (var sender = CreateMemoryQueue("async-bytes-sender", senderAddress))
            {
                var id = await sender.SendBytesAsync(receiverAddress, payload, "application/octet-stream");
                var message = await receiver.ReceiveAsync(3000);
                Assert.NotNull(message);
                Assert.Equal(id, message.Id);
                Assert.Equal(payload, message.MessageBytes);
            }
        }

        [Fact]
        public async Task BroadcastAsync_DeliversToEveryReceiver()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("async-broadcast-sender-" + suffix);
            var r1 = new Address("async-broadcast-r1-" + suffix);
            var r2 = new Address("async-broadcast-r2-" + suffix);
            using (var q1 = CreateMemoryQueue("async-broadcast-r1", r1))
            using (var q2 = CreateMemoryQueue("async-broadcast-r2", r2))
            using (var sender = CreateMemoryQueue("async-broadcast-sender", senderAddress))
            {
                var id = await sender.BroadcastAsync(new[] { r1, r2 }, "broadcast payload");
                var m1 = await q1.ReceiveAsync(3000);
                var m2 = await q2.ReceiveAsync(3000);
                Assert.NotNull(m1);
                Assert.NotNull(m2);
                Assert.Equal(id, m1.Id);
                Assert.Equal(id, m2.Id);
                Assert.Equal("broadcast payload", m1.To<string>());
                Assert.Equal("broadcast payload", m2.To<string>());
            }
        }

        [Fact]
        public async Task ReceiveAsync_ReturnsNull_OnTimeout()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var receiverAddress = new Address("async-timeout-" + suffix);
            using (var receiver = CreateMemoryQueue("async-timeout", receiverAddress))
            {
                var message = await receiver.ReceiveAsync(100);
                Assert.Null(message);
            }
        }

        [Fact]
        public async Task ReceiveAsync_RespectsCancellation()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var receiverAddress = new Address("async-cancel-" + suffix);
            using (var receiver = CreateMemoryQueue("async-cancel", receiverAddress))
            using (var cts = new CancellationTokenSource(100))
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => receiver.ReceiveAsync(-1, cts.Token));
            }
        }

        [Fact]
        public async Task ReceiveBulkAsync_ReturnsBatch()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("async-bulk-sender-" + suffix);
            var receiverAddress = new Address("async-bulk-receiver-" + suffix);
            using (var receiver = CreateMemoryQueue("async-bulk-receiver", receiverAddress))
            using (var sender = CreateMemoryQueue("async-bulk-sender", senderAddress))
            {
                for (var i = 0; i < 5; i++) await sender.SendAsync(receiverAddress, i);
                Assert.True(WaitUntil(() => receiver.CountInbound == 5, 3000));
                var received = await receiver.ReceiveBulkAsync(5, 3000);
                Assert.Equal(5, received.Count);
                Assert.Equal(new[] { 0, 1, 2, 3, 4 }, received.Select(x => x.To<int>()));
            }
        }

        [Fact]
        public async Task AcceptAsync_AcknowledgeAsync_CompletesMessage()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("async-ack-sender-" + suffix);
            var receiverAddress = new Address("async-ack-receiver-" + suffix);
            using (var receiver = CreateMemoryQueue("async-ack-receiver", receiverAddress))
            using (var sender = CreateMemoryQueue("async-ack-sender", senderAddress))
            {
                await sender.SendAsync(receiverAddress, "ack me");
                var accepted = await receiver.AcceptAsync(3000);
                Assert.NotNull(accepted);
                await receiver.AcknowledgeAsync(accepted);
                // The message was completed (audited + removed), so nothing is left to receive.
                Assert.Null(await receiver.ReceiveAsync(100));
            }
        }

        [Fact]
        public async Task ReEnqueueAsync_ReturnsMessageToQueue()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("async-reenqueue-sender-" + suffix);
            var receiverAddress = new Address("async-reenqueue-receiver-" + suffix);
            var receiverOptions = MemoryOptions("async-reenqueue-receiver", receiverAddress);
            receiverOptions.VisibilityTimeout = TimeSpan.FromSeconds(5);
            using (var receiver = new MessageQueue(receiverOptions))
            using (var sender = CreateMemoryQueue("async-reenqueue-sender", senderAddress))
            {
                await sender.SendAsync(receiverAddress, "requeue me");
                var accepted = await receiver.AcceptAsync(3000);
                Assert.NotNull(accepted);
                await receiver.ReEnqueueAsync(accepted);
                var redelivered = await receiver.ReceiveAsync(3000);
                Assert.NotNull(redelivered);
                Assert.Equal(accepted.Id, redelivered.Id);
            }
        }

        [Fact]
        public async Task DeadLetter_AsyncInspectReplayDelete()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("async-dead-sender-" + suffix);
            var receiverAddress = new Address("async-dead-receiver-" + suffix);
            var senderOptions = MemoryOptions("async-dead-sender", senderAddress);
            senderOptions.ConnectTimeOutMs = 50;
            senderOptions.Delivery.MaxAttempts = 1;
            using (var sender = new MessageQueue(senderOptions))
            {
                var id = await sender.SendAsync(receiverAddress, "recover async");
                Assert.True(WaitUntil(() => sender.GetDeadLetters().Count == 1, 3000));
                var deadLetters = await sender.GetDeadLettersAsync();
                var deadLetter = deadLetters.Single();
                Assert.Equal(id, deadLetter.MessageId);

                using (var receiver = CreateMemoryQueue("async-dead-receiver", receiverAddress))
                {
                    Assert.True(await sender.ReplayDeadLetterAsync(deadLetter.Key));
                    var received = await receiver.ReceiveAsync(3000);
                    Assert.NotNull(received);
                    Assert.Equal(id, received.Id);
                    Assert.Empty(await sender.GetDeadLettersAsync());
                }
            }
        }

        [Fact]
        public async Task FlushStorageAsync_And_GetStorageHealthAsync_Work()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("async-flush-sender-" + suffix);
            var receiverAddress = new Address("async-flush-receiver-" + suffix);
            using (var receiver = CreateMemoryQueue("async-flush-receiver", receiverAddress))
            using (var sender = CreateMemoryQueue("async-flush-sender", senderAddress))
            {
                await sender.SendAsync(receiverAddress, "flush me");
                Assert.True(WaitUntil(() => receiver.CountInbound == 1, 3000));
                var message = await receiver.ReceiveAsync(3000);
                Assert.NotNull(message);
                await receiver.FlushStorageAsync();
                var health = await receiver.GetStorageHealthAsync();
                Assert.NotNull(health);
                Assert.Equal(0, health.IncomingMessages);
            }
        }


        [Fact]
        public async Task MemoryMessageStore_AsyncSurface_RoundTrips()
        {
            using (var store = new MemoryMessageStore())
            {
                await store.WriteAsync(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);
                Assert.True(await store.ContainsAsync(StorageArea.Outgoing, "a.omq"));
                Assert.Equal("payload", (await store.ReadAsync(StorageArea.Outgoing, "a.omq")).Value);
                await store.AppendAsync(StorageArea.Outgoing, "a.omq", "more", DurabilityMode.FlushToDisk);
                await store.MoveAsync(StorageArea.Outgoing, StorageArea.DeadLetter, "a.omq");
                Assert.True(await store.ContainsAsync(StorageArea.DeadLetter, "a.omq"));
                await store.DeleteAsync(StorageArea.DeadLetter, "a.omq");
                Assert.False(await store.ContainsAsync(StorageArea.DeadLetter, "a.omq"));
            }
        }

        [Fact]
        public async Task FileMessageStore_AsyncSurface_RoundTrips()
        {
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", "async-file-" + Guid.NewGuid().ToString("N"));
            try
            {
                using (var store = new FileMessageStore(root))
                {
                    await store.WriteAsync(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);
                    Assert.True(await store.ContainsAsync(StorageArea.Outgoing, "a.omq"));
                    Assert.Equal("payload", (await store.ReadAsync(StorageArea.Outgoing, "a.omq")).Value);
                    await store.AppendAsync(StorageArea.Outgoing, "a.omq", "more", DurabilityMode.FlushToDisk);
                    Assert.Equal(1, (await store.GetStatisticsAsync(StorageArea.Outgoing)).Count);
                    await store.MoveAsync(StorageArea.Outgoing, StorageArea.DeadLetter, "a.omq");
                    Assert.True(await store.ContainsAsync(StorageArea.DeadLetter, "a.omq"));
                    await store.DeleteAsync(StorageArea.DeadLetter, "a.omq");
                    Assert.False(await store.ContainsAsync(StorageArea.DeadLetter, "a.omq"));
                }
            }
            finally { if (Directory.Exists(root)) Directory.Delete(root, true); }
        }

        [Fact]
        public async Task SqliteMessageStore_AsyncSurface_RoundTrips()
        {
            var path = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", Guid.NewGuid().ToString("N"), "queue.db");
            using (var store = new SqliteMessageStore(path))
            {
                await store.WriteAsync(StorageArea.Outgoing, "a.omq", "payload", DurabilityMode.FlushToDisk);
                Assert.True(await store.ContainsAsync(StorageArea.Outgoing, "a.omq"));
                Assert.Equal("payload", (await store.ReadAsync(StorageArea.Outgoing, "a.omq")).Value);
                await store.MoveAsync(StorageArea.Outgoing, StorageArea.DeadLetter, "a.omq");
                Assert.Equal(1, (await store.GetStatisticsAsync(StorageArea.DeadLetter)).Count);
                await store.DeleteAsync(StorageArea.DeadLetter, "a.omq");
                Assert.False(await store.ContainsAsync(StorageArea.DeadLetter, "a.omq"));
            }
        }

        private static MessageQueue CreateMemoryQueue(string name, Address address)
        {
            return new MessageQueue(MemoryOptions(name, address));
        }

        private static MessageQueueOptions MemoryOptions(string name, Address address, IMessageStore store = null)
        {
            return new MessageQueueOptions
            {
                Name = name,
                Address = address,
                ConnectTimeOutMs = 50,
                Storage = new StorageOptions
                {
                    Durability = DurabilityMode.MemoryOnly,
                    Provider = store ?? new MemoryMessageStore()
                }
            };
        }

        private static bool WaitUntil(Func<bool> condition, int timeoutMs)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (condition()) return true;
                Thread.Sleep(25);
            }
            return condition();
        }
    }
}

