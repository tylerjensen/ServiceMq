using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ServiceMq.Tests
{
    public class AsyncReliabilityTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AsyncDelete_ReleasesMessageAndByteCapacity(bool limitBytes)
        {
            using var inner = new MemoryMessageStore();
            using var store = new ConfiguredMessageStore(inner, new StorageOptions
            {
                MaxMessages = limitBytes ? (long?)null : 1,
                MaxBytes = limitBytes ? 5 : (long?)null
            }, false);
            store.Write(StorageArea.Incoming, "first.imq", "first", DurabilityMode.MemoryOnly);
            await store.ReadAsync(StorageArea.Incoming, "first.imq");
            await store.DeleteAsync(StorageArea.Incoming, "first.imq");
            await store.WriteAsync(StorageArea.Incoming, "next.imq", "next", DurabilityMode.MemoryOnly);
            Assert.Equal("next", inner.Read(StorageArea.Incoming, "next.imq").Value);
            Assert.Equal(1, inner.GetStatistics(StorageArea.Incoming).Count);
        }

        [Fact]
        public async Task AsyncMove_TracksCapacityAcrossActiveAndInactiveAreas()
        {
            using var inner = new MemoryMessageStore();
            using var store = new ConfiguredMessageStore(inner, new StorageOptions { MaxMessages = 1 }, false);
            store.Write(StorageArea.Incoming, "first", "payload", DurabilityMode.MemoryOnly);
            await store.MoveAsync(StorageArea.Incoming, StorageArea.DeadLetter, "first");
            await store.WriteAsync(StorageArea.Outgoing, "second", "payload", DurabilityMode.MemoryOnly);
            await store.MoveAsync(StorageArea.Outgoing, StorageArea.Incoming, "second");
            await Assert.ThrowsAsync<QueueCapacityExceededException>(() =>
                store.WriteAsync(StorageArea.Outgoing, "third", "payload", DurabilityMode.MemoryOnly));
            await store.DeleteAsync(StorageArea.Incoming, "second");
            await store.MoveAsync(StorageArea.DeadLetter, StorageArea.Outgoing, "first");
            await Assert.ThrowsAsync<QueueCapacityExceededException>(() =>
                store.WriteAsync(StorageArea.Incoming, "third", "payload", DurabilityMode.MemoryOnly));
            await store.PurgeAsync(StorageArea.Outgoing, DateTime.UtcNow.AddMinutes(1));
            await store.WriteAsync(StorageArea.Incoming, "third", "payload", DurabilityMode.MemoryOnly);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AsyncRemoval_WakesBlockedWriter(bool move)
        {
            using var waiting = new ManualResetEventSlim();
            using var inner = new ControlledStore { AfterContains = key =>
            {
                if (key == "second") waiting.Set();
            } };
            using var store = new ConfiguredMessageStore(inner, new StorageOptions
            {
                MaxMessages = 1, FullBehavior = QueueFullBehavior.Block,
                FullWaitTimeout = TimeSpan.FromSeconds(10)
            }, false);
            store.Write(StorageArea.Incoming, "first", "payload", DurabilityMode.MemoryOnly);
            var writer = Task.Run(() => store.Write(StorageArea.Incoming, "second", "payload", DurabilityMode.MemoryOnly));
            try
            {
                Assert.True(waiting.Wait(TimeSpan.FromSeconds(5)));
                Assert.False(writer.IsCompleted);
                if (move) await store.MoveAsync(StorageArea.Incoming, StorageArea.DeadLetter, "first");
                else await store.DeleteAsync(StorageArea.Incoming, "first");
                await writer.WaitAsync(TimeSpan.FromSeconds(2));
                Assert.True(inner.Contains(StorageArea.Incoming, "second"));
            }
            finally
            {
                // Purge also reconciles counters, ensuring a failed test leaves no blocked worker.
                store.Purge(StorageArea.Incoming, DateTime.UtcNow.AddMinutes(1));
                await writer;
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AsyncConsumption_ReleasesConfiguredCapacity(bool acknowledge)
        {
            using var inner = new MemoryMessageStore();
            var options = new StorageOptions { MaxMessages = 1, ReadAuditPayload = AuditPayloadMode.None };
            using var store = new ConfiguredMessageStore(inner, options, false);
            var queue = new InboundQueue(store, options, null, 16, 8);
            try
            {
                queue.Enqueue(NewMessage());
                var message = await queue.ReceiveAsync(0, !acknowledge);
                Assert.NotNull(message);
                if (acknowledge) await queue.AcknowledgeAsync(message);
                queue.Enqueue(NewMessage());
                Assert.NotNull(await queue.ReceiveAsync(0));
            }
            finally { queue.Stop(); }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Receive_CancellationAfterDequeue_ReturnsCompletedMessages(bool bulk)
        {
            using var cts = new CancellationTokenSource();
            using var store = new ControlledStore { AfterDelete = () => cts.Cancel() };
            var queue = NewInbound(store);
            try
            {
                var first = NewMessage();
                queue.Enqueue(first);
                if (bulk)
                {
                    var second = NewMessage();
                    queue.Enqueue(second);
                    var messages = await queue.ReceiveBulkAsync(2, 0, true, cts.Token);
                    Assert.Collection(messages, m => Assert.Equal(first.Id, m.Id), m => Assert.Equal(second.Id, m.Id));
                }
                else Assert.Equal(first.Id, (await queue.ReceiveAsync(0, true, cts.Token)).Id);
                Assert.True(cts.IsCancellationRequested);
                Assert.Equal(0, store.GetStatistics(StorageArea.Incoming).Count);
            }
            finally { queue.Stop(); }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Receive_PreCanceledToken_LeavesMessagesAvailable(bool bulk)
        {
            using var store = new MemoryMessageStore();
            var queue = NewInbound(store);
            try
            {
                var message = NewMessage();
                queue.Enqueue(message);
                using var cts = new CancellationTokenSource();
                cts.Cancel();
                if (bulk) await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queue.ReceiveBulkAsync(2, 0, true, cts.Token));
                else await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queue.ReceiveAsync(0, true, cts.Token));
                Assert.Equal(message.Id, (await queue.ReceiveAsync(0)).Id);
            }
            finally { queue.Stop(); }
        }

        [Fact]
        public async Task Acknowledge_PreCanceledToken_PreservesVisibilityLease()
        {
            using var store = new MemoryMessageStore();
            var queue = NewInbound(store, TimeSpan.Zero);
            try
            {
                queue.Enqueue(NewMessage());
                var message = await queue.ReceiveAsync(0, false);
                using var cts = new CancellationTokenSource();
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queue.AcknowledgeAsync(message, cts.Token));
                ExpireLeases(queue);
                Assert.Equal(message.Id, (await queue.ReceiveAsync(0, false)).Id);
            }
            finally { queue.Stop(); }
        }

        [Fact]
        public async Task Acknowledge_StorageFailure_PreservesVisibilityLease()
        {
            using var store = new ControlledStore { BeforeDelete = () => throw new IOException("Injected failure") };
            var queue = NewInbound(store, TimeSpan.Zero);
            try
            {
                queue.Enqueue(NewMessage());
                var message = await queue.ReceiveAsync(0, false);
                await Assert.ThrowsAsync<IOException>(() => queue.AcknowledgeAsync(message));
                ExpireLeases(queue);
                Assert.Equal(message.Id, (await queue.ReceiveAsync(0, false)).Id);
                Assert.Equal(1, store.GetStatistics(StorageArea.Incoming).Count);
            }
            finally { queue.Stop(); }
        }

        [Fact]
        public async Task Acknowledge_InProgress_DoesNotExpireAndFinishesAfterCancellation()
        {
            using var entered = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            using var cts = new CancellationTokenSource();
            using var store = new ControlledStore { BeforeDelete = () =>
            {
                entered.Set();
                if (!release.Wait(TimeSpan.FromSeconds(5))) throw new TimeoutException("Test did not release delete");
            } };
            var queue = NewInbound(store, TimeSpan.Zero);
            Task acknowledgement = null;
            try
            {
                queue.Enqueue(NewMessage());
                var message = await queue.ReceiveAsync(0, false);
                acknowledgement = queue.AcknowledgeAsync(message, cts.Token);
                Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                cts.Cancel();
                ExpireLeases(queue);
                Assert.Null(await queue.ReceiveAsync(0, false));
                release.Set();
                await acknowledgement;
                ExpireLeases(queue);
                Assert.Null(await queue.ReceiveAsync(0, false));
                Assert.Equal(0, store.GetStatistics(StorageArea.Incoming).Count);
            }
            finally
            {
                release.Set();
                if (acknowledgement != null) await acknowledgement;
                queue.Stop();
            }
        }

        private static InboundQueue NewInbound(IMessageStore store, TimeSpan? visibility = null) =>
            new InboundQueue(store, new StorageOptions { ReadAuditPayload = AuditPayloadMode.None }, visibility, 16, 8);

        private static Message NewMessage() => new Message
        {
            Id = Guid.NewGuid(), From = new Address("reliability-test"), Sent = DateTime.UtcNow,
            Received = DateTime.UtcNow, MessageTypeName = "string", MessageString = "payload"
        };

        // Invoke the timer callback directly so lease expiry tests do not depend on wall-clock sleeps.
        private static void ExpireLeases(InboundQueue queue) => typeof(InboundQueue)
            .GetMethod("RequeueExpiredLeases", BindingFlags.NonPublic | BindingFlags.Instance)
            .Invoke(queue, new object[] { null });

        private sealed class ControlledStore : IMessageStore
        {
            private readonly MemoryMessageStore inner = new MemoryMessageStore();
            public Action BeforeDelete { get; set; }
            public Action AfterDelete { get; set; }
            public Action<string> AfterContains { get; set; }
            public Exception LastException => inner.LastException;
            public IReadOnlyList<string> GetKeys(StorageArea area) => inner.GetKeys(area);
            public bool Contains(StorageArea area, string key)
            {
                var result = inner.Contains(area, key);
                AfterContains?.Invoke(key);
                return result;
            }
            public StorageEntry Read(StorageArea area, string key) => inner.Read(area, key);
            public void Write(StorageArea area, string key, string value, DurabilityMode durability) => inner.Write(area, key, value, durability);
            public void Append(StorageArea area, string key, string value, DurabilityMode durability) => inner.Append(area, key, value, durability);
            public void Delete(StorageArea area, string key)
            {
                BeforeDelete?.Invoke();
                inner.Delete(area, key);
                AfterDelete?.Invoke();
            }
            public void Move(StorageArea source, StorageArea destination, string key) => inner.Move(source, destination, key);
            public void Purge(StorageArea area, DateTime before) => inner.Purge(area, before);
            public StorageAreaStatistics GetStatistics(StorageArea area) => inner.GetStatistics(area);
            public void ClearException() => inner.ClearException();
            public void Flush() => inner.Flush();
            public void Dispose() => inner.Dispose();
        }
    }
}
