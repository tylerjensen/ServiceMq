using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Xunit;

namespace ServiceMq.Tests
{
    public class StorageTests
    {
        [Fact]
        public void MemoryStore_EndToEndAndHealth()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("memory-sender-" + suffix);
            var receiverAddress = new Address("memory-receiver-" + suffix);
            using (var receiver = CreateMemoryQueue("receiver", receiverAddress))
            using (var sender = CreateMemoryQueue("sender", senderAddress))
            {
                sender.Send(receiverAddress, "hello memory");
                var message = receiver.Receive(3000);
                Assert.NotNull(message);
                Assert.Equal("hello memory", message.To<string>());
                Assert.Equal(0, receiver.StorageHealth.IncomingMessages);
                Assert.Equal(0, sender.StorageHealth.OutgoingMessages);
            }
        }

        [Fact]
        public void CapacityLimit_RejectsAdditionalMessages()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var options = MemoryOptions("capacity", new Address("capacity-source-" + suffix));
            options.Storage.MaxMessages = 1;
            options.Storage.FullBehavior = QueueFullBehavior.Reject;
            options.ConnectTimeOutMs = 50;
            using (var queue = new MessageQueue(options))
            {
                var unavailable = new Address("capacity-missing-" + suffix);
                queue.Send(unavailable, "first");
                Assert.Throws<QueueCapacityExceededException>(() => queue.Send(unavailable, "second"));
            }
        }

        [Fact]
        public void Accept_VisibilityTimeout_RequeuesUnacknowledgedMessage()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("lease-sender-" + suffix);
            var receiverAddress = new Address("lease-receiver-" + suffix);
            var receiverOptions = MemoryOptions("lease-receiver", receiverAddress);
            receiverOptions.VisibilityTimeout = TimeSpan.FromMilliseconds(100);
            using (var receiver = new MessageQueue(receiverOptions))
            using (var sender = CreateMemoryQueue("lease-sender", senderAddress))
            {
                sender.Send(receiverAddress, "leased");
                var accepted = receiver.Accept(3000);
                Assert.NotNull(accepted);
                var redelivered = receiver.Receive(3000);
                Assert.NotNull(redelivered);
                Assert.Equal(accepted.Id, redelivered.Id);
            }
        }

        [Fact]
        public void DeadLetter_CanBeInspectedAndReplayed()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderAddress = new Address("dead-sender-" + suffix);
            var receiverAddress = new Address("dead-receiver-" + suffix);
            var senderOptions = MemoryOptions("dead-sender", senderAddress);
            senderOptions.ConnectTimeOutMs = 50;
            senderOptions.Delivery.MaxAttempts = 1;
            using (var sender = new MessageQueue(senderOptions))
            {
                var id = sender.Send(receiverAddress, "recover me");
                Assert.True(WaitUntil(() => sender.GetDeadLetters().Count == 1, 3000));
                var deadLetter = sender.GetDeadLetters().Single();
                Assert.Equal(id, deadLetter.MessageId);
                using (var receiver = CreateMemoryQueue("dead-receiver", receiverAddress))
                {
                    Assert.True(sender.ReplayDeadLetter(deadLetter.Key));
                    var received = receiver.Receive(3000);
                    Assert.NotNull(received);
                    Assert.Equal(id, received.Id);
                    Assert.Empty(sender.GetDeadLetters());
                }
            }
        }

        [Fact]
        public void SqliteStore_PersistsAcrossInstances()
        {
            var path = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", Guid.NewGuid().ToString("N"), "queue.db");
            using (var store = new SqliteMessageStore(path))
            {
                store.Write(StorageArea.Outgoing, "one.omq", "payload", DurabilityMode.FlushToDisk);
                Assert.True(store.Contains(StorageArea.Outgoing, "one.omq"));
            }
            using (var store = new SqliteMessageStore(path))
            {
                Assert.Equal("payload", store.Read(StorageArea.Outgoing, "one.omq").Value);
                store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "one.omq");
                Assert.Equal(1, store.GetStatistics(StorageArea.DeadLetter).Count);
            }
        }

        [Fact]
        public void AesProtector_RoundTripsAndDetectsTampering()
        {
            var protector = new AesStorageProtector(Encoding.UTF8.GetBytes("0123456789abcdef0123456789abcdef"));
            var protectedValue = protector.Protect("sensitive payload");
            Assert.DoesNotContain("sensitive payload", protectedValue);
            Assert.Equal("sensitive payload", protector.Unprotect(protectedValue));
            var tampered = protectedValue.Substring(0, protectedValue.Length - 2) + "AA";
            Assert.ThrowsAny<Exception>(() => protector.Unprotect(tampered));
        }

        [Fact]
        public void LegacyInboundRecord_RemainsReadable()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", suffix);
            var incoming = Path.Combine(root, "in");
            Directory.CreateDirectory(incoming);
            var address = new Address("legacy-" + suffix);
            var id = Guid.NewGuid();
            var now = DateTime.Now;
            var record = string.Join("\t", id, address, now.ToString("yyyyMMddHHmmssfff"),
                now.ToString("yyyyMMddHHmmssfff"), 1, typeof(string).FullName, "str", "\"legacy\"");
            File.WriteAllText(Path.Combine(incoming, "0000000000000000000-legacy.imq"), record);
            using (var queue = new MessageQueue("legacy", address, root))
            {
                var message = queue.Receive(1000);
                Assert.NotNull(message);
                Assert.Equal("legacy", message.To<string>());
            }
        }

        [Fact]
        public void MetadataOnlyAudit_DoesNotStorePayload()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var senderStore = new MemoryMessageStore();
            var receiverStore = new MemoryMessageStore();
            var senderAddress = new Address("audit-sender-" + suffix);
            var receiverAddress = new Address("audit-receiver-" + suffix);
            var senderOptions = MemoryOptions("audit-sender", senderAddress, senderStore);
            var receiverOptions = MemoryOptions("audit-receiver", receiverAddress, receiverStore);
            senderOptions.Storage.SentAuditPayload = AuditPayloadMode.MetadataOnly;
            receiverOptions.Storage.ReadAuditPayload = AuditPayloadMode.MetadataOnly;
            using (var receiver = new MessageQueue(receiverOptions))
            using (var sender = new MessageQueue(senderOptions))
            {
                sender.Send(receiverAddress, "secret value");
                Assert.NotNull(receiver.Receive(3000));
            }
            var sent = senderStore.Read(StorageArea.Sent, senderStore.GetKeys(StorageArea.Sent).Single()).Value;
            var read = receiverStore.Read(StorageArea.Read, receiverStore.GetKeys(StorageArea.Read).Single()).Value;
            Assert.DoesNotContain("secret value", sent);
            Assert.DoesNotContain("secret value", read);
        }

        [Fact]
        public void CorruptRecord_IsQuarantinedWithoutBlockingStartup()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", suffix);
            Directory.CreateDirectory(Path.Combine(root, "in"));
            File.WriteAllText(Path.Combine(root, "in", "bad.imq"), "not a message");
            using (var queue = new MessageQueue("corrupt", new Address("corrupt-" + suffix), root))
            {
                Assert.Equal(0, queue.CountInbound);
                Assert.Equal(1, queue.StorageHealth.CorruptMessages);
                Assert.Single(queue.GetCorruptEntries());
            }
        }

        [Fact]
        public void DropOldestCapacityPolicy_DropsTheOldestActiveMessage()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var sourceAddress = new Address("drop-source-" + suffix);
            var destinationAddress = new Address("drop-destination-" + suffix);
            var sourceOptions = MemoryOptions("drop-source", sourceAddress);
            sourceOptions.ConnectTimeOutMs = 50;
            sourceOptions.Storage.MaxMessages = 1;
            sourceOptions.Storage.FullBehavior = QueueFullBehavior.DropOldest;
            using (var source = new MessageQueue(sourceOptions))
            {
                source.Send(destinationAddress, "old");
                Thread.Sleep(150); // allow the first connection attempt to enter the retry queue
                source.Send(destinationAddress, "new");
                using (var destination = CreateMemoryQueue("drop-destination", destinationAddress))
                {
                    var received = destination.Receive(3000);
                    Assert.NotNull(received);
                    Assert.Equal("new", received.To<string>());
                }
            }
        }

        [Fact]
        public void FileStore_DeleteIsPortableAndAtomicWriteLeavesNoTemporaryFile()
        {
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", Guid.NewGuid().ToString("N"));
            using (var store = new FileMessageStore(root))
            {
                store.Write(StorageArea.Incoming, "one.imq", "first", DurabilityMode.FlushToDisk);
                store.Write(StorageArea.Incoming, "one.imq", "second", DurabilityMode.FlushToDisk);
                Assert.Equal("second", store.Read(StorageArea.Incoming, "one.imq").Value);
                Assert.DoesNotContain(Directory.GetFiles(Path.Combine(root, "in")), x => x.EndsWith(".tmp"));
                store.Delete(StorageArea.Incoming, "one.imq");
                Assert.False(store.Contains(StorageArea.Incoming, "one.imq"));
            }
        }

        [Fact]
        public void FastFile_DisposeDrainsBufferedWritesAppendsAndDeletes()
        {
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);
            var messagePath = Path.Combine(root, "message.txt");
            var logPath = Path.Combine(root, "audit.log");
            var deletePath = Path.Combine(root, "delete.txt");
            File.WriteAllText(deletePath, "delete me");
            using (var files = new FastFile(asyncDeletes: true, asyncAppends: true, asyncWrites: true))
            {
                files.WriteAllText(messagePath, "first");
                files.WriteAllText(messagePath, "second");
                files.AppendAllLines(logPath, new[] { "one" });
                files.AppendAllLines(logPath, new[] { "two" });
                files.Delete(deletePath);
            }
            Assert.Equal("second", File.ReadAllText(messagePath));
            Assert.Equal(new[] { "one", "two" }, File.ReadAllLines(logPath));
            Assert.False(File.Exists(deletePath));
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
