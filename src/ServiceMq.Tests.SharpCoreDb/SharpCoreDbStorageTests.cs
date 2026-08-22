using System;
using System.IO;
using System.Text;
using Xunit;

namespace ServiceMq.Tests.SharpCoreDb
{
    public class SharpCoreDbStorageTests
    {
        [Fact]
        public void SharpCoreDbStore_PersistsAcrossInstances()
        {
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.SharpCoreDb.Tests", Guid.NewGuid().ToString("N"));
            try
            {
                using (var store = new SharpCoreDbMessageStore(root))
                {
                    store.Write(StorageArea.Outgoing, "one.omq", "payload", DurabilityMode.FlushToDisk);
                    Assert.True(store.Contains(StorageArea.Outgoing, "one.omq"));
                    Assert.Equal("payload", store.Read(StorageArea.Outgoing, "one.omq").Value);
                }

                // Reopen the same database directory — SharpCoreDB directory mode is encrypted
                // with AES-256-GCM derived from the default master password, so the default
                // password must be reused (or an explicit one).
                using (var store = new SharpCoreDbMessageStore(root))
                {
                    Assert.Equal("payload", store.Read(StorageArea.Outgoing, "one.omq").Value);
                    store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "one.omq");
                    Assert.Equal(1, store.GetStatistics(StorageArea.DeadLetter).Count);
                    Assert.Equal(0, store.GetStatistics(StorageArea.Outgoing).Count);
                    store.Delete(StorageArea.DeadLetter, "one.omq");
                    Assert.False(store.Contains(StorageArea.DeadLetter, "one.omq"));
                }
            }
            finally
            {
                Cleanup(root);
            }
        }

        [Fact]
        public void SharpCoreDbStore_AppendAndWriteOverwrite()
        {
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.SharpCoreDb.Tests", Guid.NewGuid().ToString("N"));
            try
            {
                using (var store = new SharpCoreDbMessageStore(root))
                {
                    store.Append(StorageArea.Incoming, "one.imq", "first", DurabilityMode.FlushToDisk);
                    store.Append(StorageArea.Incoming, "one.imq", "second", DurabilityMode.FlushToDisk);
                    var entry = store.Read(StorageArea.Incoming, "one.imq");
                    Assert.Equal("first" + Environment.NewLine + "second", entry.Value);

                    store.Write(StorageArea.Incoming, "one.imq", "third", DurabilityMode.FlushToDisk);
                    Assert.Equal("third", store.Read(StorageArea.Incoming, "one.imq").Value);
                }
            }
            finally
            {
                Cleanup(root);
            }
        }

        [Fact]
        public void SharpCoreDbStore_EndToEndThroughMessageQueue()
        {
            var suffix = Guid.NewGuid().ToString("N");
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.SharpCoreDb.Tests", suffix);
            var senderAddress = new Address("scdb-sender-" + suffix);
            var receiverAddress = new Address("scdb-receiver-" + suffix);
            try
            {
                var receiverOptions = new MessageQueueOptions
                {
                    Name = "scdb-receiver",
                    Address = receiverAddress,
                    Storage = new StorageOptions
                    {
                        Durability = DurabilityMode.FlushToDisk,
                        Provider = new SharpCoreDbMessageStore(Path.Combine(root, "receiver"))
                    }
                };
                using (var receiver = new MessageQueue(receiverOptions))
                using (var sender = new MessageQueue(new MessageQueueOptions
                {
                    Name = "scdb-sender",
                    Address = senderAddress,
                    Storage = new StorageOptions
                    {
                        Durability = DurabilityMode.FlushToDisk,
                        Provider = new SharpCoreDbMessageStore(Path.Combine(root, "sender"))
                    }
                }))
                {
                    sender.Send(receiverAddress, "hello sharpcoredb");
                    var message = receiver.Receive(3000);
                    Assert.NotNull(message);
                    Assert.Equal("hello sharpcoredb", message.To<string>());
                    Assert.Equal(0, receiver.StorageHealth.IncomingMessages);
                    Assert.Equal(0, sender.StorageHealth.OutgoingMessages);
                }
            }
            finally
            {
                Cleanup(root);
            }
        }

        [Fact]
        public void SharpCoreDbStore_PayloadIsEncryptedAtRest()
        {
            var root = Path.Combine(Path.GetTempPath(), "ServiceMq.SharpCoreDb.Tests", Guid.NewGuid().ToString("N"));
            const string secret = "super-secret-queue-payload";
            try
            {
                using (var store = new SharpCoreDbMessageStore(root))
                {
                    store.Write(StorageArea.Outgoing, "secret.omq", secret, DurabilityMode.FlushToDisk);
                }

                // SharpCoreDB stores payloads encrypted with AES-256-GCM (master password
                // derived). The plaintext value must not appear anywhere inside the database.
                Assert.False(ContainsText(root, secret),
                    "Encrypted storage must not contain the plaintext payload.");
                Assert.False(ContainsText(root, Encoding.UTF8.GetBytes(secret)),
                    "Encrypted storage must not contain the plaintext payload bytes.");

                // And reopening with the default password still reads it back.
                using (var store = new SharpCoreDbMessageStore(root))
                {
                    Assert.Equal(secret, store.Read(StorageArea.Outgoing, "secret.omq").Value);
                }
            }
            finally
            {
                Cleanup(root);
            }
        }

        private static void Cleanup(string path)
        {
            try
            {
                if (Directory.Exists(path)) Directory.Delete(path, true);
            }
            catch { /* best-effort cleanup */ }
        }

        private static bool ContainsText(string directory, string text)
        {
            return ContainsText(directory, Encoding.UTF8.GetBytes(text));
        }

        private static bool ContainsText(string directory, byte[] bytes)
        {
            if (!Directory.Exists(directory)) return false;
            foreach (var file in Directory.EnumerateFiles(directory, "*", SearchOption.AllDirectories))
            {
                try
                {
                    var content = File.ReadAllBytes(file);
                    if (IndexOf(content, bytes) >= 0) return true;
                }
                catch { /* skip locked/temp files */ }
            }
            return false;
        }

        private static int IndexOf(byte[] haystack, byte[] needle)
        {
            if (needle.Length == 0) return 0;
            for (int i = 0; i <= haystack.Length - needle.Length; i++)
            {
                bool match = true;
                for (int j = 0; j < needle.Length; j++)
                {
                    if (haystack[i + j] != needle[j]) { match = false; break; }
                }
                if (match) return i;
            }
            return -1;
        }
    }
}