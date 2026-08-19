using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using Newtonsoft.Json;
using ServiceWire.NamedPipes;
using ServiceWire.TcpIp;

namespace ServiceMq
{
    public class MessageQueue : IDisposable
    {
        private readonly Address address;
        private readonly NpEndPoint npEndPoint;
        private readonly IPEndPoint ipEndPoint;
        private readonly InboundQueue inboundQueue;
        private readonly OutboundQueue outboundQueue;
        private readonly IMessageStore store;
        private readonly NpHost npHost;
        private readonly TcpHost tcpHost;
        private readonly StorageOptions storageOptions;
        private readonly Timer cleanupTimer;
        private readonly object sendLock = new object();
        private readonly JsonSerializerSettings serializerSettings = new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore
        };
        private bool disposed;

        public MessageQueue(string name, Address address, string msgDir = null, ServiceWire.ILog log = null,
            ServiceWire.IStats stats = null, double hoursReadSentLogsToLive = 48.0, int connectTimeOutMs = 500,
            bool persistMessagesSentLogs = true, bool persistMessagesReadLogs = true,
            int maxMessagesInMemory = 8192, int reorderLevel = 4096,
            bool persistMessagesAsynchronously = false)
            : this(CreateLegacyOptions(name, address, msgDir, log, stats, hoursReadSentLogsToLive,
                connectTimeOutMs, persistMessagesSentLogs, persistMessagesReadLogs,
                maxMessagesInMemory, reorderLevel, persistMessagesAsynchronously))
        {
        }

        public MessageQueue(MessageQueueOptions options)
        {
            Validate(options);
            address = options.Address;
            storageOptions = options.Storage;
            var provider = storageOptions.Provider;
            var disposeProvider = provider == null || storageOptions.DisposeProvider;
            if (provider == null)
            {
                if (storageOptions.Durability == DurabilityMode.MemoryOnly) provider = new MemoryMessageStore();
                else
                {
                    var root = storageOptions.RootPath;
                    if (string.IsNullOrWhiteSpace(root)) root = GetDefaultStoragePath(options.Name);
                    provider = new FileMessageStore(root);
                }
            }
            store = new ConfiguredMessageStore(provider, storageOptions, disposeProvider);
            outboundQueue = new OutboundQueue(store, storageOptions, options.Delivery,
                options.ConnectTimeOutMs, options.MaxMessagesInMemory, options.ReorderLevel);
            inboundQueue = new InboundQueue(store, storageOptions, options.VisibilityTimeout,
                options.MaxMessagesInMemory, options.ReorderLevel);

            if (address.Transport == Transport.Both || address.Transport == Transport.Tcp)
            {
                ipEndPoint = new IPEndPoint(IPAddress.Parse(address.IpAddress), address.Port);
                tcpHost = new TcpHost(ipEndPoint, options.Log, options.Stats);
                tcpHost.AddService<IMessageService>(new MessageService(inboundQueue));
                tcpHost.Open();
            }
            if (address.Transport == Transport.Both || address.Transport == Transport.Np)
            {
                npEndPoint = new NpEndPoint(address.ServerName, address.PipeName);
                npHost = new NpHost(npEndPoint.PipeName, options.Log, options.Stats);
                npHost.AddService<IMessageService>(new MessageService(inboundQueue));
                npHost.Open();
            }

            var cleanupInterval = storageOptions.CleanupInterval <= TimeSpan.Zero
                ? TimeSpan.FromMinutes(15) : storageOptions.CleanupInterval;
            cleanupTimer = new Timer(CleanupStorage, null, cleanupInterval, cleanupInterval);
        }

        public long CountOutbound { get { return outboundQueue.Count; } }
        public int CountInbound { get { return inboundQueue.Count; } }
        public Exception StateExceptionOutbound { get { return outboundQueue.StateException; } }
        public QueueState StateOutbound { get { return outboundQueue.State; } }
        public Exception StateExceptionInbound { get { return inboundQueue.StateException; } }
        public QueueState StateInbound { get { return inboundQueue.State; } }

        public QueueStorageHealth StorageHealth
        {
            get
            {
                var incoming = store.GetStatistics(StorageArea.Incoming);
                var outgoing = store.GetStatistics(StorageArea.Outgoing);
                var dead = store.GetStatistics(StorageArea.DeadLetter);
                var corrupt = store.GetStatistics(StorageArea.Corrupt);
                var exception = StateExceptionInbound ?? StateExceptionOutbound ?? store.LastException;
                return new QueueStorageHealth
                {
                    State = exception == null ? QueueState.Running : QueueState.Cautioned,
                    LastException = exception,
                    IncomingMessages = incoming.Count,
                    OutgoingMessages = outgoing.Count,
                    DeadLetterMessages = dead.Count,
                    CorruptMessages = corrupt.Count,
                    StoredBytes = incoming.Bytes + outgoing.Bytes + dead.Bytes + corrupt.Bytes +
                        store.GetStatistics(StorageArea.Read).Bytes + store.GetStatistics(StorageArea.Sent).Bytes,
                    OldestIncomingUtc = incoming.OldestUtc,
                    OldestOutgoingUtc = outgoing.OldestUtc
                };
            }
        }

        public void ClearState()
        {
            inboundQueue.ClearState();
            outboundQueue.ClearState();
        }

        public Guid Send<T>(Address destination, T message)
        {
            return SendMsg(JsonConvert.SerializeObject(message, serializerSettings), typeof(T).FullName,
                GetOptimalAddress(destination));
        }

        public Guid Send(Address destination, string messageType, string message)
        {
            return SendMsg(message, messageType, GetOptimalAddress(destination));
        }

        public Guid SendBytes(Address destination, byte[] message, string messageType)
        {
            return SendMsg(message, messageType, GetOptimalAddress(destination));
        }

        public Guid Broadcast<T>(IEnumerable<Address> destinations, T message)
        {
            return BroadcastMsg(JsonConvert.SerializeObject(message, serializerSettings), typeof(T).FullName,
                destinations.Select(GetOptimalAddress));
        }

        public Guid Broadcast(IEnumerable<Address> destinations, string messageType, string message)
        {
            return BroadcastMsg(message, messageType, destinations.Select(GetOptimalAddress));
        }

        public Guid BroadcastBytes(IEnumerable<Address> destinations, byte[] message, string messageType)
        {
            return BroadcastMsg(message, messageType, destinations.Select(GetOptimalAddress));
        }

        public Message Receive(int timeoutMs = -1)
        {
            ThrowIfInboundFailed();
            return inboundQueue.Receive(timeoutMs);
        }

        public IList<Message> ReceiveBulk(int maxMessagesToReceive, int timeoutMs = -1)
        {
            ThrowIfInboundFailed();
            return inboundQueue.ReceiveBulk(maxMessagesToReceive, timeoutMs);
        }

        public Message Accept(int timeoutMs = -1)
        {
            ThrowIfInboundFailed();
            return inboundQueue.Receive(timeoutMs, false);
        }

        public IList<Message> AcceptBulk(int maxMessagesToReceive, int timeoutMs = -1)
        {
            ThrowIfInboundFailed();
            return inboundQueue.ReceiveBulk(maxMessagesToReceive, timeoutMs, false);
        }

        public void Acknowledge(Message message)
        {
            ThrowIfInboundFailed();
            inboundQueue.Acknowledge(message);
        }

        public void ReEnqueue(Message message)
        {
            ThrowIfInboundFailed();
            inboundQueue.ReEnqueue(message);
        }

        public IReadOnlyList<DeadLetter> GetDeadLetters() { return outboundQueue.GetDeadLetters(); }
        public bool ReplayDeadLetter(string key) { return outboundQueue.ReplayDeadLetter(key); }
        public bool DeleteDeadLetter(string key) { return outboundQueue.DeleteDeadLetter(key); }

        public void PurgeDeadLetters()
        {
            foreach (var deadLetter in GetDeadLetters()) outboundQueue.DeleteDeadLetter(deadLetter.Key);
        }

        public IReadOnlyList<StorageEntry> GetCorruptEntries()
        {
            return store.GetKeys(StorageArea.Corrupt).Select(x => store.Read(StorageArea.Corrupt, x)).ToArray();
        }

        public bool DeleteCorruptEntry(string key)
        {
            if (!store.Contains(StorageArea.Corrupt, key)) return false;
            store.Delete(StorageArea.Corrupt, key);
            return true;
        }

        public void RunStorageMaintenance() { CleanupStorage(null); }
        public void FlushStorage() { store.Flush(); }

        private Guid SendMsg(string value, string messageType, Address destination)
        {
            lock (sendLock)
            {
                ThrowIfOutboundFailed();
                var message = NewOutbound(destination, messageType);
                message.MessageString = value;
                outboundQueue.Enqueue(message);
                return message.Id;
            }
        }

        private Guid SendMsg(byte[] value, string messageType, Address destination)
        {
            lock (sendLock)
            {
                ThrowIfOutboundFailed();
                var message = NewOutbound(destination, messageType);
                message.MessageBytes = value;
                outboundQueue.Enqueue(message);
                return message.Id;
            }
        }

        private Guid BroadcastMsg(string value, string messageType, IEnumerable<Address> destinations)
        {
            lock (sendLock)
            {
                ThrowIfOutboundFailed();
                var id = Guid.NewGuid();
                var sent = DateTime.UtcNow;
                foreach (var destination in destinations)
                {
                    var message = NewOutbound(destination, messageType, id, sent);
                    message.MessageString = value;
                    outboundQueue.Enqueue(message);
                }
                return id;
            }
        }

        private Guid BroadcastMsg(byte[] value, string messageType, IEnumerable<Address> destinations)
        {
            lock (sendLock)
            {
                ThrowIfOutboundFailed();
                var id = Guid.NewGuid();
                var sent = DateTime.UtcNow;
                foreach (var destination in destinations)
                {
                    var message = NewOutbound(destination, messageType, id, sent);
                    message.MessageBytes = value;
                    outboundQueue.Enqueue(message);
                }
                return id;
            }
        }

        private OutboundMessage NewOutbound(Address destination, string messageType, Guid? id = null, DateTime? sent = null)
        {
            return new OutboundMessage
            {
                From = address,
                To = destination,
                Id = id ?? Guid.NewGuid(),
                MessageTypeName = messageType,
                Sent = sent ?? DateTime.UtcNow
            };
        }

        private Address GetOptimalAddress(Address destination)
        {
            var chooseTcp = destination.Transport == Transport.Both && address.ServerName != destination.ServerName;
            if (chooseTcp || destination.Transport == Transport.Tcp)
            {
                if (ipEndPoint == null) throw new ArgumentException("This queue has no TCP endpoint.", "destination");
                return new Address(destination.ServerName, destination.Port);
            }
            if (npEndPoint == null) throw new ArgumentException("This queue has no named-pipe endpoint.", "destination");
            return new Address(destination.PipeName);
        }

        private void ThrowIfOutboundFailed()
        {
            if (outboundQueue.State == QueueState.Failed)
                throw new IOException("Outbound queue exception state. See inner exception.", outboundQueue.StateException);
        }

        private void ThrowIfInboundFailed()
        {
            if (inboundQueue.State == QueueState.Failed)
                throw new IOException("Inbound queue exception state. See inner exception.", inboundQueue.StateException);
        }

        private void CleanupStorage(object ignored)
        {
            try
            {
                PurgeByRetention(StorageArea.Sent, storageOptions.SentRetention);
                PurgeByRetention(StorageArea.Read, storageOptions.ReadRetention);
                PurgeByRetention(StorageArea.DeadLetter, storageOptions.DeadLetterRetention);
            }
            catch { }
        }

        private void PurgeByRetention(StorageArea area, TimeSpan retention)
        {
            if (retention != TimeSpan.MaxValue) store.Purge(area, DateTime.UtcNow - retention);
        }

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
            cleanupTimer.Dispose();
            outboundQueue.Stop();
            inboundQueue.Stop();
            if (npHost != null) npHost.Dispose();
            if (tcpHost != null) tcpHost.Dispose();
            store.Flush();
            store.Dispose();
            GC.SuppressFinalize(this);
        }

        private static MessageQueueOptions CreateLegacyOptions(string name, Address address, string msgDir,
            ServiceWire.ILog log, ServiceWire.IStats stats, double retentionHours, int connectTimeout,
            bool sentLogs, bool readLogs, int maxMemory, int reorderLevel, bool asyncWrites)
        {
            return new MessageQueueOptions
            {
                Name = name,
                Address = address,
                Log = log,
                Stats = stats,
                ConnectTimeOutMs = connectTimeout,
                MaxMessagesInMemory = maxMemory,
                ReorderLevel = reorderLevel,
                Storage = new StorageOptions
                {
                    RootPath = msgDir,
                    Durability = asyncWrites ? DurabilityMode.Buffered : DurabilityMode.FlushToDisk,
                    SentRetention = TimeSpan.FromHours(retentionHours),
                    ReadRetention = TimeSpan.FromHours(retentionHours),
                    SentAuditPayload = sentLogs ? AuditPayloadMode.Full : AuditPayloadMode.None,
                    ReadAuditPayload = readLogs ? AuditPayloadMode.Full : AuditPayloadMode.None
                }
            };
        }

        private static void Validate(MessageQueueOptions options)
        {
            if (options == null) throw new ArgumentNullException("options");
            if (string.IsNullOrWhiteSpace(options.Name)) throw new ArgumentException("A queue name is required.", "options");
            if (options.Address == null) throw new ArgumentException("A queue address is required.", "options");
            if (options.Storage == null) throw new ArgumentException("Storage options are required.", "options");
            if (options.Delivery == null) throw new ArgumentException("Delivery options are required.", "options");
            if (options.MaxMessagesInMemory < 1) throw new ArgumentOutOfRangeException("options.MaxMessagesInMemory");
            if (options.ReorderLevel < 1 || options.ReorderLevel > options.MaxMessagesInMemory)
                throw new ArgumentOutOfRangeException("options.ReorderLevel");
            if (options.Delivery.MaxAttempts < 1) throw new ArgumentOutOfRangeException("options.Delivery.MaxAttempts");
            if (options.Delivery.MaxConcurrentDestinations < 1)
                throw new ArgumentOutOfRangeException("options.Delivery.MaxConcurrentDestinations");
            if (options.Delivery.MaxAge <= TimeSpan.Zero) throw new ArgumentOutOfRangeException("options.Delivery.MaxAge");
        }

        private static string GetDefaultStoragePath(string name)
        {
            foreach (var invalid in Path.GetInvalidFileNameChars()) name = name.Replace(invalid, '_');
            return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "ServiceMq", name);
        }
    }
}
