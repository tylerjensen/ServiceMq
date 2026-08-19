using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ServiceWire;
using ServiceWire.NamedPipes;
using ServiceWire.TcpIp;

namespace ServiceMq
{
    public enum QueueState { Running, Cautioned, Failed }

    internal sealed class OutboundQueue
    {
        private sealed class DestinationState
        {
            public readonly object SyncRoot = new object();
            public CachingQueue<OutboundMessage> Messages;
            public string Key;
            public bool Scheduled;
            public bool Processing;
            public DateTime RetryAfterUtc;
        }

        private readonly CachingQueue<OutboundMessage> queue;
        private readonly Dictionary<string, DestinationState> destinations =
            new Dictionary<string, DestinationState>(StringComparer.Ordinal);
        private readonly ConcurrentQueue<DestinationState> readyDestinations =
            new ConcurrentQueue<DestinationState>();
        private readonly SemaphoreSlim readySignal = new SemaphoreSlim(0);
        private readonly IMessageStore store;
        private readonly StorageOptions storageOptions;
        private readonly DeliveryOptions deliveryOptions;
        private readonly int connectTimeOutMs;
        private readonly bool validateExistence;
        private readonly object enqueueLock = new object();
        private readonly MonotonicQueueKeyGenerator keyGenerator;
        private readonly ManualResetEvent outgoingSignal = new ManualResetEvent(false);
        private readonly Timer retryTimer;
        private readonly Task dispatchTask;
        private readonly Task[] deliveryTasks;
        private readonly PooledDictionary<string, NpClient<IMessageService>> npClientPool =
            new PooledDictionary<string, NpClient<IMessageService>>();
        private readonly PooledDictionary<string, TcpClient<IMessageService>> tcpClientPool =
            new PooledDictionary<string, TcpClient<IMessageService>>();
        private volatile bool continueProcessing = true;
        private long pendingCount;
        private Exception stateException;
        private QueueState state = QueueState.Running;

        public OutboundQueue(IMessageStore store, StorageOptions storageOptions, DeliveryOptions deliveryOptions,
            int connectTimeOutMs, int maxMessagesInMemory, int reorderLevel)
        {
            this.store = store;
            this.storageOptions = storageOptions;
            this.deliveryOptions = deliveryOptions;
            this.connectTimeOutMs = connectTimeOutMs;
            validateExistence = RequiresExistenceValidation(storageOptions);
            var existingKeys = store.GetKeys(StorageArea.Outgoing);
            keyGenerator = new MonotonicQueueKeyGenerator(existingKeys);
            queue = new CachingQueue<OutboundMessage>(store, StorageArea.Outgoing, ".omq",
                OutboundMessage.Deserialize, x => x.ToString(), maxMessagesInMemory, reorderLevel,
                storageOptions.Durability, true, validateExistence, existingKeys, OnQueueRecordDiscarded);
            pendingCount = queue.Count;

            dispatchTask = Task.Factory.StartNew(DispatchMessages, CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
            deliveryTasks = new Task[deliveryOptions.MaxConcurrentDestinations];
            for (var i = 0; i < deliveryTasks.Length; i++)
                deliveryTasks[i] = Task.Factory.StartNew(DeliverMessages, CancellationToken.None,
                    TaskCreationOptions.LongRunning, TaskScheduler.Default);

            if (queue.Count > 0) outgoingSignal.Set();
            retryTimer = new Timer(ScheduleDueDestinations, null, 100, 100);
        }

        public long Count { get { return Interlocked.Read(ref pendingCount); } }
        public Exception StateException { get { return stateException ?? queue.ReloadException ?? store.LastException; } }
        public QueueState State { get { return state == QueueState.Failed ? state : StateException == null ? state : QueueState.Cautioned; } }
        public void ClearState() { stateException = null; state = QueueState.Running; queue.ClearException(); store.ClearException(); }

        public void Stop()
        {
            continueProcessing = false;
            retryTimer.Dispose();
            outgoingSignal.Set();
            for (var i = 0; i < deliveryTasks.Length; i++) readySignal.Release();

            var stopped = dispatchTask.Wait(5000);
            try { stopped = Task.WaitAll(deliveryTasks, 5000) && stopped; }
            catch (AggregateException ex) { stateException = ex.Flatten(); stopped = false; }
            if (!stopped) stateException = new TimeoutException("The outbound queue did not stop within five seconds.");

            npClientPool.Dispose();
            tcpClientPool.Dispose();
            readySignal.Dispose();
            outgoingSignal.Dispose();
        }

        public void Enqueue(OutboundMessage message)
        {
            lock (enqueueLock)
            {
                var key = keyGenerator.Next(".omq");
                message.Filename = key;
                queue.Enqueue(key, message);
                Interlocked.Increment(ref pendingCount);
            }
            outgoingSignal.Set();
        }

        public IReadOnlyList<DeadLetter> GetDeadLetters()
        {
            var result = new List<DeadLetter>();
            foreach (var key in store.GetKeys(StorageArea.DeadLetter).Where(x => x.EndsWith(".dlq", StringComparison.OrdinalIgnoreCase)))
            {
                try
                {
                    string reason;
                    var message = ParseDeadLetter(key, store.Read(StorageArea.DeadLetter, key).Value, out reason);
                    result.Add(new DeadLetter
                    {
                        Key = key,
                        MessageId = message.Id,
                        Destination = message.To,
                        Sent = message.Sent,
                        Attempts = message.SendAttempts,
                        MessageTypeName = message.MessageTypeName,
                        Reason = reason
                    });
                }
                catch (Exception ex) { stateException = ex; }
            }
            return result;
        }

        public bool ReplayDeadLetter(string key)
        {
            if (!store.Contains(StorageArea.DeadLetter, key)) return false;
            string reason;
            var message = ParseDeadLetter(key, store.Read(StorageArea.DeadLetter, key).Value, out reason);
            message.SendAttempts = 0;
            message.LastSendAttempt = default(DateTime);
            lock (enqueueLock)
            {
                message.Filename = keyGenerator.Next(".omq");
                queue.Enqueue(message.Filename, message);
                Interlocked.Increment(ref pendingCount);
            }
            store.Delete(StorageArea.DeadLetter, key);
            outgoingSignal.Set();
            return true;
        }

        public bool DeleteDeadLetter(string key)
        {
            if (!store.Contains(StorageArea.DeadLetter, key)) return false;
            store.Delete(StorageArea.DeadLetter, key);
            return true;
        }

        private void DispatchMessages()
        {
            while (continueProcessing)
            {
                try
                {
                    if (!outgoingSignal.WaitOne(100)) continue;
                    if (!continueProcessing) break;

                    OutboundMessage message;
                    while (continueProcessing && (message = queue.Dequeue()) != null)
                        RouteMessage(message);

                    outgoingSignal.Reset();
                    if (queue.Count > 0) outgoingSignal.Set();
                }
                catch (Exception ex) { stateException = ex; state = QueueState.Cautioned; }
            }
        }

        private void RouteMessage(OutboundMessage message)
        {
            var key = message.To.ToFileNameString();
            DestinationState destination;
            lock (destinations)
            {
                if (!destinations.TryGetValue(key, out destination))
                {
                    destination = new DestinationState
                    {
                        Key = key,
                        Messages = new CachingQueue<OutboundMessage>(store, StorageArea.Outgoing, ".omq",
                            OutboundMessage.Deserialize, x => x.ToString(), 1, 1, storageOptions.Durability,
                            false, validateExistence, null, OnQueueRecordDiscarded)
                    };
                    destinations.Add(key, destination);
                }
                lock (destination.SyncRoot) destination.Messages.ReEnqueue(message.Filename, message);
            }
            ScheduleDestination(destination);
        }

        private void DeliverMessages()
        {
            while (continueProcessing)
            {
                try
                {
                    if (!readySignal.Wait(100)) continue;
                    if (!continueProcessing) break;

                    DestinationState destination;
                    if (!readyDestinations.TryDequeue(out destination)) continue;

                    OutboundMessage message;
                    lock (destination.SyncRoot)
                    {
                        destination.Scheduled = false;
                        if (destination.Processing || destination.Messages.Count == 0 ||
                            destination.RetryAfterUtc > DateTime.UtcNow) continue;
                        message = destination.Messages.Peek();
                        if (message != null) destination.Processing = true;
                    }
                    if (message == null)
                    {
                        ScheduleOrRemoveDestination(destination);
                        continue;
                    }

                    var completed = false;
                    try { completed = TryDeliver(message); }
                    catch (Exception ex) { stateException = ex; state = QueueState.Cautioned; }

                    lock (destination.SyncRoot)
                    {
                        destination.Processing = false;
                        if (completed)
                        {
                            destination.Messages.DequeueWithoutValidation();
                            destination.RetryAfterUtc = default(DateTime);
                            Interlocked.Decrement(ref pendingCount);
                        }
                        else destination.RetryAfterUtc = GetNextAttemptUtc(message);
                    }
                    ScheduleOrRemoveDestination(destination);
                }
                catch (Exception ex) { stateException = ex; state = QueueState.Cautioned; }
            }
        }

        private bool TryDeliver(OutboundMessage message)
        {
            if (validateExistence && !store.Contains(StorageArea.Outgoing, message.Filename)) return true;

            message.LastSendAttempt = DateTime.UtcNow;
            message.SendAttempts++;
            store.Write(StorageArea.Outgoing, message.Filename, message.ToString(), storageOptions.Durability);
            try
            {
                SendMessage(message);
                LogSent(message);
                return true;
            }
            catch (Exception ex)
            {
                if (!ShouldDeadLetter(message)) return false;
                DeadLetterMessage(message, ex);
                return true;
            }
        }

        private void OnQueueRecordDiscarded(string ignored)
        {
            Interlocked.Decrement(ref pendingCount);
        }

        private void ScheduleDestination(DestinationState destination)
        {
            var schedule = false;
            lock (destination.SyncRoot)
            {
                if (continueProcessing && !destination.Scheduled && !destination.Processing &&
                    destination.Messages.Count > 0 && destination.RetryAfterUtc <= DateTime.UtcNow)
                {
                    destination.Scheduled = true;
                    schedule = true;
                }
            }
            if (!schedule) return;
            readyDestinations.Enqueue(destination);
            readySignal.Release();
        }

        private void ScheduleOrRemoveDestination(DestinationState destination)
        {
            var remove = false;
            lock (destination.SyncRoot)
                remove = destination.Messages.Count == 0 && !destination.Processing && !destination.Scheduled;

            if (remove)
            {
                lock (destinations)
                {
                    lock (destination.SyncRoot)
                    {
                        DestinationState current;
                        if (destination.Messages.Count == 0 && !destination.Processing && !destination.Scheduled &&
                            destinations.TryGetValue(destination.Key, out current) && object.ReferenceEquals(current, destination))
                            destinations.Remove(destination.Key);
                    }
                }
            }
            else ScheduleDestination(destination);
        }

        private void ScheduleDueDestinations(object ignored)
        {
            if (!continueProcessing) return;
            DestinationState[] snapshot;
            lock (destinations) snapshot = destinations.Values.ToArray();
            foreach (var destination in snapshot) ScheduleDestination(destination);
        }

        private DateTime GetNextAttemptUtc(OutboundMessage message)
        {
            var factor = Math.Pow(Math.Max(1.0, deliveryOptions.RetryBackoffFactor), Math.Max(0, message.SendAttempts - 1));
            var delayMs = Math.Min(deliveryOptions.MaximumRetryDelay.TotalMilliseconds,
                deliveryOptions.InitialRetryDelay.TotalMilliseconds * factor);
            return message.LastSendAttempt.ToUniversalTime() + TimeSpan.FromMilliseconds(delayMs);
        }

        private bool ShouldDeadLetter(OutboundMessage message)
        {
            return message.SendAttempts >= deliveryOptions.MaxAttempts ||
                   DateTime.UtcNow - message.Sent.ToUniversalTime() >= deliveryOptions.MaxAge;
        }

        private void SendMessage(OutboundMessage message)
        {
            NpClient<IMessageService> npClient = null;
            TcpClient<IMessageService> tcpClient = null;
            var poolKey = message.To.ToString();
            try
            {
                IMessageService proxy;
                var useNamedPipe = message.To.Transport == Transport.Np ||
                    message.To.Transport == Transport.Both && message.To.ServerName == message.From.ServerName;
                if (useNamedPipe)
                {
                    npClient = npClientPool.Request(poolKey, () => new NpClient<IMessageService>(
                        new NpEndPoint(message.To.PipeName, connectTimeOutMs)));
                    proxy = npClient.Proxy;
                }
                else
                {
                    tcpClient = tcpClientPool.Request(poolKey, () => new TcpClient<IMessageService>(new TcpEndPoint(
                        new IPEndPoint(IPAddress.Parse(message.To.IpAddress), message.To.Port), connectTimeOutMs)));
                    proxy = tcpClient.Proxy;
                }
                if (message.MessageBytes == null)
                    proxy.EnqueueString(message.Id, message.From.ToString(), message.Sent, message.SendAttempts,
                        message.MessageTypeName, message.MessageString);
                else
                    proxy.EnqueueBytes(message.Id, message.From.ToString(), message.Sent, message.SendAttempts,
                        message.MessageTypeName, message.MessageBytes);
            }
            catch
            {
                if (tcpClient != null) { tcpClient.Dispose(); tcpClient = null; }
                if (npClient != null) { npClient.Dispose(); npClient = null; }
                throw;
            }
            finally
            {
                if (npClient != null) npClientPool.Release(poolKey, npClient);
                if (tcpClient != null) tcpClientPool.Release(poolKey, tcpClient);
            }
        }

        private void LogSent(OutboundMessage message)
        {
            var audit = CreateAudit(message, storageOptions.SentAuditPayload);
            if (audit != null) store.Append(StorageArea.Sent, AuditKey("sent"), audit, storageOptions.Durability);
            store.Delete(StorageArea.Outgoing, message.Filename);
        }

        private void DeadLetterMessage(OutboundMessage message, Exception exception)
        {
            var key = Path.ChangeExtension(message.Filename, ".dlq");
            var reason = exception == null ? "Delivery policy exhausted." : exception.GetType().FullName + ": " + exception.Message;
            var value = "dlq1\t" + Encode(reason) + "\t" + Encode(message.ToString());
            store.Write(StorageArea.DeadLetter, key, value, storageOptions.Durability);
            store.Delete(StorageArea.Outgoing, message.Filename);
        }

        private static OutboundMessage ParseDeadLetter(string key, string value, out string reason)
        {
            var parts = value.Split('\t');
            if (parts.Length != 3 || parts[0] != "dlq1") throw new FormatException("Invalid ServiceMq dead-letter record.");
            reason = Decode(parts[1]);
            return OutboundMessage.Deserialize(key, Decode(parts[2]));
        }

        private static string AuditKey(string prefix)
        {
            return prefix + "-" + DateTime.UtcNow.ToString("yyyyMMdd-HH-mm", CultureInfo.InvariantCulture) + ".log";
        }

        private static string CreateAudit(OutboundMessage message, AuditPayloadMode mode)
        {
            if (mode == AuditPayloadMode.None) return null;
            if (mode == AuditPayloadMode.Full) return message.ToString();
            return string.Join("\t", "meta", message.Id, message.From, message.To,
                message.Sent.ToString("o", CultureInfo.InvariantCulture), message.SendAttempts, message.MessageTypeName,
                message.MessageBytes == null ? (message.MessageString ?? string.Empty).Length : message.MessageBytes.Length);
        }

        private static string Encode(string value) { return Convert.ToBase64String(Encoding.UTF8.GetBytes(value ?? string.Empty)); }
        private static string Decode(string value) { return Encoding.UTF8.GetString(Convert.FromBase64String(value)); }

        private static bool RequiresExistenceValidation(StorageOptions options)
        {
            return options.FullBehavior == QueueFullBehavior.DropOldest &&
                (options.MaxBytes.HasValue || options.MaxMessages.HasValue);
        }
    }
}
