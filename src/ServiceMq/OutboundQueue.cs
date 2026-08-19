using System;
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
        private readonly CachingQueue<OutboundMessage> queue;
        private readonly Dictionary<string, CachingQueue<OutboundMessage>> retryQueues =
            new Dictionary<string, CachingQueue<OutboundMessage>>();
        private readonly IMessageStore store;
        private readonly StorageOptions storageOptions;
        private readonly DeliveryOptions deliveryOptions;
        private readonly int connectTimeOutMs;
        private readonly int maxMessagesInMemory;
        private readonly int reorderLevel;
        private readonly ManualResetEvent outgoingSignal = new ManualResetEvent(false);
        private readonly Timer timer;
        private readonly Task sendTask;
        private readonly PooledDictionary<string, NpClient<IMessageService>> npClientPool =
            new PooledDictionary<string, NpClient<IMessageService>>();
        private readonly PooledDictionary<string, TcpClient<IMessageService>> tcpClientPool =
            new PooledDictionary<string, TcpClient<IMessageService>>();
        private volatile bool continueProcessing = true;
        private long sequence;
        private Exception stateException;
        private QueueState state = QueueState.Running;

        public OutboundQueue(IMessageStore store, StorageOptions storageOptions, DeliveryOptions deliveryOptions,
            int connectTimeOutMs, int maxMessagesInMemory, int reorderLevel)
        {
            this.store = store;
            this.storageOptions = storageOptions;
            this.deliveryOptions = deliveryOptions;
            this.connectTimeOutMs = connectTimeOutMs;
            this.maxMessagesInMemory = maxMessagesInMemory;
            this.reorderLevel = reorderLevel;
            queue = new CachingQueue<OutboundMessage>(store, StorageArea.Outgoing, ".omq",
                OutboundMessage.Deserialize, x => x.ToString(), maxMessagesInMemory, reorderLevel, storageOptions.Durability);
            sendTask = Task.Factory.StartNew(SendMessages, CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default);
            if (queue.Count > 0) outgoingSignal.Set();
            timer = new Timer(x => outgoingSignal.Set(), null, 1000, 1000);
        }

        public long Count
        {
            get
            {
                lock (retryQueues)
                {
                    long count = queue.Count;
                    foreach (var retry in retryQueues.Values) count += retry.Count;
                    return count;
                }
            }
        }

        public Exception StateException { get { return stateException ?? queue.ReloadException ?? store.LastException; } }
        public QueueState State { get { return state == QueueState.Failed ? state : StateException == null ? state : QueueState.Cautioned; } }
        public void ClearState() { stateException = null; state = QueueState.Running; queue.ClearException(); store.ClearException(); }

        public void Stop()
        {
            continueProcessing = false;
            timer.Dispose();
            outgoingSignal.Set();
            if (!sendTask.Wait(5000)) stateException = new TimeoutException("The outbound queue did not stop within five seconds.");
            npClientPool.Dispose();
            tcpClientPool.Dispose();
            outgoingSignal.Dispose();
        }

        public void Enqueue(OutboundMessage message)
        {
            var key = NewKey(".omq");
            message.Filename = key;
            queue.Enqueue(key, message);
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
            message.Filename = NewKey(".omq");
            queue.Enqueue(message.Filename, message);
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

        private void SendMessages()
        {
            while (continueProcessing)
            {
                try
                {
                    if (!outgoingSignal.WaitOne(100)) continue;
                    if (!continueProcessing) break;
                    var message = queue.Dequeue();
                    var fromRegularQueue = true;
                    if (message == null)
                    {
                        message = GetRetryCandidate();
                        fromRegularQueue = false;
                    }
                    if (message == null)
                    {
                        outgoingSignal.Reset();
                        continue;
                    }

                    var destination = message.To.ToFileNameString();
                    CachingQueue<OutboundMessage> retryQueue;
                    lock (retryQueues) retryQueues.TryGetValue(destination, out retryQueue);
                    if (fromRegularQueue && retryQueue != null && retryQueue.Count > 0)
                    {
                        retryQueue.ReEnqueue(message.Filename, message);
                        continue;
                    }

                    message.LastSendAttempt = DateTime.UtcNow;
                    message.SendAttempts++;
                    store.Write(StorageArea.Outgoing, message.Filename, message.ToString(), storageOptions.Durability);
                    try
                    {
                        SendMessage(message);
                        LogSent(message);
                        if (!fromRegularQueue) retryQueue.Dequeue();
                    }
                    catch (Exception ex)
                    {
                        if (ShouldDeadLetter(message))
                        {
                            DeadLetterMessage(message, ex);
                            if (!fromRegularQueue) retryQueue.Dequeue();
                        }
                        else if (fromRegularQueue)
                        {
                            lock (retryQueues)
                            {
                                if (!retryQueues.TryGetValue(destination, out retryQueue))
                                {
                                    retryQueue = new CachingQueue<OutboundMessage>(store, StorageArea.Outgoing, ".omq",
                                        OutboundMessage.Deserialize, x => x.ToString(), maxMessagesInMemory, reorderLevel,
                                        storageOptions.Durability, false);
                                    retryQueues.Add(destination, retryQueue);
                                }
                                retryQueue.ReEnqueue(message.Filename, message);
                            }
                        }
                    }
                }
                catch (Exception ex) { stateException = ex; state = QueueState.Cautioned; }
            }
        }

        private OutboundMessage GetRetryCandidate()
        {
            lock (retryQueues)
            {
                OutboundMessage selected = null;
                foreach (var retry in retryQueues.Values)
                {
                    var candidate = retry.Peek();
                    if (candidate == null || !RetryDelayElapsed(candidate)) continue;
                    if (selected == null || candidate.LastSendAttempt < selected.LastSendAttempt) selected = candidate;
                }
                return selected;
            }
        }

        private bool RetryDelayElapsed(OutboundMessage message)
        {
            var factor = Math.Pow(Math.Max(1.0, deliveryOptions.RetryBackoffFactor), Math.Max(0, message.SendAttempts - 1));
            var delayMs = Math.Min(deliveryOptions.MaximumRetryDelay.TotalMilliseconds,
                deliveryOptions.InitialRetryDelay.TotalMilliseconds * factor);
            return DateTime.UtcNow - message.LastSendAttempt.ToUniversalTime() >= TimeSpan.FromMilliseconds(delayMs);
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

        private string NewKey(string suffix)
        {
            return DateTime.UtcNow.ToString("yyyyMMddHHmmssfffffff", CultureInfo.InvariantCulture) + "-" +
                Interlocked.Increment(ref sequence).ToString("D10", CultureInfo.InvariantCulture) + "-" +
                Guid.NewGuid().ToString("N") + suffix;
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
    }
}
