using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;

namespace ServiceMq
{
    internal sealed class InboundQueue
    {
        private sealed class Lease
        {
            public Message Message;
            public DateTime ExpiresUtc;
        }

        private readonly CachingQueue<Message> queue;
        private readonly IMessageStore store;
        private readonly StorageOptions options;
        private readonly TimeSpan? visibilityTimeout;
        private readonly object leaseLock = new object();
        private readonly Dictionary<Guid, Lease> leases = new Dictionary<Guid, Lease>();
        private readonly ManualResetEvent incomingSignal = new ManualResetEvent(false);
        private readonly Timer leaseTimer;
        private volatile bool continueProcessing = true;
        private long sequence;
        private Exception stateException;
        private QueueState state = QueueState.Running;

        public InboundQueue(IMessageStore store, StorageOptions options, TimeSpan? visibilityTimeout,
            int maxMessagesInMemory, int reorderLevel)
        {
            this.store = store;
            this.options = options;
            this.visibilityTimeout = visibilityTimeout;
            queue = new CachingQueue<Message>(store, StorageArea.Incoming, ".imq", Message.Deserialize,
                x => x.ToString(), maxMessagesInMemory, reorderLevel, options.Durability);
            if (queue.Count > 0) incomingSignal.Set();
            if (visibilityTimeout.HasValue)
                leaseTimer = new Timer(RequeueExpiredLeases, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        public int Count { get { return queue.Count; } }
        public Exception StateException { get { return stateException ?? queue.ReloadException ?? store.LastException; } }
        public QueueState State { get { return state == QueueState.Failed ? state : StateException == null ? state : QueueState.Cautioned; } }

        public void ClearState()
        {
            stateException = null;
            state = QueueState.Running;
            queue.ClearException();
            store.ClearException();
        }

        public void Stop()
        {
            continueProcessing = false;
            incomingSignal.Set();
            if (leaseTimer != null) leaseTimer.Dispose();
            incomingSignal.Dispose();
        }

        public void Enqueue(Message message)
        {
            try
            {
                var key = NewKey(".imq");
                message.Filename = key;
                queue.Enqueue(key, message);
                if (continueProcessing) incomingSignal.Set();
            }
            catch (Exception ex)
            {
                stateException = ex;
                state = QueueState.Failed;
                throw;
            }
        }

        public void ReEnqueue(Message message)
        {
            lock (leaseLock) leases.Remove(message.Id);
            queue.ReEnqueue(message.Filename, message);
            incomingSignal.Set();
        }

        public Message Receive(int timeoutMs, bool logRead = true)
        {
            while (continueProcessing)
            {
                if (!incomingSignal.WaitOne(timeoutMs)) break;
                if (!continueProcessing) break;
                var message = queue.Dequeue();
                if (message == null)
                {
                    incomingSignal.Reset();
                    continue;
                }
                if (logRead) Complete(message);
                else RegisterLease(message);
                return message;
            }
            return null;
        }

        public IList<Message> ReceiveBulk(int maxMessagesToReceive, int timeoutMs, bool logRead = true)
        {
            if (maxMessagesToReceive < 1) maxMessagesToReceive = 1;
            while (continueProcessing)
            {
                if (!incomingSignal.WaitOne(timeoutMs)) break;
                if (!continueProcessing) break;
                var messages = queue.DequeueBulk(maxMessagesToReceive);
                if (messages.Count == 0)
                {
                    incomingSignal.Reset();
                    continue;
                }
                foreach (var message in messages)
                {
                    if (logRead) Complete(message);
                    else RegisterLease(message);
                }
                return messages;
            }
            return new List<Message>();
        }

        public void Acknowledge(Message message)
        {
            if (message == null) throw new ArgumentNullException("message");
            lock (leaseLock) leases.Remove(message.Id);
            Complete(message);
        }

        private void Complete(Message message)
        {
            try
            {
                var audit = CreateAudit(message, options.ReadAuditPayload);
                if (audit != null) store.Append(StorageArea.Read, AuditKey("read"), audit, options.Durability);
                store.Delete(StorageArea.Incoming, message.Filename);
            }
            catch (Exception ex)
            {
                stateException = ex;
                state = QueueState.Cautioned;
                throw;
            }
        }

        private void RegisterLease(Message message)
        {
            if (!visibilityTimeout.HasValue) return;
            lock (leaseLock)
                leases[message.Id] = new Lease { Message = message, ExpiresUtc = DateTime.UtcNow + visibilityTimeout.Value };
        }

        private void RequeueExpiredLeases(object ignored)
        {
            try
            {
                Lease[] expired;
                lock (leaseLock)
                {
                    expired = leases.Values.Where(x => x.ExpiresUtc <= DateTime.UtcNow).ToArray();
                    foreach (var lease in expired) leases.Remove(lease.Message.Id);
                }
                foreach (var lease in expired) queue.ReEnqueue(lease.Message.Filename, lease.Message);
                if (expired.Length > 0) incomingSignal.Set();
            }
            catch (Exception ex) { stateException = ex; state = QueueState.Cautioned; }
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

        private static string CreateAudit(Message message, AuditPayloadMode mode)
        {
            if (mode == AuditPayloadMode.None) return null;
            if (mode == AuditPayloadMode.Full) return message.ToString();
            return string.Join("\t", "meta", message.Id, message.From, message.Sent.ToString("o", CultureInfo.InvariantCulture),
                message.Received.ToString("o", CultureInfo.InvariantCulture), message.SendAttempt, message.MessageTypeName,
                message.MessageBytes == null ? (message.MessageString ?? string.Empty).Length : message.MessageBytes.Length);
        }
    }
}
