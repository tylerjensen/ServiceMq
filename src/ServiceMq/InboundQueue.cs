using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    internal sealed class InboundQueue
    {
        private sealed class Lease
        {
            public Message Message;
            public DateTime ExpiresUtc;
            public bool Completing;
        }

        private readonly CachingQueue<Message> queue;
        private readonly IMessageStore store;
        private readonly StorageOptions options;
        private readonly TimeSpan? visibilityTimeout;
        private readonly object enqueueLock = new object();
        private readonly MonotonicQueueKeyGenerator keyGenerator;
        private readonly object leaseLock = new object();
        private readonly Dictionary<Guid, Lease> leases = new Dictionary<Guid, Lease>();
        private readonly ManualResetEvent incomingSignal = new ManualResetEvent(false);
        private readonly Timer leaseTimer;
        private volatile bool continueProcessing = true;
        private Exception stateException;
        private QueueState state = QueueState.Running;

        public InboundQueue(IMessageStore store, StorageOptions options, TimeSpan? visibilityTimeout,
            int maxMessagesInMemory, int reorderLevel)
        {
            this.store = store;
            this.options = options;
            this.visibilityTimeout = visibilityTimeout;
            var existingKeys = store.GetKeys(StorageArea.Incoming);
            keyGenerator = new MonotonicQueueKeyGenerator(existingKeys);
            queue = new CachingQueue<Message>(store, StorageArea.Incoming, ".imq", Message.Deserialize,
                x => x.ToString(), maxMessagesInMemory, reorderLevel, options.Durability, true,
                RequiresExistenceValidation(options), existingKeys);
            if (queue.Count > 0) incomingSignal.Set();
            if (visibilityTimeout.HasValue)
                leaseTimer = new Timer(RequeueExpiredLeases, null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        }

        public int Count { get { return queue.Count; } }
        public Exception StateException { get { return stateException ?? queue.ReloadException ?? store.LastException; } }
        public QueueState State
        {
            get
            {
                if (state == QueueState.Failed) return state;
                if (StateException == null) return state;
                return QueueState.Cautioned;
            }
        }

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
                lock (enqueueLock)
                {
                    var key = keyGenerator.Next(".imq");
                    message.Filename = key;
                    queue.Enqueue(key, message);
                }
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

        public Task ReEnqueueAsync(Message message, CancellationToken cancellationToken = default)
        {
            if (message == null) throw new ArgumentNullException("message");
            cancellationToken.ThrowIfCancellationRequested();
            lock (leaseLock) leases.Remove(message.Id);
            queue.ReEnqueue(message.Filename, message);
            incomingSignal.Set();
            return Task.CompletedTask;
        }

        public Message Receive(int timeoutMs, bool logRead = true)
        {
            while (continueProcessing)
            {
                if (!incomingSignal.WaitOne(timeoutMs)) break;
                // continueProcessing is volatile and written by Stop() from another thread,
                // so this check is not constant; it exits promptly on stop.
                if (!continueProcessing) break; // NOSONAR(S2589)
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

        public async Task<Message> ReceiveAsync(int timeoutMs, bool logRead = true, CancellationToken cancellationToken = default)
        {
            var deadline = timeoutMs < 0 ? (DateTime?)null : DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (continueProcessing)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var message = await queue.DequeueAsync(cancellationToken).ConfigureAwait(false);
                if (message != null)
                {
                    // Dequeue is the cancellation boundary: completion must return the
                    // message even if cancellation is requested while storage is updated.
                    if (logRead) await CompleteAsync(message, CancellationToken.None).ConfigureAwait(false);
                    else RegisterLease(message);
                    return message;
                }
                if (queue.Count > 0) continue;
                if (deadline.HasValue && DateTime.UtcNow >= deadline.Value) break;
                var waitMs = deadline.HasValue
                    ? (int)Math.Max(1, Math.Min(50, (deadline.Value - DateTime.UtcNow).TotalMilliseconds))
                    : 50;
                await Task.Delay(waitMs, cancellationToken).ConfigureAwait(false);
            }
            return null;
        }

        public IList<Message> ReceiveBulk(int maxMessagesToReceive, int timeoutMs, bool logRead = true)
        {
            if (maxMessagesToReceive < 1) maxMessagesToReceive = 1;
            while (continueProcessing)
            {
                if (!incomingSignal.WaitOne(timeoutMs)) break;
                // continueProcessing is volatile and written by Stop() from another thread,
                // so this check is not constant; it exits promptly on stop.
                if (!continueProcessing) break; // NOSONAR(S2589)
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

        public async Task<IList<Message>> ReceiveBulkAsync(int maxMessagesToReceive, int timeoutMs, bool logRead = true, CancellationToken cancellationToken = default)
        {
            if (maxMessagesToReceive < 1) maxMessagesToReceive = 1;
            var deadline = timeoutMs < 0 ? (DateTime?)null : DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (continueProcessing)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var messages = await queue.DequeueBulkAsync(maxMessagesToReceive, cancellationToken).ConfigureAwait(false);
                if (messages.Count > 0)
                {
                    await CompleteMessagesAsync(messages, logRead).ConfigureAwait(false);
                    return messages;
                }
                if (deadline.HasValue && DateTime.UtcNow >= deadline.Value) break;
                await Task.Delay(ComputeWaitMs(deadline), cancellationToken).ConfigureAwait(false);
            }
            return new List<Message>();
        }

        private async Task CompleteMessagesAsync(IList<Message> messages, bool logRead)
        {
            foreach (var message in messages)
            {
                // A batch cannot be canceled after some of its records have been deleted.
                if (logRead) await CompleteAsync(message, CancellationToken.None).ConfigureAwait(false);
                else RegisterLease(message);
            }
        }

        private static int ComputeWaitMs(DateTime? deadline)
        {
            if (!deadline.HasValue) return 50;
            return (int)Math.Max(1, Math.Min(50, (deadline.Value - DateTime.UtcNow).TotalMilliseconds));
        }

        public void Acknowledge(Message message)
        {
            if (message == null) throw new ArgumentNullException("message");
            lock (leaseLock) leases.Remove(message.Id);
            Complete(message);
        }

        public async Task AcknowledgeAsync(Message message, CancellationToken cancellationToken = default)
        {
            if (message == null) throw new ArgumentNullException("message");
            cancellationToken.ThrowIfCancellationRequested();
            Lease lease;
            lock (leaseLock)
            {
                leases.TryGetValue(message.Id, out lease);
                if (lease != null) lease.Completing = true;
            }
            try
            {
                // Once completion starts, finish the acknowledgement even if the caller
                // cancels. Keep the lease recoverable if storage completion fails.
                await CompleteAsync(message, CancellationToken.None).ConfigureAwait(false);
                lock (leaseLock)
                {
                    if (leases.TryGetValue(message.Id, out var current) && ReferenceEquals(current, lease))
                        leases.Remove(message.Id);
                }
            }
            finally
            {
                lock (leaseLock)
                {
                    if (lease != null) lease.Completing = false;
                }
            }
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

        private async Task CompleteAsync(Message message, CancellationToken cancellationToken)
        {
            try
            {
                var audit = CreateAudit(message, options.ReadAuditPayload);
                var asyncStore = store as IAsyncMessageStore;
                if (audit != null)
                {
                    if (asyncStore != null) await asyncStore.AppendAsync(StorageArea.Read, AuditKey("read"), audit, options.Durability, cancellationToken).ConfigureAwait(false);
                    else await Task.Run(() => store.Append(StorageArea.Read, AuditKey("read"), audit, options.Durability), cancellationToken).ConfigureAwait(false);
                }
                if (asyncStore != null) await asyncStore.DeleteAsync(StorageArea.Incoming, message.Filename, cancellationToken).ConfigureAwait(false);
                else await Task.Run(() => store.Delete(StorageArea.Incoming, message.Filename), cancellationToken).ConfigureAwait(false);
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
                    expired = leases.Values.Where(x => !x.Completing && x.ExpiresUtc <= DateTime.UtcNow).ToArray();
                    foreach (var lease in expired) leases.Remove(lease.Message.Id);
                }
                foreach (var lease in expired) queue.ReEnqueue(lease.Message.Filename, lease.Message);
                if (expired.Length > 0) incomingSignal.Set();
            }
            catch (Exception ex) { stateException = ex; state = QueueState.Cautioned; }
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

        private static bool RequiresExistenceValidation(StorageOptions options)
        {
            return options.FullBehavior == QueueFullBehavior.DropOldest &&
                (options.MaxBytes.HasValue || options.MaxMessages.HasValue);
        }
    }
}
