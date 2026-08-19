using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    internal sealed class CachingQueue<T>
    {
        private sealed class QueueEntry { public string Key; public T Value; }

        private readonly int reorderQty;
        private readonly object syncRoot = new object();
        private readonly int maxMessagesInMemory;
        private readonly int reorderLevel;
        private readonly bool persistMessages;
        private readonly bool validateExistence;
        private readonly Queue<string> keysQueue;
        private readonly Queue<QueueEntry> messageQueue;
        private readonly Func<string, string, T> deserialize;
        private readonly Func<T, string> serialize;
        private readonly IMessageStore store;
        private readonly StorageArea area;
        private readonly DurabilityMode durability;
        private readonly string suffix;
        private readonly Action<string> recordDiscarded;
        private int reloading;
        private Exception reloadException;

        public Exception ReloadException { get { return reloadException; } }
        public void ClearException() { reloadException = null; }

        public CachingQueue(IMessageStore store, StorageArea area, string suffix,
            Func<string, string, T> deserialize, Func<T, string> serialize,
            int maxMessagesInMemory, int reorderLevel, DurabilityMode durability, bool persistMessages = true,
            bool validateExistence = true, IEnumerable<string> initialKeys = null,
            Action<string> recordDiscarded = null)
        {
            this.store = store;
            this.area = area;
            this.suffix = suffix;
            this.deserialize = deserialize;
            this.serialize = serialize;
            this.maxMessagesInMemory = Math.Max(1, maxMessagesInMemory);
            this.reorderLevel = Math.Max(1, Math.Min(reorderLevel, this.maxMessagesInMemory));
            reorderQty = Math.Max(1, this.reorderLevel > 2048 ? 512 : this.reorderLevel / 4);
            this.durability = durability;
            this.persistMessages = persistMessages;
            this.validateExistence = validateExistence;
            this.recordDiscarded = recordDiscarded;
            keysQueue = new Queue<string>(this.maxMessagesInMemory);
            messageQueue = new Queue<QueueEntry>(this.maxMessagesInMemory);
            if (persistMessages) Initialize(initialKeys);
        }

        private void Initialize(IEnumerable<string> initialKeys)
        {
            lock (syncRoot)
            {
                foreach (var key in (initialKeys ?? store.GetKeys(area))
                    .Where(x => x.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(x => x, StringComparer.Ordinal))
                {
                    if (messageQueue.Count < maxMessagesInMemory) LoadIntoMemory(key);
                    else keysQueue.Enqueue(key);
                }
            }
        }

        public T Dequeue()
        {
            return Dequeue(validateExistence);
        }

        public T DequeueWithoutValidation()
        {
            return Dequeue(false);
        }

        private T Dequeue(bool checkExistence)
        {
            lock (syncRoot)
            {
                EnsureMessageAvailable();
                while (messageQueue.Count > 0)
                {
                    var entry = messageQueue.Dequeue();
                    RefillCheck();
                    if (!checkExistence || store.Contains(area, entry.Key)) return entry.Value;
                    if (recordDiscarded != null) recordDiscarded(entry.Key);
                    EnsureMessageAvailable();
                }
                return default(T);
            }
        }

        public IList<T> DequeueBulk(int maxMessagesToReceive)
        {
            var result = new List<T>(maxMessagesToReceive);
            lock (syncRoot)
            {
                EnsureMessageAvailable();
                while (result.Count < maxMessagesToReceive && messageQueue.Count > 0)
                {
                    var entry = messageQueue.Dequeue();
                    if (!validateExistence || store.Contains(area, entry.Key)) result.Add(entry.Value);
                    else if (recordDiscarded != null) recordDiscarded(entry.Key);
                    if (messageQueue.Count == 0) EnsureMessageAvailable();
                }
                RefillCheck();
            }
            return result;
        }

        public T Peek()
        {
            lock (syncRoot)
            {
                EnsureMessageAvailable();
                while (messageQueue.Count > 0)
                {
                    var entry = messageQueue.Peek();
                    if (!validateExistence || store.Contains(area, entry.Key)) return entry.Value;
                    messageQueue.Dequeue();
                    if (recordDiscarded != null) recordDiscarded(entry.Key);
                    EnsureMessageAvailable();
                }
                return default(T);
            }
        }

        public void Enqueue(string key, T message)
        {
            if (persistMessages) store.Write(area, key, serialize(message), durability);
            lock (syncRoot)
            {
                if (messageQueue.Count < maxMessagesInMemory && keysQueue.Count == 0)
                    messageQueue.Enqueue(new QueueEntry { Key = key, Value = message });
                else keysQueue.Enqueue(key);
            }
        }

        public void ReEnqueue(string key, T message)
        {
            lock (syncRoot)
            {
                if (messageQueue.Count < maxMessagesInMemory && keysQueue.Count == 0)
                    messageQueue.Enqueue(new QueueEntry { Key = key, Value = message });
                else keysQueue.Enqueue(key);
            }
        }

        public int Count { get { lock (syncRoot) return messageQueue.Count + keysQueue.Count; } }

        private void RefillCheck()
        {
            if (maxMessagesInMemory > 1 && keysQueue.Count > 0 && messageQueue.Count < reorderLevel)
                RefillMessageQueueAsync();
        }

        private void RefillMessageQueueAsync()
        {
            if (Interlocked.CompareExchange(ref reloading, 1, 0) != 0) return;
            Task.Factory.StartNew(() =>
            {
                try
                {
                    while (true)
                    {
                        lock (syncRoot)
                        {
                            var loadCount = 0;
                            while (keysQueue.Count > 0 && loadCount < reorderQty && messageQueue.Count < maxMessagesInMemory)
                            {
                                LoadIntoMemory(keysQueue.Dequeue());
                                loadCount++;
                            }
                            if (keysQueue.Count == 0 || messageQueue.Count >= maxMessagesInMemory) break;
                        }
                    }
                }
                catch (Exception ex) { reloadException = ex; }
                finally { Interlocked.Exchange(ref reloading, 0); }
            }, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        private void EnsureMessageAvailable()
        {
            // Do not report an empty queue merely because the asynchronous prefetch has
            // not run yet. Loading one record here keeps dequeue FIFO and prevents a
            // consumer from resetting its signal while durable records still exist.
            while (messageQueue.Count == 0 && keysQueue.Count > 0)
                LoadIntoMemory(keysQueue.Dequeue());
        }

        private void LoadIntoMemory(string key)
        {
            try
            {
                if (validateExistence && !store.Contains(area, key))
                {
                    if (recordDiscarded != null) recordDiscarded(key);
                    return;
                }
                var value = deserialize(key, store.Read(area, key).Value);
                if (object.Equals(value, default(T))) throw new FormatException("The stored message is empty or invalid.");
                messageQueue.Enqueue(new QueueEntry { Key = key, Value = value });
            }
            catch (Exception ex)
            {
                reloadException = ex;
                try { store.Move(area, StorageArea.Corrupt, key); } catch { }
                if (recordDiscarded != null) recordDiscarded(key);
            }
        }
    }
}
