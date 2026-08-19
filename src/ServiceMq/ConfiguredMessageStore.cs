using System;
using System.Linq;
using System.Text;
using System.Threading;

namespace ServiceMq
{
    internal sealed class ConfiguredMessageStore : IMessageStore
    {
        private readonly object capacityLock = new object();
        private readonly IMessageStore inner;
        private readonly StorageOptions options;
        private readonly bool disposeInner;

        public Exception LastException { get { return inner.LastException; } }

        public ConfiguredMessageStore(IMessageStore inner, StorageOptions options, bool disposeInner)
        {
            this.inner = inner ?? throw new ArgumentNullException("inner");
            this.options = options ?? throw new ArgumentNullException("options");
            this.disposeInner = disposeInner;
        }

        public System.Collections.Generic.IReadOnlyList<string> GetKeys(StorageArea area)
        {
            return inner.GetKeys(area);
        }

        public bool Contains(StorageArea area, string key) { return inner.Contains(area, key); }

        public StorageEntry Read(StorageArea area, string key)
        {
            var entry = inner.Read(area, key);
            if (options.Protector != null && IsProtectedArea(area)) entry.Value = options.Protector.Unprotect(entry.Value);
            return entry;
        }

        public void Write(StorageArea area, string key, string value, DurabilityMode durability)
        {
            var storedValue = options.Protector != null && IsProtectedArea(area)
                ? options.Protector.Protect(value)
                : value;
            if (IsActiveArea(area)) EnsureCapacity(area, key, storedValue);
            inner.Write(area, key, storedValue, durability);
        }

        public void Append(StorageArea area, string key, string value, DurabilityMode durability)
        {
            var storedValue = options.Protector == null ? value : options.Protector.Protect(value);
            inner.Append(area, key, storedValue, durability);
        }

        public void Delete(StorageArea area, string key)
        {
            inner.Delete(area, key);
        }

        public void Move(StorageArea source, StorageArea destination, string key)
        {
            inner.Move(source, destination, key);
        }

        public void Purge(StorageArea area, DateTime olderThanUtc)
        {
            inner.Purge(area, olderThanUtc);
        }

        public StorageAreaStatistics GetStatistics(StorageArea area)
        {
            return inner.GetStatistics(area);
        }

        public void Flush() { inner.Flush(); }
        public void ClearException() { inner.ClearException(); }
        public void Dispose() { if (disposeInner) inner.Dispose(); }

        private void EnsureCapacity(StorageArea area, string key, string value)
        {
            if (!options.MaxBytes.HasValue && !options.MaxMessages.HasValue) return;
            var deadline = DateTime.UtcNow + options.FullWaitTimeout;
            while (true)
            {
                lock (capacityLock)
                {
                    var incoming = inner.GetStatistics(StorageArea.Incoming);
                    var outgoing = inner.GetStatistics(StorageArea.Outgoing);
                    var exists = inner.GetKeys(area).Contains(key);
                    var addedMessages = exists ? 0 : 1;
                    long addedBytes = Encoding.UTF8.GetByteCount(value ?? string.Empty);
                    if (exists) addedBytes -= inner.Read(area, key).Length;
                    var messagesFit = !options.MaxMessages.HasValue || incoming.Count + outgoing.Count + addedMessages <= options.MaxMessages.Value;
                    var bytesFit = !options.MaxBytes.HasValue || incoming.Bytes + outgoing.Bytes + addedBytes <= options.MaxBytes.Value;
                    if (messagesFit && bytesFit) return;

                    if (options.FullBehavior == QueueFullBehavior.DropOldest)
                    {
                        if (!DropOldest()) throw CapacityException();
                        continue;
                    }
                    if (options.FullBehavior == QueueFullBehavior.Reject) throw CapacityException();
                }
                if (DateTime.UtcNow >= deadline) throw CapacityException();
                Thread.Sleep(25);
            }
        }

        private bool DropOldest()
        {
            StorageArea? selectedArea = null;
            StorageEntry selected = null;
            foreach (var area in new[] { StorageArea.Incoming, StorageArea.Outgoing })
            {
                var key = inner.GetKeys(area).FirstOrDefault();
                if (key == null) continue;
                var entry = inner.Read(area, key);
                if (selected == null || entry.CreatedUtc < selected.CreatedUtc)
                {
                    selected = entry;
                    selectedArea = area;
                }
            }
            if (!selectedArea.HasValue) return false;
            inner.Delete(selectedArea.Value, selected.Key);
            return true;
        }

        private QueueCapacityExceededException CapacityException()
        {
            return new QueueCapacityExceededException("The ServiceMq storage capacity limit has been reached.");
        }

        private static bool IsActiveArea(StorageArea area)
        {
            return area == StorageArea.Incoming || area == StorageArea.Outgoing;
        }

        private static bool IsProtectedArea(StorageArea area)
        {
            return area == StorageArea.Incoming || area == StorageArea.Outgoing ||
                   area == StorageArea.DeadLetter || area == StorageArea.Corrupt;
        }
    }
}
