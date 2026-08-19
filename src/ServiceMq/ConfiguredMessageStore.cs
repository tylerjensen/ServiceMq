using System;
using System.Collections.Generic;
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
        private readonly bool capacityEnabled;
        private long incomingCount;
        private long incomingBytes;
        private long outgoingCount;
        private long outgoingBytes;
        private readonly Dictionary<string, long> incomingLengths = new Dictionary<string, long>(StringComparer.Ordinal);
        private readonly Dictionary<string, long> outgoingLengths = new Dictionary<string, long>(StringComparer.Ordinal);

        public Exception LastException { get { return inner.LastException; } }

        public ConfiguredMessageStore(IMessageStore inner, StorageOptions options, bool disposeInner)
        {
            this.inner = inner ?? throw new ArgumentNullException("inner");
            this.options = options ?? throw new ArgumentNullException("options");
            this.disposeInner = disposeInner;
            capacityEnabled = options.MaxBytes.HasValue || options.MaxMessages.HasValue;
            if (capacityEnabled)
            {
                var incoming = inner.GetStatistics(StorageArea.Incoming);
                var outgoing = inner.GetStatistics(StorageArea.Outgoing);
                incomingCount = incoming.Count;
                incomingBytes = incoming.Bytes;
                outgoingCount = outgoing.Count;
                outgoingBytes = outgoing.Bytes;
            }
        }

        public System.Collections.Generic.IReadOnlyList<string> GetKeys(StorageArea area)
        {
            return inner.GetKeys(area);
        }

        public bool Contains(StorageArea area, string key) { return inner.Contains(area, key); }

        public StorageEntry Read(StorageArea area, string key)
        {
            StorageEntry entry;
            if (capacityEnabled && IsActiveArea(area))
            {
                lock (capacityLock)
                {
                    entry = inner.Read(area, key);
                    Lengths(area)[key] = entry.Length;
                }
            }
            else entry = inner.Read(area, key);
            if (options.Protector != null && IsProtectedArea(area)) entry.Value = options.Protector.Unprotect(entry.Value);
            return entry;
        }

        public void Write(StorageArea area, string key, string value, DurabilityMode durability)
        {
            var storedValue = options.Protector != null && IsProtectedArea(area)
                ? options.Protector.Protect(value)
                : value;
            if (!capacityEnabled || !IsActiveArea(area))
            {
                inner.Write(area, key, storedValue, durability);
                return;
            }
            WriteWithCapacity(area, key, storedValue, durability);
        }

        public void Append(StorageArea area, string key, string value, DurabilityMode durability)
        {
            var storedValue = options.Protector == null ? value : options.Protector.Protect(value);
            inner.Append(area, key, storedValue, durability);
        }

        public void Delete(StorageArea area, string key)
        {
            if (!capacityEnabled || !IsActiveArea(area))
            {
                inner.Delete(area, key);
                return;
            }
            lock (capacityLock)
            {
                long length;
                var existed = TryGetLength(area, key, out length);
                inner.Delete(area, key);
                if (existed)
                {
                    AddToCounters(area, -1, -length);
                    Lengths(area).Remove(key);
                    Monitor.PulseAll(capacityLock);
                }
            }
        }

        public void Move(StorageArea source, StorageArea destination, string key)
        {
            if (!capacityEnabled || !IsActiveArea(source) && !IsActiveArea(destination))
            {
                inner.Move(source, destination, key);
                return;
            }
            if (source == destination) return;
            lock (capacityLock)
            {
                long sourceLength;
                long destinationLength;
                var sourceExists = TryGetLength(source, key, out sourceLength);
                var destinationExists = TryGetLength(destination, key, out destinationLength);
                inner.Move(source, destination, key);
                if (IsActiveArea(source) && sourceExists)
                {
                    AddToCounters(source, -1, -sourceLength);
                    Lengths(source).Remove(key);
                }
                if (IsActiveArea(destination))
                {
                    if (destinationExists)
                    {
                        AddToCounters(destination, -1, -destinationLength);
                        Lengths(destination).Remove(key);
                    }
                    if (sourceExists)
                    {
                        AddToCounters(destination, 1, sourceLength);
                        Lengths(destination)[key] = sourceLength;
                    }
                }
                Monitor.PulseAll(capacityLock);
            }
        }

        public void Purge(StorageArea area, DateTime olderThanUtc)
        {
            if (!capacityEnabled || !IsActiveArea(area))
            {
                inner.Purge(area, olderThanUtc);
                return;
            }
            lock (capacityLock)
            {
                inner.Purge(area, olderThanUtc);
                var statistics = inner.GetStatistics(area);
                SetCounters(area, statistics.Count, statistics.Bytes);
                Lengths(area).Clear();
                Monitor.PulseAll(capacityLock);
            }
        }

        public StorageAreaStatistics GetStatistics(StorageArea area)
        {
            return inner.GetStatistics(area);
        }

        public void Flush() { inner.Flush(); }
        public void ClearException() { inner.ClearException(); }
        public void Dispose() { if (disposeInner) inner.Dispose(); }

        private void WriteWithCapacity(StorageArea area, string key, string value, DurabilityMode durability)
        {
            var deadline = DateTime.UtcNow + options.FullWaitTimeout;
            lock (capacityLock)
            {
                while (true)
                {
                    long previousLength;
                    var exists = TryGetLength(area, key, out previousLength);
                    var addedMessages = exists ? 0 : 1;
                    var valueLength = Encoding.UTF8.GetByteCount(value ?? string.Empty);
                    var addedBytes = valueLength - previousLength;
                    var messagesFit = !options.MaxMessages.HasValue ||
                        incomingCount + outgoingCount + addedMessages <= options.MaxMessages.Value;
                    var bytesFit = !options.MaxBytes.HasValue ||
                        incomingBytes + outgoingBytes + addedBytes <= options.MaxBytes.Value;
                    if (messagesFit && bytesFit)
                    {
                        inner.Write(area, key, value, durability);
                        AddToCounters(area, addedMessages, addedBytes);
                        Lengths(area)[key] = valueLength;
                        return;
                    }

                    if (options.FullBehavior == QueueFullBehavior.DropOldest)
                    {
                        if (!DropOldest()) throw CapacityException();
                        continue;
                    }
                    if (options.FullBehavior == QueueFullBehavior.Reject) throw CapacityException();

                    var remaining = deadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero) throw CapacityException();
                    Monitor.Wait(capacityLock, remaining);
                }
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
            AddToCounters(selectedArea.Value, -1, -selected.Length);
            Lengths(selectedArea.Value).Remove(selected.Key);
            Monitor.PulseAll(capacityLock);
            return true;
        }

        private bool TryGetLength(StorageArea area, string key, out long length)
        {
            if (IsActiveArea(area) && Lengths(area).TryGetValue(key, out length)) return true;
            length = 0;
            if (!inner.Contains(area, key)) return false;
            length = inner.Read(area, key).Length;
            if (IsActiveArea(area)) Lengths(area)[key] = length;
            return true;
        }

        private Dictionary<string, long> Lengths(StorageArea area)
        {
            return area == StorageArea.Incoming ? incomingLengths : outgoingLengths;
        }

        private void AddToCounters(StorageArea area, long count, long bytes)
        {
            if (area == StorageArea.Incoming)
            {
                incomingCount += count;
                incomingBytes += bytes;
            }
            else if (area == StorageArea.Outgoing)
            {
                outgoingCount += count;
                outgoingBytes += bytes;
            }
        }

        private void SetCounters(StorageArea area, long count, long bytes)
        {
            if (area == StorageArea.Incoming)
            {
                incomingCount = count;
                incomingBytes = bytes;
            }
            else if (area == StorageArea.Outgoing)
            {
                outgoingCount = count;
                outgoingBytes = bytes;
            }
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
