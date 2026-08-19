using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    public sealed class MemoryMessageStore : IMessageStore
    {
        private readonly object syncRoot = new object();
        private readonly Dictionary<StorageArea, SortedDictionary<string, StorageEntry>> areas =
            new Dictionary<StorageArea, SortedDictionary<string, StorageEntry>>();

        public Exception LastException { get; private set; }

        public MemoryMessageStore()
        {
            foreach (StorageArea area in Enum.GetValues(typeof(StorageArea)))
                areas[area] = new SortedDictionary<string, StorageEntry>(StringComparer.Ordinal);
        }

        public IReadOnlyList<string> GetKeys(StorageArea area)
        {
            lock (syncRoot) return areas[area].Keys.ToList();
        }

        public bool Contains(StorageArea area, string key)
        {
            lock (syncRoot) return areas[area].ContainsKey(key);
        }

        public StorageEntry Read(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                StorageEntry entry;
                if (!areas[area].TryGetValue(key, out entry)) throw new KeyNotFoundException(key);
                return Clone(entry);
            }
        }

        public void Write(StorageArea area, string key, string value, DurabilityMode durability)
        {
            lock (syncRoot)
            {
                StorageEntry previous;
                var now = DateTime.UtcNow;
                areas[area].TryGetValue(key, out previous);
                areas[area][key] = new StorageEntry
                {
                    Key = key,
                    Value = value,
                    Length = Encoding.UTF8.GetByteCount(value ?? string.Empty),
                    CreatedUtc = previous == null ? now : previous.CreatedUtc,
                    LastModifiedUtc = now
                };
            }
        }

        public void Append(StorageArea area, string key, string value, DurabilityMode durability)
        {
            lock (syncRoot)
            {
                StorageEntry current;
                var combined = areas[area].TryGetValue(key, out current)
                    ? current.Value + Environment.NewLine + value
                    : value;
                Write(area, key, combined, durability);
            }
        }

        public void Delete(StorageArea area, string key)
        {
            lock (syncRoot) areas[area].Remove(key);
        }

        public void Move(StorageArea source, StorageArea destination, string key)
        {
            lock (syncRoot)
            {
                StorageEntry entry;
                if (!areas[source].TryGetValue(key, out entry)) return;
                areas[source].Remove(key);
                entry.LastModifiedUtc = DateTime.UtcNow;
                areas[destination][key] = entry;
            }
        }

        public void Purge(StorageArea area, DateTime olderThanUtc)
        {
            lock (syncRoot)
            {
                foreach (var key in areas[area].Where(x => x.Value.LastModifiedUtc < olderThanUtc)
                    .Select(x => x.Key).ToArray()) areas[area].Remove(key);
            }
        }

        public StorageAreaStatistics GetStatistics(StorageArea area)
        {
            lock (syncRoot)
            {
                var entries = areas[area].Values;
                return new StorageAreaStatistics
                {
                    Count = entries.Count,
                    Bytes = entries.Sum(x => x.Length),
                    OldestUtc = entries.Count == 0 ? (DateTime?)null : entries.Min(x => x.CreatedUtc)
                };
            }
        }

        public void Flush() { }
        public void ClearException() { LastException = null; }
        public void Dispose() { }

        private static StorageEntry Clone(StorageEntry entry)
        {
            return new StorageEntry
            {
                Key = entry.Key,
                Value = entry.Value,
                Length = entry.Length,
                CreatedUtc = entry.CreatedUtc,
                LastModifiedUtc = entry.LastModifiedUtc
            };
        }
    }
}
