using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    public sealed class MemoryMessageStore : IMessageStore, IAsyncMessageStore
    {
        private readonly object syncRoot = new object();
        private readonly Dictionary<StorageArea, SortedDictionary<string, StorageEntry>> areas =
            new Dictionary<StorageArea, SortedDictionary<string, StorageEntry>>();
        private readonly Dictionary<StorageArea, Dictionary<string, List<string>>> appendedValues =
            new Dictionary<StorageArea, Dictionary<string, List<string>>>();

        public Exception LastException { get; private set; }

        public MemoryMessageStore()
        {
            foreach (StorageArea area in Enum.GetValues(typeof(StorageArea)))
            {
                areas[area] = new SortedDictionary<string, StorageEntry>(StringComparer.Ordinal);
                appendedValues[area] = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            }
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
                List<string> appended;
                appendedValues[area].TryGetValue(key, out appended);
                return Clone(entry, appended);
            }
        }

        public void Write(StorageArea area, string key, string value, DurabilityMode durability)
        {
            lock (syncRoot)
            {
                StorageEntry previous;
                var now = DateTime.UtcNow;
                areas[area].TryGetValue(key, out previous);
                appendedValues[area].Remove(key);
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
                var now = DateTime.UtcNow;
                if (!areas[area].TryGetValue(key, out current))
                {
                    areas[area][key] = new StorageEntry
                    {
                        Key = key,
                        Value = value,
                        Length = Encoding.UTF8.GetByteCount(value ?? string.Empty),
                        CreatedUtc = now,
                        LastModifiedUtc = now
                    };
                    return;
                }

                List<string> appended;
                if (!appendedValues[area].TryGetValue(key, out appended))
                {
                    appended = new List<string>();
                    appendedValues[area][key] = appended;
                }
                var segment = Environment.NewLine + (value ?? string.Empty);
                appended.Add(segment);
                current.Length += Encoding.UTF8.GetByteCount(segment);
                current.LastModifiedUtc = now;
            }
        }

        public void Delete(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                areas[area].Remove(key);
                appendedValues[area].Remove(key);
            }
        }

        public void Move(StorageArea source, StorageArea destination, string key)
        {
            lock (syncRoot)
            {
                StorageEntry entry;
                if (!areas[source].TryGetValue(key, out entry)) return;
                areas[source].Remove(key);
                List<string> appended;
                appendedValues[source].TryGetValue(key, out appended);
                appendedValues[source].Remove(key);
                appendedValues[destination].Remove(key);
                entry.LastModifiedUtc = DateTime.UtcNow;
                areas[destination][key] = entry;
                if (appended != null) appendedValues[destination][key] = appended;
            }
        }

        public void Purge(StorageArea area, DateTime olderThanUtc)
        {
            lock (syncRoot)
            {
                foreach (var key in areas[area].Where(x => x.Value.LastModifiedUtc < olderThanUtc)
                    .Select(x => x.Key).ToArray())
                {
                    areas[area].Remove(key);
                    appendedValues[area].Remove(key);
                }
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

        // IAsyncMessageStore — all operations are in-memory, so the async surface completes the
        // work synchronously under the same lock and returns a completed task.
        public Task<IReadOnlyList<string>> GetKeysAsync(StorageArea area, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult<IReadOnlyList<string>>(GetKeys(area));
        }

        public Task<bool> ContainsAsync(StorageArea area, string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Contains(area, key));
        }

        public Task<StorageEntry> ReadAsync(StorageArea area, string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Read(area, key));
        }

        public Task WriteAsync(StorageArea area, string key, string value, DurabilityMode durability, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Write(area, key, value, durability);
            return Task.CompletedTask;
        }

        public Task AppendAsync(StorageArea area, string key, string value, DurabilityMode durability, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Append(area, key, value, durability);
            return Task.CompletedTask;
        }

        public Task DeleteAsync(StorageArea area, string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Delete(area, key);
            return Task.CompletedTask;
        }

        public Task MoveAsync(StorageArea source, StorageArea destination, string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Move(source, destination, key);
            return Task.CompletedTask;
        }

        public Task PurgeAsync(StorageArea area, DateTime olderThanUtc, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Purge(area, olderThanUtc);
            return Task.CompletedTask;
        }

        public Task<StorageAreaStatistics> GetStatisticsAsync(StorageArea area, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetStatistics(area));
        }

        public Task ClearExceptionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ClearException();
            return Task.CompletedTask;
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        private static StorageEntry Clone(StorageEntry entry, IList<string> appended)
        {
            return new StorageEntry
            {
                Key = entry.Key,
                Value = appended == null || appended.Count == 0
                    ? entry.Value
                    : entry.Value + string.Concat(appended),
                Length = entry.Length,
                CreatedUtc = entry.CreatedUtc,
                LastModifiedUtc = entry.LastModifiedUtc
            };
        }
    }
}
