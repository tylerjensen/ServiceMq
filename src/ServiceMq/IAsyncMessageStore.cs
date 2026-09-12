using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    /// <summary>
    /// Asynchronous counterpart to <see cref="IMessageStore"/>. A provider may implement both
    /// interfaces; callers and the queue internals prefer the async surface when it is present
    /// and fall back to the synchronous surface otherwise. The async surface never blocks the
    /// caller's thread on storage I/O.
    /// </summary>
    public interface IAsyncMessageStore : IDisposable
    {
        Task<IReadOnlyList<string>> GetKeysAsync(StorageArea area, CancellationToken cancellationToken = default);
        Task<bool> ContainsAsync(StorageArea area, string key, CancellationToken cancellationToken = default);
        Task<StorageEntry> ReadAsync(StorageArea area, string key, CancellationToken cancellationToken = default);
        Task WriteAsync(StorageArea area, string key, string value, DurabilityMode durability, CancellationToken cancellationToken = default);
        Task AppendAsync(StorageArea area, string key, string value, DurabilityMode durability, CancellationToken cancellationToken = default);
        Task DeleteAsync(StorageArea area, string key, CancellationToken cancellationToken = default);
        Task MoveAsync(StorageArea source, StorageArea destination, string key, CancellationToken cancellationToken = default);
        Task PurgeAsync(StorageArea area, DateTime olderThanUtc, CancellationToken cancellationToken = default);
        Task<StorageAreaStatistics> GetStatisticsAsync(StorageArea area, CancellationToken cancellationToken = default);
        Task ClearExceptionAsync(CancellationToken cancellationToken = default);
        Task FlushAsync(CancellationToken cancellationToken = default);
    }
}
