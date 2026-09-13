using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    public sealed class FileMessageStore : IMessageStore, IAsyncMessageStore
    {
        private readonly string rootPath;
        private readonly FileStream lockFile;
        private readonly SemaphoreSlim appendLock = new SemaphoreSlim(1, 1);
        private Exception lastException;

        public Exception LastException { get { return lastException; } }
        public string RootPath { get { return rootPath; } }

        public FileMessageStore(string rootPath)
        {
            if (string.IsNullOrWhiteSpace(rootPath)) throw new ArgumentException("A storage root path is required.", "rootPath");
            this.rootPath = Path.GetFullPath(rootPath);
            Directory.CreateDirectory(this.rootPath);
            foreach (StorageArea area in Enum.GetValues(typeof(StorageArea))) Directory.CreateDirectory(GetAreaPath(area));
            var lockPath = Path.Combine(this.rootPath, ".servicemq.lock");
            lockFile = new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        }

        public IReadOnlyList<string> GetKeys(StorageArea area)
        {
            try
            {
                return Directory.GetFiles(GetAreaPath(area)).Select(Path.GetFileName)
                    .Where(x => !x.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
                    .OrderBy(x => x, StringComparer.Ordinal).ToArray();
            }
            catch (Exception ex) { lastException = ex; throw; }
        }

        public bool Contains(StorageArea area, string key)
        {
            return File.Exists(GetPath(area, key));
        }

        public StorageEntry Read(StorageArea area, string key)
        {
            try
            {
                var path = GetPath(area, key);
                var info = new FileInfo(path);
                return new StorageEntry
                {
                    Key = key,
                    Value = File.ReadAllText(path, Encoding.UTF8),
                    Length = info.Length,
                    CreatedUtc = info.CreationTimeUtc,
                    LastModifiedUtc = info.LastWriteTimeUtc
                };
            }
            catch (Exception ex) { lastException = ex; throw; }
        }

        public void Write(StorageArea area, string key, string value, DurabilityMode durability)
        {
            var path = GetPath(area, key);
            var tempPath = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
            try
            {
                WriteFile(tempPath, value, durability == DurabilityMode.FlushToDisk);
                if (File.Exists(path)) File.Replace(tempPath, path, null);
                else File.Move(tempPath, path);
            }
            catch (Exception ex)
            {
                lastException = ex;
                // Best-effort removal of the temp file so a failed insert never leaves a stray .tmp.
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* best-effort cleanup */ }
                throw;
            }
        }

        public void Append(StorageArea area, string key, string value, DurabilityMode durability)
        {
            try
            {
                appendLock.Wait();
                try
                {
                    var path = GetPath(area, key);
                    using (var stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read))
                    using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
                    {
                        writer.WriteLine(value);
                        writer.Flush();
                        if (durability == DurabilityMode.FlushToDisk) stream.Flush(true);
                    }
                }
                finally { appendLock.Release(); }
            }
            catch (Exception ex) { lastException = ex; throw; }
        }

        public void Delete(StorageArea area, string key)
        {
            try
            {
                var path = GetPath(area, key);
                if (File.Exists(path)) File.Delete(path);
            }
            catch (Exception ex) { lastException = ex; throw; }
        }

        public void Move(StorageArea source, StorageArea destination, string key)
        {
            try
            {
                var sourcePath = GetPath(source, key);
                if (!File.Exists(sourcePath)) return;
                var destinationPath = GetPath(destination, key);
                if (File.Exists(destinationPath)) File.Delete(destinationPath);
                File.Move(sourcePath, destinationPath);
            }
            catch (Exception ex) { lastException = ex; throw; }
        }

        public void Purge(StorageArea area, DateTime olderThanUtc)
        {
            foreach (var key in GetKeys(area))
            {
                var path = GetPath(area, key);
                if (File.GetLastWriteTimeUtc(path) < olderThanUtc) Delete(area, key);
            }
        }

        public StorageAreaStatistics GetStatistics(StorageArea area)
        {
            try
            {
                var files = Directory.GetFiles(GetAreaPath(area))
                    .Where(x => !x.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase))
                    .Select(x => new FileInfo(x)).ToArray();
                return new StorageAreaStatistics
                {
                    Count = files.LongLength,
                    Bytes = files.Sum(x => x.Length),
                    OldestUtc = files.Length == 0 ? (DateTime?)null : files.Min(x => x.CreationTimeUtc)
                };
            }
            catch (Exception ex) { lastException = ex; throw; }
        }

        public void Flush() { }
        public void ClearException() { lastException = null; }

        public void Dispose()
        {
            lockFile.Dispose();
            appendLock.Dispose();
        }

        // IAsyncMessageStore — directory listing, existence and move operations have no true
        // async .NET equivalent, so they are offloaded with Task.Run; file reads and writes use
        // the real async file APIs.
        public Task<IReadOnlyList<string>> GetKeysAsync(StorageArea area, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.Run(() => GetKeys(area), cancellationToken);
        }

        public Task<bool> ContainsAsync(StorageArea area, string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Contains(area, key));
        }

        public async Task<StorageEntry> ReadAsync(StorageArea area, string key, CancellationToken cancellationToken = default)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var path = GetPath(area, key);
                var info = new FileInfo(path);
                string value;
                using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var reader = new StreamReader(stream, Encoding.UTF8))
                {
#if NET
                    value = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
#else
                    value = await reader.ReadToEndAsync().ConfigureAwait(false);
#endif
                }
                return new StorageEntry
                {
                    Key = key,
                    Value = value,
                    Length = info.Length,
                    CreatedUtc = info.CreationTimeUtc,
                    LastModifiedUtc = info.LastWriteTimeUtc
                };
            }
            catch (Exception ex) { lastException = ex; throw; }
        }

        public async Task WriteAsync(StorageArea area, string key, string value, DurabilityMode durability, CancellationToken cancellationToken = default)
        {
            var path = GetPath(area, key);
            var tempPath = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
            try
            {
                await WriteFileAsync(tempPath, value, durability == DurabilityMode.FlushToDisk, cancellationToken).ConfigureAwait(false);
                if (File.Exists(path)) File.Replace(tempPath, path, null);
                else File.Move(tempPath, path);
            }
            catch (Exception ex)
            {
                lastException = ex;
                // Best-effort removal of the temp file so a failed insert never leaves a stray .tmp.
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { /* best-effort cleanup */ }
                throw;
            }
        }

        public async Task AppendAsync(StorageArea area, string key, string value, DurabilityMode durability, CancellationToken cancellationToken = default)
        {
            try
            {
                await appendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    var path = GetPath(area, key);
                    using (var stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read))
                    using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
                    {
#if NET
                        await writer.WriteLineAsync(value.AsMemory(), cancellationToken).ConfigureAwait(false);
#else
                        await writer.WriteLineAsync(value).ConfigureAwait(false);
#endif
#if NET
                        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
#else
                        await writer.FlushAsync().ConfigureAwait(false);
#endif
                        // FlushAsync only drains managed buffers. A durable append must
                        // also flush the OS buffers, even if cancellation arrives now.
                        if (durability == DurabilityMode.FlushToDisk)
                            await Task.Run(() => stream.Flush(true)).ConfigureAwait(false);
                    }
                }
                finally { appendLock.Release(); }
            }
            catch (Exception ex) { lastException = ex; throw; }
        }

        public Task DeleteAsync(StorageArea area, string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.Run(() => Delete(area, key), cancellationToken);
        }

        public Task MoveAsync(StorageArea source, StorageArea destination, string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.Run(() => Move(source, destination, key), cancellationToken);
        }

        public async Task PurgeAsync(StorageArea area, DateTime olderThanUtc, CancellationToken cancellationToken = default)
        {
            foreach (var key in await GetKeysAsync(area, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var path = GetPath(area, key);
                if (File.GetLastWriteTimeUtc(path) < olderThanUtc) await DeleteAsync(area, key, cancellationToken).ConfigureAwait(false);
            }
        }

        public Task<StorageAreaStatistics> GetStatisticsAsync(StorageArea area, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.Run(() => GetStatistics(area), cancellationToken);
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

        private async Task WriteFileAsync(string path, string value, bool flushToDisk, CancellationToken cancellationToken)
        {
            using (var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
            {
#if NET
                await writer.WriteAsync((value ?? string.Empty).AsMemory(), cancellationToken).ConfigureAwait(false);
#else
                await writer.WriteAsync(value ?? string.Empty).ConfigureAwait(false);
#endif
#if NET
                await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
#else
                await writer.FlushAsync().ConfigureAwait(false);
#endif
                // Complete the physical flush before publishing the atomic replacement.
                if (flushToDisk) await Task.Run(() => stream.Flush(true)).ConfigureAwait(false);
            }
        }

        private string GetAreaPath(StorageArea area)
        {
            switch (area)
            {
                case StorageArea.Incoming: return Path.Combine(rootPath, "in");
                case StorageArea.Outgoing: return Path.Combine(rootPath, "out");
                case StorageArea.Sent: return Path.Combine(rootPath, "sent");
                case StorageArea.Read: return Path.Combine(rootPath, "read");
                case StorageArea.DeadLetter: return Path.Combine(rootPath, "fail");
                case StorageArea.Corrupt: return Path.Combine(rootPath, "corrupt");
                default: throw new ArgumentOutOfRangeException("area");
            }
        }

        private string GetPath(StorageArea area, string key)
        {
            if (string.IsNullOrWhiteSpace(key) || Path.GetFileName(key) != key)
                throw new ArgumentException("Storage keys must be plain file names.", "key");
            return Path.Combine(GetAreaPath(area), key);
        }

        private static void WriteFile(string path, string value, bool flushToDisk)
        {
            using (var stream = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
            {
                writer.Write(value ?? string.Empty);
                writer.Flush();
                if (flushToDisk) stream.Flush(true);
            }
        }
    }
}
