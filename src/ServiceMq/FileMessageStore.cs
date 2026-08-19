using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    public sealed class FileMessageStore : IMessageStore
    {
        private readonly string rootPath;
        private readonly FileStream lockFile;
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
                try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
                throw;
            }
        }

        public void Append(StorageArea area, string key, string value, DurabilityMode durability)
        {
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
