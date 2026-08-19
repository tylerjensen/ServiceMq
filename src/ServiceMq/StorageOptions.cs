using System;
using System.Collections.Generic;
using System.IO;
using ServiceWire;

namespace ServiceMq
{
    public enum DurabilityMode
    {
        FlushToDisk,
        Buffered,
        MemoryOnly
    }

    public enum QueueFullBehavior
    {
        Reject,
        Block,
        DropOldest
    }

    public enum AuditPayloadMode
    {
        Full,
        MetadataOnly,
        None
    }

    public enum StorageArea
    {
        Incoming,
        Outgoing,
        Sent,
        Read,
        DeadLetter,
        Corrupt
    }

    public sealed class StorageOptions
    {
        public string RootPath { get; set; }
        public DurabilityMode Durability { get; set; } = DurabilityMode.FlushToDisk;
        public long? MaxBytes { get; set; }
        public long? MaxMessages { get; set; }
        public QueueFullBehavior FullBehavior { get; set; } = QueueFullBehavior.Reject;
        public TimeSpan FullWaitTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan SentRetention { get; set; } = TimeSpan.FromHours(48);
        public TimeSpan ReadRetention { get; set; } = TimeSpan.FromHours(48);
        public TimeSpan DeadLetterRetention { get; set; } = TimeSpan.FromDays(30);
        public TimeSpan CleanupInterval { get; set; } = TimeSpan.FromMinutes(15);
        public AuditPayloadMode SentAuditPayload { get; set; } = AuditPayloadMode.Full;
        public AuditPayloadMode ReadAuditPayload { get; set; } = AuditPayloadMode.Full;
        public IStorageProtector Protector { get; set; }
        public IMessageStore Provider { get; set; }
        public bool DisposeProvider { get; set; } = true;
    }

    public sealed class DeliveryOptions
    {
        public int MaxConcurrentDestinations { get; set; } = 4;
        public int MaxAttempts { get; set; } = int.MaxValue;
        public TimeSpan MaxAge { get; set; } = TimeSpan.FromHours(24);
        public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(1);
        public TimeSpan MaximumRetryDelay { get; set; } = TimeSpan.FromMinutes(1);
        public double RetryBackoffFactor { get; set; } = 1.5;
    }

    public sealed class MessageQueueOptions
    {
        public string Name { get; set; }
        public Address Address { get; set; }
        public ILog Log { get; set; }
        public IStats Stats { get; set; }
        public int ConnectTimeOutMs { get; set; } = 500;
        public int MaxMessagesInMemory { get; set; } = 8192;
        public int ReorderLevel { get; set; } = 4096;
        public TimeSpan? VisibilityTimeout { get; set; }
        public StorageOptions Storage { get; set; } = new StorageOptions();
        public DeliveryOptions Delivery { get; set; } = new DeliveryOptions();
    }

    public sealed class StorageEntry
    {
        public string Key { get; set; }
        public string Value { get; set; }
        public long Length { get; set; }
        public DateTime CreatedUtc { get; set; }
        public DateTime LastModifiedUtc { get; set; }
    }

    public sealed class StorageAreaStatistics
    {
        public long Count { get; set; }
        public long Bytes { get; set; }
        public DateTime? OldestUtc { get; set; }
    }

    public sealed class QueueStorageHealth
    {
        public QueueState State { get; set; }
        public Exception LastException { get; set; }
        public long IncomingMessages { get; set; }
        public long OutgoingMessages { get; set; }
        public long DeadLetterMessages { get; set; }
        public long CorruptMessages { get; set; }
        public long StoredBytes { get; set; }
        public DateTime? OldestIncomingUtc { get; set; }
        public DateTime? OldestOutgoingUtc { get; set; }
    }

    public sealed class DeadLetter
    {
        public string Key { get; set; }
        public Guid MessageId { get; set; }
        public Address Destination { get; set; }
        public DateTime Sent { get; set; }
        public int Attempts { get; set; }
        public string MessageTypeName { get; set; }
        public string Reason { get; set; }
    }

    public sealed class QueueCapacityExceededException : IOException
    {
        public QueueCapacityExceededException(string message) : base(message) { }
    }

    public interface IStorageProtector
    {
        string Protect(string value);
        string Unprotect(string value);
    }

    public interface IMessageStore : IDisposable
    {
        IReadOnlyList<string> GetKeys(StorageArea area);
        bool Contains(StorageArea area, string key);
        StorageEntry Read(StorageArea area, string key);
        void Write(StorageArea area, string key, string value, DurabilityMode durability);
        void Append(StorageArea area, string key, string value, DurabilityMode durability);
        void Delete(StorageArea area, string key);
        void Move(StorageArea source, StorageArea destination, string key);
        void Purge(StorageArea area, DateTime olderThanUtc);
        StorageAreaStatistics GetStatistics(StorageArea area);
        Exception LastException { get; }
        void ClearException();
        void Flush();
    }
}
