using System;
using System.Collections.Generic;
using System.Globalization;

namespace ServiceMq
{
    /// <summary>
    /// Creates keys whose ordinal sort order is their enqueue order. The last durable
    /// timestamp is used as a floor so a clock adjustment cannot move a new record in
    /// front of a record that was already stored.
    /// </summary>
    internal sealed class MonotonicQueueKeyGenerator
    {
        private const string TimestampFormat = "yyyyMMddHHmmssfffffff";
        private const string LegacyTimestampFormat = "yyyyMMddHHmmssfff";
        private DateTime lastTimestampUtc;
        private long sequence;

        public MonotonicQueueKeyGenerator(IEnumerable<string> existingKeys)
        {
            if (existingKeys == null) return;

            foreach (var key in existingKeys)
            {
                DateTime timestamp;
                if (TryReadTimestamp(key, out timestamp) && timestamp > lastTimestampUtc)
                    lastTimestampUtc = timestamp;
            }
        }

        public string Next(string suffix)
        {
            var timestamp = DateTime.UtcNow;
            if (timestamp <= lastTimestampUtc)
                timestamp = lastTimestampUtc.AddTicks(1);

            lastTimestampUtc = timestamp;
            sequence++;
            return timestamp.ToString(TimestampFormat, CultureInfo.InvariantCulture) + "-" +
                sequence.ToString("D10", CultureInfo.InvariantCulture) + "-" +
                Guid.NewGuid().ToString("N") + suffix;
        }

        private static bool TryReadTimestamp(string key, out DateTime timestamp)
        {
            timestamp = default(DateTime);
            if (string.IsNullOrEmpty(key)) return false;

            if (key.Length > TimestampFormat.Length && key[TimestampFormat.Length] == '-' &&
                DateTime.TryParseExact(key.Substring(0, TimestampFormat.Length), TimestampFormat,
                    CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out timestamp)) return true;

            return key.Length > LegacyTimestampFormat.Length && key[LegacyTimestampFormat.Length] == '-' &&
                DateTime.TryParseExact(key.Substring(0, LegacyTimestampFormat.Length), LegacyTimestampFormat,
                    CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out timestamp);
        }
    }
}
