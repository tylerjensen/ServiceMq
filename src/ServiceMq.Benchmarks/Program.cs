using System;
using System.Diagnostics;
using System.IO;
using ServiceMq;

namespace ServiceMq.Benchmarks
{
    internal static class Program
    {
        private const int WriteCount = 5000;
        private const int ReadCount = 5000;
        private const int ContainsCount = 5000;
        private const int AppendCount = 1000;
        private const int MoveCount = 500;
        private const int DeleteCount = 500;

        public static void Main()
        {
            Console.WriteLine("ServiceMq Storage Provider Benchmark (SQLite vs SharpCoreDB)");
            Console.WriteLine("Machine: .NET 10.0, in-process IMessageStore operations");
            Console.WriteLine();

            var sqliteRoot = Path.Combine(Path.GetTempPath(), "ServiceMq.Benchmarks", "sqlite-" + Guid.NewGuid().ToString("N"));
            var sharpRoot = Path.Combine(Path.GetTempPath(), "ServiceMq.Benchmarks", "sharpcoredb-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(sqliteRoot);
            Directory.CreateDirectory(sharpRoot);

            try
            {
                using (var sqlite = new SqliteMessageStore(Path.Combine(sqliteRoot, "queue.db")))
                {
                    Run("SQLite (net8.0 provider, unencrypted)", "sqlite.db", sqlite);
                }

                using (var sharp = new SharpCoreDbMessageStore(sharpRoot))
                {
                    Run("SharpCoreDB (net10.0, AES-256-GCM payload encryption)", "queue_items", sharp);
                }
            }
            finally
            {
                try { if (Directory.Exists(sqliteRoot)) Directory.Delete(sqliteRoot, true); } catch { }
                try { if (Directory.Exists(sharpRoot)) Directory.Delete(sharpRoot, true); } catch { }
            }

            Console.WriteLine();
            Console.WriteLine("Note: SharpCoreDB performs provider-level AES-256-GCM encryption on every");
            Console.WriteLine("payload (Write/Append), which SQLite does not offer out of the box. The");
            Console.WriteLine("encryption overhead is included in these numbers.");
            Console.WriteLine();
            Console.WriteLine("For a fair 'apples-to-apples' SQL comparison run SharpCoreDB without the");
            Console.WriteLine("provider-encryption layer (see the store source for the Protect/Unprotect calls).");
        }

        private static void Run(string title, string label, IMessageStore store)
        {
            Console.WriteLine($"=== {title} ===");
            Console.WriteLine($"  {label}");

            // Warm-up
            store.Write(StorageArea.Outgoing, "warmup", "warmup", DurabilityMode.FlushToDisk);

            Measure("Write (FlushToDisk)", WriteCount, () =>
            {
                for (int i = 0; i < WriteCount; i++)
                    store.Write(StorageArea.Outgoing, "msg-" + i, BuildPayload(i), DurabilityMode.FlushToDisk);
            });
            store.Flush();

            Measure("Contains", ContainsCount, () =>
            {
                for (int i = 0; i < ContainsCount; i++)
                {
                    if (!store.Contains(StorageArea.Outgoing, "msg-" + i)) throw new InvalidOperationException("missing");
                }
            });

            Measure("Read", ReadCount, () =>
            {
                for (int i = 0; i < ReadCount; i++)
                {
                    var e = store.Read(StorageArea.Outgoing, "msg-" + i);
                    if (e.Value == null) throw new InvalidOperationException("null value");
                }
            });

            Measure("Append (FlushToDisk)", AppendCount, () =>
            {
                for (int i = 0; i < AppendCount; i++)
                    store.Append(StorageArea.Incoming, "audit-" + i, BuildPayload(i), DurabilityMode.FlushToDisk);
            });
            store.Flush();

            Measure("GetStatistics", 100, () =>
            {
                for (int i = 0; i < 100; i++) store.GetStatistics(StorageArea.Outgoing);
            });

            Measure("Move (Outgoing → DeadLetter)", MoveCount, () =>
            {
                for (int i = 0; i < MoveCount; i++)
                    store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, "msg-" + i);
            });
            store.Flush();

            // Purge a subset (entries not moved).
            Measure("Purge", 50, () =>
            {
                for (int i = 0; i < 50; i++)
                    store.Purge(StorageArea.Outgoing, DateTime.UtcNow);
            });

            Measure("Delete (DeadLetter)", DeleteCount, () =>
            {
                for (int i = 0; i < DeleteCount; i++)
                    store.Delete(StorageArea.DeadLetter, "msg-" + i);
            });
            store.Flush();

            Console.WriteLine();
        }

        private static string BuildPayload(int i) => $"payload-{i}-" + new string('x', 64);

        private static void Measure(string name, int iterations, Action action)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var sw = Stopwatch.StartNew();
            action();
            sw.Stop();

            var opsPerSec = (long)(iterations / sw.Elapsed.TotalSeconds);
            Console.WriteLine($"  {name,-32} {sw.Elapsed.TotalMilliseconds,10:F1} ms  ({opsPerSec:N0} ops/sec)");
        }
    }
}