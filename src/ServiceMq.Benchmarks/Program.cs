using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using ServiceMq;

namespace ServiceMq.Benchmarks
{
    /// <summary>
    /// In-process IMessageStore comparison: SQLite provider vs SharpCoreDB provider.
    /// Each operation is measured over several samples on freshly prepared fixtures and the
    /// median is reported, so no sample benefits from an earlier one having emptied the table
    /// or created the rows it is supposed to touch.
    /// </summary>
    internal static class Program
    {
        private const int Samples = 3;
        private const int WriteCount = 5000;
        private const int ReadCount = 5000;
        private const int ContainsCount = 5000;
        private const int AppendCount = 1000;
        private const int MoveCount = 500;
        private const int DeleteCount = 500;
        private const int PurgeRows = 500;
        private const string BenchmarkPassword = "servicemq-benchmark-master-password";

        public static void Main()
        {
            Console.WriteLine("ServiceMq Storage Provider Benchmark (SQLite vs SharpCoreDB)");
            Console.WriteLine($"  OS:        {RuntimeInformation.OSDescription} ({RuntimeInformation.OSArchitecture})");
            Console.WriteLine($"  Runtime:   {RuntimeInformation.FrameworkDescription} ({RuntimeInformation.ProcessArchitecture})");
            Console.WriteLine($"  CPU:       {Environment.ProcessorCount} logical processors");
            Console.WriteLine($"  Method:    median of {Samples} samples per operation, fixtures rebuilt per sample,");
            Console.WriteLine($"             both providers run sequentially in this one process");
            Console.WriteLine();

            var sqliteRoot = Path.Combine(Path.GetTempPath(), "ServiceMq.Benchmarks", "sqlite-" + Guid.NewGuid().ToString("N"));
            var sharpRoot = Path.Combine(Path.GetTempPath(), "ServiceMq.Benchmarks", "sharpcoredb-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(sqliteRoot);
            Directory.CreateDirectory(sharpRoot);

            try
            {
                using (var sqlite = new SqliteMessageStore(Path.Combine(sqliteRoot, "queue.db")))
                {
                    Run("SQLite (net8.0 provider, unencrypted)", sqlite);
                }

                using (var sharp = new SharpCoreDbMessageStore(sharpRoot, BenchmarkPassword))
                {
                    Run("SharpCoreDB (net10.0, AES-256-GCM payload encryption)", sharp);
                }
            }
            finally
            {
                try { if (Directory.Exists(sqliteRoot)) Directory.Delete(sqliteRoot, true); } catch { }
                try { if (Directory.Exists(sharpRoot)) Directory.Delete(sharpRoot, true); } catch { }
            }

            Console.WriteLine();
            Console.WriteLine("Note: SharpCoreDB performs provider-level AES-256-GCM encryption on every");
            Console.WriteLine("payload (Write/Append/Move), which SQLite does not offer out of the box. The");
            Console.WriteLine("encryption overhead is included in these numbers, so write-side rows are not");
            Console.WriteLine("comparing equivalent work.");
        }

        private static void Run(string title, IMessageStore store)
        {
            Console.WriteLine($"=== {title} ===");

            // Warm-up
            store.Write(StorageArea.Outgoing, "warmup", "warmup", DurabilityMode.FlushToDisk);
            store.Flush();

            // Durable point writes. Each sample writes a distinct key range so every write is an
            // insert, not an overwrite of the previous sample.
            Measure("Write (FlushToDisk)", WriteCount, sample =>
            {
                for (int i = 0; i < WriteCount; i++)
                    store.Write(StorageArea.Outgoing, WriteKey(sample, i), BuildPayload(i), DurabilityMode.FlushToDisk);
            });

            // Buffered writes, with the flush that makes them durable included in the timing.
            Measure("Write (Buffered + 1 Flush)", WriteCount, sample =>
            {
                for (int i = 0; i < WriteCount; i++)
                    store.Write(StorageArea.Incoming, $"buf-s{sample}-{i}", BuildPayload(i), DurabilityMode.Buffered);
                store.Flush();
            });

            // Point lookups over the rows written by sample 0 of the durable-write run.
            Measure("Contains", ContainsCount, _ =>
            {
                for (int i = 0; i < ContainsCount; i++)
                {
                    if (!store.Contains(StorageArea.Outgoing, WriteKey(0, i))) throw new InvalidOperationException("missing");
                }
            });

            Measure("Read", ReadCount, _ =>
            {
                for (int i = 0; i < ReadCount; i++)
                {
                    var e = store.Read(StorageArea.Outgoing, WriteKey(0, i));
                    if (e.Value == null) throw new InvalidOperationException("null value");
                }
            });

            // Append to rows that already exist, so the measured path is read-decrypt-concat-
            // encrypt-update, not the missing-key shortcut that simply performs a Write.
            Measure("Append to existing (FlushToDisk)", AppendCount,
                setup: sample =>
                {
                    for (int i = 0; i < AppendCount; i++)
                        store.Write(StorageArea.Sent, $"audit-s{sample}-{i}", BuildPayload(i), DurabilityMode.FlushToDisk);
                },
                action: sample =>
                {
                    for (int i = 0; i < AppendCount; i++)
                        store.Append(StorageArea.Sent, $"audit-s{sample}-{i}", BuildPayload(i), DurabilityMode.FlushToDisk);
                });

            Measure("GetStatistics", 100, _ =>
            {
                for (int i = 0; i < 100; i++) store.GetStatistics(StorageArea.Outgoing);
            });

            // Each sample moves its own key range, so every Move finds a source row.
            Measure("Move (Outgoing → DeadLetter)", MoveCount,
                setup: sample =>
                {
                    for (int i = 0; i < MoveCount; i++)
                    {
                        if (!store.Contains(StorageArea.Outgoing, WriteKey(sample, i)))
                            store.Write(StorageArea.Outgoing, WriteKey(sample, i), BuildPayload(i), DurabilityMode.FlushToDisk);
                    }
                },
                action: sample =>
                {
                    for (int i = 0; i < MoveCount; i++)
                        store.Move(StorageArea.Outgoing, StorageArea.DeadLetter, WriteKey(sample, i));
                });

            // One purge that removes PurgeRows rows, freshly written per sample. Reported per row.
            Measure($"Purge ({PurgeRows} rows, per row)", PurgeRows,
                setup: sample =>
                {
                    for (int i = 0; i < PurgeRows; i++)
                        store.Write(StorageArea.Read, $"purge-s{sample}-{i}", BuildPayload(i), DurabilityMode.Buffered);
                    store.Flush();
                },
                action: _ => store.Purge(StorageArea.Read, DateTime.UtcNow.AddSeconds(1)));

            // Deletes the rows the Move run placed in DeadLetter for the same sample.
            Measure("Delete (DeadLetter)", DeleteCount,
                setup: sample =>
                {
                    for (int i = 0; i < DeleteCount; i++)
                    {
                        if (!store.Contains(StorageArea.DeadLetter, WriteKey(sample, i)))
                            store.Write(StorageArea.DeadLetter, WriteKey(sample, i), BuildPayload(i), DurabilityMode.FlushToDisk);
                    }
                },
                action: sample =>
                {
                    for (int i = 0; i < DeleteCount; i++)
                        store.Delete(StorageArea.DeadLetter, WriteKey(sample, i));
                });

            store.Flush();
            Console.WriteLine();
        }

        private static string WriteKey(int sample, int i) => $"msg-s{sample}-{i}";

        private static string BuildPayload(int i) => $"payload-{i}-" + new string('x', 64);

        private static void Measure(string name, int iterations, Action<int> action) =>
            Measure(name, iterations, null, action);

        private static void Measure(string name, int iterations, Action<int> setup, Action<int> action)
        {
            var elapsed = new List<double>(Samples);
            for (int sample = 0; sample < Samples; sample++)
            {
                setup?.Invoke(sample);

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                var sw = Stopwatch.StartNew();
                action(sample);
                sw.Stop();
                elapsed.Add(sw.Elapsed.TotalMilliseconds);
            }

            elapsed.Sort();
            var median = elapsed[elapsed.Count / 2];
            var opsPerSec = (long)(iterations / (median / 1000.0));
            Console.WriteLine($"  {name,-34} median {median,9:F1} ms  ({opsPerSec,10:N0} ops/sec)   min {elapsed.First(),8:F1}  max {elapsed.Last(),8:F1} ms");
        }
    }
}
