using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Data.Sqlite;

namespace ServiceMq
{
    public sealed class SqliteMessageStore : IMessageStore
    {
        private readonly object syncRoot = new object();
        private readonly SqliteConnection connection;
        private Exception lastException;

        public Exception LastException { get { return lastException; } }
        public string DatabasePath { get; private set; }

        public SqliteMessageStore(string databasePath)
        {
            if (string.IsNullOrWhiteSpace(databasePath)) throw new ArgumentException("A database path is required.", "databasePath");
            DatabasePath = Path.GetFullPath(databasePath);
            var directory = Path.GetDirectoryName(DatabasePath);
            if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
            var builder = new SqliteConnectionStringBuilder
            {
                DataSource = DatabasePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Shared
            };
            connection = new SqliteConnection(builder.ToString());
            connection.Open();
            ExecuteNonQuery("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; " +
                "CREATE TABLE IF NOT EXISTS queue_items (" +
                "area INTEGER NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, length INTEGER NOT NULL, " +
                "created_ticks INTEGER NOT NULL, modified_ticks INTEGER NOT NULL, PRIMARY KEY(area, key)); " +
                "CREATE INDEX IF NOT EXISTS ix_queue_items_area_created ON queue_items(area, created_ticks);");
        }

        public IReadOnlyList<string> GetKeys(StorageArea area)
        {
            lock (syncRoot)
            {
                var result = new List<string>();
                using (var command = CreateCommand("SELECT key FROM queue_items WHERE area = $area ORDER BY key"))
                {
                    command.Parameters.AddWithValue("$area", (int)area);
                    using (var reader = command.ExecuteReader()) while (reader.Read()) result.Add(reader.GetString(0));
                }
                return result;
            }
        }

        public bool Contains(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                using (var command = CreateCommand("SELECT 1 FROM queue_items WHERE area=$area AND key=$key LIMIT 1"))
                {
                    command.Parameters.AddWithValue("$area", (int)area);
                    command.Parameters.AddWithValue("$key", key);
                    return command.ExecuteScalar() != null;
                }
            }
        }

        public StorageEntry Read(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                using (var command = CreateCommand("SELECT value, length, created_ticks, modified_ticks FROM queue_items WHERE area = $area AND key = $key"))
                {
                    command.Parameters.AddWithValue("$area", (int)area);
                    command.Parameters.AddWithValue("$key", key);
                    using (var reader = command.ExecuteReader())
                    {
                        if (!reader.Read()) throw new KeyNotFoundException(key);
                        return new StorageEntry
                        {
                            Key = key,
                            Value = reader.GetString(0),
                            Length = reader.GetInt64(1),
                            CreatedUtc = new DateTime(reader.GetInt64(2), DateTimeKind.Utc),
                            LastModifiedUtc = new DateTime(reader.GetInt64(3), DateTimeKind.Utc)
                        };
                    }
                }
            }
        }

        public void Write(StorageArea area, string key, string value, DurabilityMode durability)
        {
            lock (syncRoot)
            {
                try
                {
                    var now = DateTime.UtcNow.Ticks;
                    using (var command = CreateCommand(
                        "INSERT INTO queue_items(area,key,value,length,created_ticks,modified_ticks) VALUES($area,$key,$value,$length,$now,$now) " +
                        "ON CONFLICT(area,key) DO UPDATE SET value=$value,length=$length,modified_ticks=$now"))
                    {
                        command.Parameters.AddWithValue("$area", (int)area);
                        command.Parameters.AddWithValue("$key", key);
                        command.Parameters.AddWithValue("$value", value ?? string.Empty);
                        command.Parameters.AddWithValue("$length", Encoding.UTF8.GetByteCount(value ?? string.Empty));
                        command.Parameters.AddWithValue("$now", now);
                        command.ExecuteNonQuery();
                    }
                }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void Append(StorageArea area, string key, string value, DurabilityMode durability)
        {
            lock (syncRoot)
            {
                try
                {
                    var now = DateTime.UtcNow.Ticks;
                    var normalized = value ?? string.Empty;
                    var separator = Environment.NewLine;
                    using (var command = CreateCommand(
                        "INSERT INTO queue_items(area,key,value,length,created_ticks,modified_ticks) " +
                        "VALUES($area,$key,$value,$value_length,$now,$now) " +
                        "ON CONFLICT(area,key) DO UPDATE SET " +
                        "value=queue_items.value || $separator || $value," +
                        "length=queue_items.length + $append_length,modified_ticks=$now"))
                    {
                        command.Parameters.AddWithValue("$area", (int)area);
                        command.Parameters.AddWithValue("$key", key);
                        command.Parameters.AddWithValue("$value", normalized);
                        command.Parameters.AddWithValue("$separator", separator);
                        command.Parameters.AddWithValue("$value_length", Encoding.UTF8.GetByteCount(normalized));
                        command.Parameters.AddWithValue("$append_length", Encoding.UTF8.GetByteCount(separator + normalized));
                        command.Parameters.AddWithValue("$now", now);
                        command.ExecuteNonQuery();
                    }
                }
                catch (Exception ex) { lastException = ex; throw; }
            }
        }

        public void Delete(StorageArea area, string key)
        {
            lock (syncRoot)
            {
                using (var command = CreateCommand("DELETE FROM queue_items WHERE area=$area AND key=$key"))
                {
                    command.Parameters.AddWithValue("$area", (int)area);
                    command.Parameters.AddWithValue("$key", key);
                    command.ExecuteNonQuery();
                }
            }
        }

        public void Move(StorageArea source, StorageArea destination, string key)
        {
            lock (syncRoot)
            {
                using (var transaction = connection.BeginTransaction())
                {
                    using (var delete = CreateCommand("DELETE FROM queue_items WHERE area=$destination AND key=$key"))
                    {
                        delete.Transaction = transaction;
                        delete.Parameters.AddWithValue("$destination", (int)destination);
                        delete.Parameters.AddWithValue("$key", key);
                        delete.ExecuteNonQuery();
                    }
                    using (var move = CreateCommand("UPDATE queue_items SET area=$destination,modified_ticks=$now WHERE area=$source AND key=$key"))
                    {
                        move.Transaction = transaction;
                        move.Parameters.AddWithValue("$destination", (int)destination);
                        move.Parameters.AddWithValue("$source", (int)source);
                        move.Parameters.AddWithValue("$key", key);
                        move.Parameters.AddWithValue("$now", DateTime.UtcNow.Ticks);
                        move.ExecuteNonQuery();
                    }
                    transaction.Commit();
                }
            }
        }

        public void Purge(StorageArea area, DateTime olderThanUtc)
        {
            lock (syncRoot)
            {
                using (var command = CreateCommand("DELETE FROM queue_items WHERE area=$area AND modified_ticks < $ticks"))
                {
                    command.Parameters.AddWithValue("$area", (int)area);
                    command.Parameters.AddWithValue("$ticks", olderThanUtc.Ticks);
                    command.ExecuteNonQuery();
                }
            }
        }

        public StorageAreaStatistics GetStatistics(StorageArea area)
        {
            lock (syncRoot)
            {
                using (var command = CreateCommand("SELECT COUNT(*),COALESCE(SUM(length),0),MIN(created_ticks) FROM queue_items WHERE area=$area"))
                {
                    command.Parameters.AddWithValue("$area", (int)area);
                    using (var reader = command.ExecuteReader())
                    {
                        reader.Read();
                        return new StorageAreaStatistics
                        {
                            Count = reader.GetInt64(0),
                            Bytes = reader.GetInt64(1),
                            OldestUtc = reader.IsDBNull(2) ? (DateTime?)null : new DateTime(reader.GetInt64(2), DateTimeKind.Utc)
                        };
                    }
                }
            }
        }

        public void Flush()
        {
            lock (syncRoot) ExecuteNonQuery("PRAGMA wal_checkpoint(FULL)");
        }

        public void ClearException() { lastException = null; }

        public void Dispose()
        {
            lock (syncRoot)
            {
                Flush();
                connection.Dispose();
            }
        }

        private SqliteCommand CreateCommand(string sql)
        {
            var command = connection.CreateCommand();
            command.CommandText = sql;
            return command;
        }

        private void ExecuteNonQuery(string sql)
        {
            using (var command = CreateCommand(sql)) command.ExecuteNonQuery();
        }
    }
}
