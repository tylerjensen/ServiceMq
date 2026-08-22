#nullable enable

using System.Collections.Generic;
using SharpCoreDB.Interfaces;

namespace ServiceMq
{
    /// <summary>
    /// The slice of SharpCoreDB that <see cref="SharpCoreDbMessageStore"/> actually uses.
    /// Exists so tests can wrap the real table and inject failures or tampered rows at exact
    /// points (see the internal constructor on the store); production code always gets
    /// <see cref="SharpCoreDbQueueTable"/>.
    /// </summary>
    internal interface IQueueTable
    {
        Dictionary<string, object>? FindByPrimaryKey(object key);
        void Insert(Dictionary<string, object> row);
        bool UpdateByPrimaryKey(object key, Dictionary<string, object> updates);
        bool DeleteByPrimaryKey(object key);
        List<Dictionary<string, object>> Select();
        /// <summary>Flushes the owning database, not just this table.</summary>
        void Flush();
    }

    internal sealed class SharpCoreDbQueueTable(IDatabase database, ITable table) : IQueueTable
    {
        public Dictionary<string, object>? FindByPrimaryKey(object key) => table.FindByPrimaryKey(key);
        public void Insert(Dictionary<string, object> row) => table.Insert(row);
        public bool UpdateByPrimaryKey(object key, Dictionary<string, object> updates) => table.UpdateByPrimaryKey(key, updates);
        public bool DeleteByPrimaryKey(object key) => table.DeleteByPrimaryKey(key);
        public List<Dictionary<string, object>> Select() => table.Select();
        public void Flush() => database.Flush();
    }
}
