using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    public class FastFile : IDisposable
    {
        private const int AppendQueueMaxMinutesToLive = 5;

        private readonly object syncRoot = new object();
        private Exception lastDeleteException;
        private Exception lastAppendException;
        private Exception lastWriteAllException;

        private readonly ConcurrentQueue<string> deleteQueue = new ConcurrentQueue<string>();
        private readonly ConcurrentDictionary<string, ConcurrentQueue<string[]>> appendQueues =
            new ConcurrentDictionary<string, ConcurrentQueue<string[]>>();
        private readonly ConcurrentDictionary<string, int> pendingWrites =
            new ConcurrentDictionary<string, int>();
        private readonly ConcurrentQueue<FileText> writeTextQueue = new ConcurrentQueue<FileText>();
        private readonly ConcurrentDictionary<string, DateTime> lastAppendTimes =
            new ConcurrentDictionary<string, DateTime>();

        private ManualResetEvent deleteSignal = null;
        private Task deleteTask = null;
        private ManualResetEvent writeAllSignal = null;
        private Task writeAllTask = null;
        private ManualResetEvent appendSignal = null;
        private Task appendTask = null;
        private bool continueProcessing = true;

        private readonly bool asyncDeletes;
        private readonly bool asyncAppends;
        private readonly bool asyncWrites;

        public Exception LastDeleteException { get { return lastDeleteException; } }
        public Exception LastAppendException { get { return lastAppendException; } }
        public Exception LastWriteAllException { get { return lastWriteAllException; } }

        /// <summary>
        /// FastFile provides fast asynchronous I/O for writing message files, 
        /// appending to message logs and deleting message files to improve performance.
        /// </summary>
        /// <param name="asyncDeletes">Set to true for async deletes of message files. Default is true.</param>
        /// <param name="asyncAppends">Set to true for async appends to log files. Default is true.</param>
        /// <param name="asyncWrites">Set to true for async writes of message files files. Default is false.</param>
        public FastFile(bool asyncDeletes = true, bool asyncAppends = true, bool asyncWrites = false)
        {
            this.asyncDeletes = asyncDeletes;
            this.asyncAppends = asyncAppends;
            this.asyncWrites = asyncWrites;
        }

        public void ClearExceptions()
        {
            lastDeleteException = null;
            lastAppendException = null;
            lastWriteAllException = null;
        }

        public string[] GetFiles(string path, string pattern)
        {
            return Directory.GetFiles(path, pattern);
        }

        public string ReadAllText(string fileName)
        {
            return File.ReadAllText(fileName);
        }

        public void WriteAllText(string fileName, string text)
        {
            if (asyncWrites)
            {
                pendingWrites.AddOrUpdate(fileName, 1, (key, count) => count + 1);
                writeTextQueue.Enqueue(new FileText(fileName, text));
                if (null == writeAllTask)
                {
                    lock (syncRoot)
                    {
                        if (null == writeAllTask)
                        {
                            writeAllSignal = new ManualResetEvent(false);
                            writeAllTask = Task.Factory.StartNew(ProcessWriteAll, CancellationToken.None,
                                TaskCreationOptions.LongRunning, TaskScheduler.Default);
                        }
                    }
                }
                writeAllSignal.Set();
            }
            else
            {
                WriteAtomic(fileName, text);
            }
        }

        private void ProcessWriteAll()
        {
            while (continueProcessing || !writeTextQueue.IsEmpty)
            {
                if (writeAllSignal.WaitOne(100))
                {
                    writeAllSignal.Reset();
                    while (!writeTextQueue.IsEmpty) WriteQueuedFile();
                }
            }
        }

        private void WriteQueuedFile()
        {
            if (!writeTextQueue.TryDequeue(out var data)) return;
            try
            {
                WriteAtomic(data.FileName, data.Text);
            }
            catch (Exception e)
            {
                lastWriteAllException = e;
            }
            finally
            {
                DecrementPendingWrites(data.FileName);
            }
        }

        private void DecrementPendingWrites(string fileName)
        {
            int remaining;
            while (pendingWrites.TryGetValue(fileName, out remaining))
            {
                if (remaining <= 1)
                {
                    int removed;
                    if (pendingWrites.TryRemove(fileName, out removed)) break;
                }
                else if (pendingWrites.TryUpdate(fileName, remaining - 1, remaining)) break;
            }
        }

        public void AppendAllLines(string fileName, string[] lines)
        {
            if (asyncAppends)
            {
                var queue = appendQueues.GetOrAdd(fileName, new ConcurrentQueue<string[]>());
                queue.Enqueue(lines);
                lastAppendTimes.AddOrUpdate(fileName, DateTime.UtcNow, (s, time) => DateTime.UtcNow);
                if (null == appendTask)
                {
                    lock (syncRoot)
                    {
                        if (null == appendTask)
                        {
                            appendSignal = new ManualResetEvent(false);
                            appendTask = Task.Factory.StartNew(ProcessAppends, CancellationToken.None,
                                TaskCreationOptions.LongRunning, TaskScheduler.Default);
                        }
                    }
                }
                appendSignal.Set();
            }
            else
            {
#if (!NET35)
                File.AppendAllLines(fileName, lines);
#else
                File.AppendAllText(fileName, string.Join("\r\n", lines));
#endif
            }
        }

        private void ProcessAppends()
        {
            while (continueProcessing || appendQueues.Values.Any(x => !x.IsEmpty))
            {
                if (appendSignal.WaitOne(100))
                {
                    appendSignal.Reset();
                    // Append and clean up.
                    try
                    {
                        foreach (var file in appendQueues.Keys.ToArray()) AppendQueuedLines(file);
                    }
                    catch (Exception e)
                    {
                        lastAppendException = e;
                    }
                }
            }
        }

        private void AppendQueuedLines(string file)
        {
            ConcurrentQueue<string[]> queue;
            if (!appendQueues.TryGetValue(file, out queue)) return;

            // Pull all lines from the queue.
            var lines = new List<string>();
            while (!queue.IsEmpty)
            {
                string[] txt;
                if (queue.TryDequeue(out txt)) lines.AddRange(txt);
            }

            // We have all lines, write them to the file in one write.
            try
            {
#if (!NET35)
                File.AppendAllLines(file, lines);
#else
                File.AppendAllText(file, string.Join("\r\n", lines.ToArray()));
#endif
            }
            catch (Exception ex)
            {
                lastAppendException = ex;
            }

            // See if the queue should be retired.
            DateTime lastWrite;
            if (lastAppendTimes.TryGetValue(file, out lastWrite) &&
                (DateTime.UtcNow - lastWrite).TotalMinutes > AppendQueueMaxMinutesToLive)
            {
                appendQueues.TryRemove(file, out queue);
                lastAppendTimes.TryRemove(file, out lastWrite);
            }
        }

        public void Delete(string fileName)
        {
            if (asyncDeletes)
            {
                deleteQueue.Enqueue(fileName);
                if (null == deleteTask)
                {
                    lock (syncRoot)
                    {
                        if (null == deleteTask)
                        {
                            deleteSignal = new ManualResetEvent(false);
                            deleteTask = Task.Factory.StartNew(ProcessDeletes, CancellationToken.None,
                                TaskCreationOptions.LongRunning, TaskScheduler.Default);
                        }
                    }
                }
                deleteSignal.Set();
            }
            else
            {
                DeleteFile(fileName);
            }
        }

        private void ProcessDeletes()
        {
            while (continueProcessing || !deleteQueue.IsEmpty)
            {
                if (deleteSignal.WaitOne(100))
                {
                    try
                    {
                        deleteSignal.Reset();
                        while (!deleteQueue.IsEmpty) DeleteQueuedFile();
                    }
                    catch (Exception e)
                    {
                        lastDeleteException = e;
                    }
                }
            }
        }

        private void DeleteQueuedFile()
        {
            try
            {
                string fileName;
                if (deleteQueue.TryDequeue(out fileName))
                {
                    // Don't try to delete it until it has been written if in fact it is pending.
                    SpinWait.SpinUntil(() => !pendingWrites.ContainsKey(fileName));
                    DeleteFile(fileName);
                }
            }
            catch (Exception ie)
            {
                lastDeleteException = ie;
            }
        }

        private void DeleteFile(string fileName)
        {
            if (File.Exists(fileName)) File.Delete(fileName);
        }

        private static void WriteAtomic(string fileName, string text)
        {
            var directory = Path.GetDirectoryName(Path.GetFullPath(fileName));
            if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);
            var temporary = fileName + "." + Guid.NewGuid().ToString("N") + ".tmp";
            try
            {
                using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
                {
                    writer.Write(text ?? string.Empty);
                    writer.Flush();
                    stream.Flush(true);
                }
                if (File.Exists(fileName)) File.Replace(temporary, fileName, null);
                else File.Move(temporary, fileName);
            }
            finally
            {
                if (File.Exists(temporary)) File.Delete(temporary);
            }
        }

        #region IDisposable

        private bool _disposed = false;

        public void Dispose()
        {
            //MS recommended dispose pattern - prevents GC from disposing again
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true; //prevent second cleanup
                if (disposing) DisposeManagedResources();
            }
        }

        private void DisposeManagedResources()
        {
            continueProcessing = false;
            if (null != deleteSignal) deleteSignal.Set();
            if (null != appendSignal) appendSignal.Set();
            if (null != writeAllSignal) writeAllSignal.Set();
            if (null != deleteTask) deleteTask.Wait();
            if (null != appendTask) appendTask.Wait();
            if (null != writeAllTask) writeAllTask.Wait();
            DisposeSignal(ref deleteSignal);
            DisposeSignal(ref appendSignal);
            DisposeSignal(ref writeAllSignal);
            DisposeTask(ref deleteTask);
            DisposeTask(ref appendTask);
            DisposeTask(ref writeAllTask);
        }

        private static void DisposeSignal(ref ManualResetEvent signal)
        {
            if (null == signal) return;
#if (!NET35)
            signal.Dispose();
#else
            signal.Close();
#endif
            signal = null;
        }

        private static void DisposeTask(ref Task task)
        {
            if (null == task) return;
            task.Dispose();
            task = null;
        }

        #endregion

    }
}
