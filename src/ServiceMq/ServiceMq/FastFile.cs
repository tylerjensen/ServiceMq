using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    public class FastFile : IDisposable
    {
        private const int AppendQueueMaxMinutesToLive = 5;

        private object syncRoot = new object();
        private Exception lastDeleteException;
        private Exception lastAppendException;
        private Exception lastWriteAllException;

        private ConcurrentQueue<string> deleteQueue = 
            new ConcurrentQueue<string>();
        private ConcurrentDictionary<string, ConcurrentQueue<string[]>> appendQueues =
            new ConcurrentDictionary<string, ConcurrentQueue<string[]>>();
        private ConcurrentDictionary<string, byte> pendingWrites = 
            new ConcurrentDictionary<string, byte>();
        private ConcurrentQueue<Tuple<string, string>> writeTextQueue = 
            new ConcurrentQueue<Tuple<string, string>>(); 
        private ConcurrentDictionary<string, DateTime> lastAppendTimes =
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
                pendingWrites.TryAdd(fileName, 0);
                writeTextQueue.Enqueue(new Tuple<string, string>(fileName, text));
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
                File.WriteAllText(fileName, text);
            }
        }

        private void ProcessWriteAll()
        {
            while (continueProcessing)
            {
                if (writeAllSignal.WaitOne(100))
                {
                    writeAllSignal.Reset();
                    while (!writeTextQueue.IsEmpty)
                    {
                        Tuple<string, string> data;
                        if (writeTextQueue.TryDequeue(out data))
                        {
                            try
                            {
                                File.WriteAllText(data.Item1, data.Item2);
                            }
                            catch (Exception e)
                            {
                                lastWriteAllException = e;
                            }
                            finally
                            {
                                byte s;
                                pendingWrites.TryRemove(data.Item1, out s);
                            }
                        }
                    }
                }
            }
        }

        public void AppendAllLines(string fileName, string[] lines)
        {
            if (asyncAppends)
            {
                var queue = appendQueues.GetOrAdd(fileName, new ConcurrentQueue<string[]>());
                queue.Enqueue(lines);
                lastAppendTimes.AddOrUpdate(fileName, DateTime.Now, (s, time) => DateTime.Now);
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
                File.AppendAllLines(fileName, lines);
            }
        }

        private void ProcessAppends()
        {
            while (continueProcessing)
            {
                if (appendSignal.WaitOne(100))
                {
                    appendSignal.Reset();
                    //append and clean up - clean up
                    try
                    {
                        var files = appendQueues.Keys.ToArray();
                        foreach (var file in files)
                        {
                            ConcurrentQueue<string[]> queue;
                            if (appendQueues.TryGetValue(file, out queue))
                            {
                                //pull all lines from queue
                                var lines = new List<string>();
                                while (!queue.IsEmpty)
                                {
                                    string[] txt;
                                    if (queue.TryDequeue(out txt))
                                    {
                                        lines.AddRange(txt);
                                    }
                                }

                                //we have all lines, write in one write to file
                                try
                                {
                                    File.AppendAllLines(file, lines);
                                }
                                catch (Exception ex)
                                {
                                    lastWriteAllException = ex;
                                }

                                //see if queue should be retired
                                DateTime lastWrite;
                                if (lastAppendTimes.TryGetValue(file, out lastWrite))
                                {
                                    if ((DateTime.Now - lastWrite).TotalMinutes > AppendQueueMaxMinutesToLive)
                                    {
                                        appendQueues.TryRemove(file, out queue);
                                        lastAppendTimes.TryRemove(file, out lastWrite);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        lastWriteAllException = e;
                    }
                }
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
            while (continueProcessing)
            {
                if (deleteSignal.WaitOne(100))
                {
                    try
                    {
                        deleteSignal.Reset();
                        while (!deleteQueue.IsEmpty)
                        {
                            try
                            {
                                string fileName;
                                if (deleteQueue.TryDequeue(out fileName))
                                {
                                    //don't try to delete it until it has been written if in fact its is pending
                                    SpinWait.SpinUntil(() => !pendingWrites.ContainsKey(fileName));
                                    DeleteFile(fileName);
                                }
                            }
                            catch (Exception ie)
                            {
                                lastDeleteException = ie;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        lastDeleteException = e;
                    }
                }
            }
        }

        private void DeleteFile(string fileName)
        {
            if (Win32Utils.DeleteFile(fileName)) return;
            int lastWin32Error = Marshal.GetLastWin32Error();
            if (lastWin32Error == 2)
                return;
            throw new IOException("Delete failed", lastWin32Error);
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
                if (disposing)
                {
                    //cleanup here
                    continueProcessing = false;
                    if (null != deleteTask) deleteTask.Wait(2000);
                    if (null != appendTask) appendTask.Wait(2000);
                    if (null != writeAllTask) writeAllTask.Wait(2000);
                    if (null != deleteSignal)
                    {
                        deleteSignal.Dispose();
                        deleteSignal = null;
                    }
                    if (null != appendSignal)
                    {
                        appendSignal.Dispose();
                        appendSignal = null;
                    }
                    if (null != writeAllSignal)
                    {
                        writeAllSignal.Dispose();
                        writeAllSignal = null;
                    }
                    if (null != deleteTask)
                    {
                        deleteTask.Dispose();
                        deleteTask = null;
                    }
                    if (null != appendTask)
                    {
                        appendTask.Dispose();
                        appendTask = null;
                    }
                    if (null != writeAllTask)
                    {
                        writeAllTask.Dispose();
                        writeAllTask = null;
                    }
                }
            }
        }

        #endregion

        internal static class Win32Utils
        {
            [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true, BestFitMapping = false)]
            internal static extern bool DeleteFile(string path);
        }
    }
}
