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
    internal static class FastFile
    {
        private const int AppendQueueMaxMinutesToLive = 5;

        private static object syncRoot = new object();
        private static Exception lastDeleteException;
        private static Exception lastAppendException;
        private static Exception lastWriteAllException;

        private static ConcurrentQueue<string> deleteQueue = new ConcurrentQueue<string>();
        private static ConcurrentDictionary<string, ConcurrentQueue<string[]>> appendQueues =
            new ConcurrentDictionary<string, ConcurrentQueue<string[]>>();
        private static ConcurrentDictionary<string, byte> pendingWrites = new ConcurrentDictionary<string, byte>();
        private static ConcurrentQueue<Tuple<string, string>> writeTextQueue = new ConcurrentQueue<Tuple<string, string>>(); 
        private static ConcurrentDictionary<string, DateTime> lastAppendTimes =
            new ConcurrentDictionary<string, DateTime>();

        private static ManualResetEvent deleteSignal = null;
        private static Task deleteTask = null;
        private static ManualResetEvent writeAllSignal = null;
        private static Task writeAllTask = null;
        private static ManualResetEvent appendSignal = null;
        private static Task appendTask = null;
        private static bool continueProcessing = true;

        internal static Exception LastDeleteException { get { return lastDeleteException; } }
        internal static Exception LastAppendException { get { return lastAppendException; } }
        internal static Exception LastWriteAllException { get { return lastWriteAllException; } }

        internal static void ClearExceptions()
        {
            lastDeleteException = null;
            lastAppendException = null;
            lastWriteAllException = null;
        }

        internal static string[] GetFiles(string path, string pattern)
        {
            return Directory.GetFiles(path, pattern);
        }

        internal static string ReadAllText(string fileName)
        {
            return File.ReadAllText(fileName);
        }

        internal static void WriteAllText(string fileName, string text)
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

        private static void ProcessWriteAll()
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

        internal static void AppendAllLines(string fileName, string[] lines)
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

        private static void ProcessAppends()
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

        internal static void Delete(string fileName)
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

        private static void ProcessDeletes()
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

        private static void DeleteFile(string fileName)
        {
            //don't try to delete it until it has been written if in fact its is pending
            SpinWait.SpinUntil(() => !pendingWrites.ContainsKey(fileName));
            if (Win32Utils.DeleteFile(fileName)) return;
            int lastWin32Error = Marshal.GetLastWin32Error();
            if (lastWin32Error == 2)
                return;
            throw new IOException("Delete failed", lastWin32Error);
        }

        internal static void Dispose()
        {
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

        internal static class Win32Utils
        {
            [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true, BestFitMapping = false)]
            internal static extern bool DeleteFile(string path);
        }
    }
}
