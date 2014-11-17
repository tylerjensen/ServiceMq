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
        private const int MaxDeleteQueueCount = 64;
        private const int MaxSecondsToDelete = 4;
        private const int MaxSecondsToAppend = 2;
        private const int MaxMinutesToLiveInAppendQueue = 3;

        private static DateTime lastDeleted = DateTime.Now;
        private static DateTime lastAppended = DateTime.Now;
        private static Exception lastDeleteException;
        private static Exception lastAppendException;
        private static Exception lastWriteAllException;
        private static ConcurrentQueue<string> deleteQueue = new ConcurrentQueue<string>();
        private static ConcurrentDictionary<string, ConcurrentQueue<string[]>> appendQueues =
            new ConcurrentDictionary<string, ConcurrentQueue<string[]>>();
        private static ConcurrentDictionary<string, byte> pendingWrites = new ConcurrentDictionary<string, byte>();
        private static ConcurrentDictionary<string, DateTime> lastAppendTimes =
            new ConcurrentDictionary<string, DateTime>(); 


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
            Task.Factory.StartNew(() =>
            {
                try
                {
                    File.WriteAllText(fileName, text);
                }
                catch (Exception e)
                {
                    lastWriteAllException = e;
                }
                finally
                {
                    byte s;
                    pendingWrites.TryRemove(fileName, out s);
                }
            });
        }

        internal static void AppendAllLines(string fileName, string[] lines)
        {
            var queue = appendQueues.GetOrAdd(fileName, new ConcurrentQueue<string[]>());
            queue.Enqueue(lines);
            lastAppendTimes.AddOrUpdate(fileName, DateTime.Now, (s, time) => DateTime.Now);
            ProcessAppends();
        }

        private static void ProcessAppends()
        {
            if ((DateTime.Now - lastAppended).TotalSeconds > MaxSecondsToAppend)
            {
                lastAppended = DateTime.Now;
                AsyncWriteAll();
            }
        }

        private static void AsyncWriteAll()
        {
            Task.Factory.StartNew(WriteAll);
        }

        internal static void WriteAll()
        {
            //append and clean up - clean up
            try
            {
                //prevent two threads from entering here at same time
                lock (appendQueues)
                {
                    lastAppended = DateTime.Now;

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
                                if ((DateTime.Now - lastWrite).TotalMinutes > MaxMinutesToLiveInAppendQueue)
                                {
                                    appendQueues.TryRemove(file, out queue);
                                    lastAppendTimes.TryRemove(file, out lastWrite);
                                }
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

        internal static void DeleteAsync(string fileName)
        {
            deleteQueue.Enqueue(fileName);
            ProcessDeletes();
        }

        private static void ProcessDeletes()
        {
            if (deleteQueue.Count > MaxDeleteQueueCount)
            {
                AsyncDeletes();
            }
            else if ((DateTime.Now - lastDeleted).TotalSeconds > MaxSecondsToDelete)
            {
                lastDeleted = DateTime.Now;
                AsyncDeletes();
            }
        }

        private static void AsyncDeletes()
        {
            Task.Factory.StartNew(DeleteAll);
        }

        internal static void DeleteAll()
        {
            try
            {
                lastDeleted = DateTime.Now;
                while (!deleteQueue.IsEmpty)
                {
                    try
                    {
                        string fileName;
                        if (deleteQueue.TryDequeue(out fileName))
                        {
                            Delete(fileName);
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

        private static void Delete(string fileName)
        {
            //don't try to delete it until it has been written if in fact its is pending
            SpinWait.SpinUntil(() => !pendingWrites.ContainsKey(fileName));
            if (Win32Utils.DeleteFile(fileName)) return;
            int lastWin32Error = Marshal.GetLastWin32Error();
            if (lastWin32Error == 2)
                return;
            throw new IOException("Delete failed", lastWin32Error);
        }

        internal static class Win32Utils
        {
            [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true, BestFitMapping = false)]
            internal static extern bool DeleteFile(string path);
        }
    }
}
