using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace ServiceMq
{
    internal static class FastFile
    {
        private const int MaxDeleteQueueCount = 64;
        private const int MaxSecondsToDelete = 4;
        private static DateTime lastDeleted = DateTime.Now;
        private static Exception lastException;

        internal static Exception LastException { get { return lastException; } }
        internal static void ClearException()
        {
            lastException = null;
        }

        internal static string[] GetFiles(string path, string pattern)
        {
            return Directory.GetFiles(path, pattern);
        }

        internal static string ReadAllText(string fileName)
        {
            return File.ReadAllText(fileName);
        }

        internal static void AppendAllLines(string fileName, string[] lines)
        {
            File.AppendAllLines(fileName, lines);
        }

        internal static void WriteAllText(string fileName, string text)
        {
            File.WriteAllText(fileName, text);
        }

        private static ConcurrentQueue<string> deleteQueue = new ConcurrentQueue<string>();

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
                AsyncDeletes();
            }
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
                        lastException = ie;
                    }
                }
            }
            catch (Exception e)
            {
                lastException = e;
            }
        }

        private static void AsyncDeletes()
        {
            Task.Factory.StartNew(() => DeleteAll());
        }

        private static void Delete(string fileName)
        {
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
