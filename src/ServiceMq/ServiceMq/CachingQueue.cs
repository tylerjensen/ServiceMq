using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    internal class CachingQueue<T>
    {
        private const int ReorderQty = 32;
        private readonly object syncRoot = new object();
        private readonly string msgDir;
        private readonly int maxMessagesInMemory;
        private readonly int reorderLevel;
        private readonly bool persistMessages;
        private readonly Queue<string> keysQueue;
        private readonly Queue<T> messageQueue;
        private readonly Func<string, T> loadFromFile;

        private volatile bool reloading = false;
        private Exception reloadException = null;
        public Exception ReloadException { get { return reloadException; } }

        public CachingQueue(string msgDir, Func<string, T> loadFromFile, string filePattern,
            int maxMessagesInMemory = 4096, int reorderLevel = 2048, bool persistMessages = true)
        {
            this.msgDir = msgDir;
            this.loadFromFile = loadFromFile;
            this.maxMessagesInMemory = maxMessagesInMemory;
            this.reorderLevel = reorderLevel;
            this.persistMessages = persistMessages;
            this.keysQueue = new Queue<string>(maxMessagesInMemory);
            this.messageQueue = new Queue<T>(maxMessagesInMemory);
            if (persistMessages) Initialize(filePattern);
        }

        private void Initialize(string filePattern)
        {
            lock (syncRoot)
            {
                // hydrate from any messages already in files
                var list = new List<string>(Directory.GetFiles(msgDir, filePattern));
                if (list.Count > 0)
                {
                    list.Sort();
                    foreach (var msgFile in list)
                    {
                        if (messageQueue.Count < maxMessagesInMemory)
                        {
                            var msg = loadFromFile(msgFile);
                            messageQueue.Enqueue(msg);
                        }
                        else
                        {
                            keysQueue.Enqueue(msgFile);
                        }
                    }
                }
            }
        }

        public T Dequeue()
        {
            lock (syncRoot)
            {
                if (messageQueue.Count > 0)
                {
                    var msg = messageQueue.Dequeue();
                    RefillCheck();
                    return msg;
                }
                return default(T);
            }
        }

        public T Peek()
        {
            lock (syncRoot)
            {
                if (messageQueue.Count > 0)
                {
                    var msg = messageQueue.Peek();
                    return msg;
                }
                return default(T);
            }
        }

        public void Enqueue(string key, T message)
        {
            if (persistMessages)
            {
                var line = message.ToString();
                File.WriteAllText(key, line);
            }
            lock (syncRoot)
            {
                if (messageQueue.Count < maxMessagesInMemory && keysQueue.Count == 0)
                {
                    messageQueue.Enqueue(message);
                }
                else
                {
                    keysQueue.Enqueue(key);
                }
            }
        }

        public void ReEnqueue(string key, T message)
        {
            lock (syncRoot)
            {
                if (messageQueue.Count < maxMessagesInMemory && keysQueue.Count == 0)
                {
                    messageQueue.Enqueue(message);
                }
                else
                {
                    keysQueue.Enqueue(key);
                }
            }
        }

        public int Count
        {
            get
            {
                lock (syncRoot)
                {
                    return messageQueue.Count + keysQueue.Count;
                }
            }
        }

        private void RefillCheck()
        {
            if (keysQueue.Count > 0 && messageQueue.Count < reorderLevel)
            {
                RefillMessageQueueAsync();
            }
        }

        private void RefillMessageQueueAsync()
        {
            if (reloading) return; //don't run more than one at a time
            Task.Factory.StartNew(() =>
            {
                reloading = true;
                while (true)
                {
                    lock (syncRoot)
                    {
                        try
                        {
                            var loadCount = 0;
                            //don't load more than ReorderQty per lock obtained
                            while (keysQueue.Count > 0
                                && loadCount < ReorderQty
                                && messageQueue.Count < maxMessagesInMemory)
                            {
                                var key = keysQueue.Dequeue();
                                var msg = loadFromFile(key);
                                messageQueue.Enqueue(msg);
                                loadCount++;
                            }
                        }
                        catch (Exception e)
                        {
                            reloadException = e;
                        }
                        if (keysQueue.Count == 0 || messageQueue.Count >= maxMessagesInMemory)
                        {
                            break;  //escape the refill loop
                        }
                    }
                }
                reloading = false;
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }
    }
}
