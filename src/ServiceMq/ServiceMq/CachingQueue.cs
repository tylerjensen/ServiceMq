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
        private readonly int reorderQty;
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
            int maxMessagesInMemory, int reorderLevel, bool persistMessages = true)
        {
            this.msgDir = msgDir;
            this.loadFromFile = loadFromFile;
            this.maxMessagesInMemory = maxMessagesInMemory < 128
                ? 128
                : maxMessagesInMemory;
            this.reorderLevel = reorderLevel > this.maxMessagesInMemory
                ? this.maxMessagesInMemory / 2
                : reorderLevel < 64
                    ? 64
                    : reorderLevel;

            //don't want to read too many at one time
            this.reorderQty = this.reorderLevel > 2048
                ? 512
                : this.reorderLevel / 4;

            this.persistMessages = persistMessages;
            this.keysQueue = new Queue<string>(this.maxMessagesInMemory);
            this.messageQueue = new Queue<T>(this.maxMessagesInMemory);
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

        public IList<T> DequeueBulk(int maxMessagesToReceive)
        {
            lock (syncRoot)
            {
                if (messageQueue.Count > 0)
                {
                    var list = new List<T>(maxMessagesToReceive);
                    while (list.Count < maxMessagesToReceive && messageQueue.Count > 0)
                    {
                        var msg = messageQueue.Dequeue();
                        list.Add(msg);
                    }
                    RefillCheck();
                    return list;
                }
                return new List<T>();
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
                FastFile.WriteAllText(key, line);
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
                                && loadCount < reorderQty
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
                    Thread.Sleep(1); //allow lock competitor on dequeue to break in
                }
                reloading = false;
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }
    }
}
