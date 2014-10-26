using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    internal class InboundQueue 
    {
        private const string DtFormat = "yyyyMMddHHmmssfff";
        private readonly Queue<Message> mq = new Queue<Message>();
        private readonly string msgDir;
        private readonly string inDir;
        private readonly string readDir;
        private readonly string name;
        private readonly double hoursReadSentLogsToLive;
        private volatile bool continueProcessing = true;
        private ManualResetEvent incomingMessageWaitHandle = new ManualResetEvent(false);
        private ushort tcount = 0;
        private DateTime lastCleaned = DateTime.Now.AddDays(-10);
        private Exception stateException = null;
        private QueueState state = QueueState.Running;

        public InboundQueue(string name, string msgDir, double hoursReadSentLogsToLive)
        {
            this.name = name;
            this.msgDir = msgDir;
            this.hoursReadSentLogsToLive = hoursReadSentLogsToLive;
            this.inDir = Path.Combine(msgDir, "in");
            this.readDir = Path.Combine(msgDir, "read");
            Directory.CreateDirectory(this.inDir);
            Directory.CreateDirectory(this.readDir);

            try
            {
                //read from in to hydrate queue
                var list = new List<string>(Directory.GetFiles(this.inDir, "*.imq"));
                if (list.Count > 0)
                {
                    list.Sort();
                    foreach (var msgFile in list)
                    {
                        var msg = Message.ReadFromFile(msgFile);
                        if (null != msg) mq.Enqueue(msg);
                    }
                    incomingMessageWaitHandle.Set();
                }
            }
            catch (Exception e)
            {
                this.stateException = e;
                this.state = QueueState.Failed;
                throw;
            }
        }

        public int Count
        {
            get
            {
                lock (mq)
                {
                    return mq.Count;
                }
            }
        }

        public Exception StateException
        {
            get { return stateException; }
        }

        public QueueState State
        {
            get { return state; }
        }

        public void ClearState()
        {
            stateException = null;
            state = QueueState.Running;
        }

        public void Stop()
        {
            try
            {
                continueProcessing = false;
                incomingMessageWaitHandle.Set();
                incomingMessageWaitHandle.Dispose();
            }
            catch (Exception e)
            {
                this.stateException = e;
                this.state = QueueState.Failed;
                throw;
            }
        }

        public void Enqueue(Message msg)
        {
            lock (mq)
            {
                //write to q file for that address
                //yyyyMMddHHmmssfff-16bit-from-to-ipport-or-pipename
                //20140324142165412-0415-010-042-024-155-08746-pipename.omq

                var fileName = string.Format("{0}-{1}-{2}.imq",
                    msg.Sent.ToString(DtFormat),
                    tcount.ToString("0000"),
                    msg.From.ToFileNameString());

                msg.Filename = Path.Combine(this.inDir, fileName);

                //idguid   address-from   address-to   senttimestamp   msgtypename   bin/str   message(base64forbin)
                var line = msg.ToLine();
                File.WriteAllText(msg.Filename, line);

                mq.Enqueue(msg);

                //increment and roll tcount - max 9999
                tcount = (tcount > 9999) ? (ushort)0 : (ushort)(tcount + 1);

                //signal received
                if (continueProcessing) incomingMessageWaitHandle.Set();
            }
        }

        public void ReEnqueue(Message message)
        {
            lock (mq)
            {
                mq.Enqueue(message);
            }
        }

        public Message Receive(int timeoutMs, bool logRead = true)
        {
            while (continueProcessing)
            {
                if (incomingMessageWaitHandle.WaitOne(timeoutMs))
                {
                    if (!continueProcessing) break;
                    Message message = null;
                    lock (mq)
                    {
                        if (mq.Count > 0)
                        {
                            message = mq.Dequeue();
                            //attempt to prevent excessive memory footprint
                            if (mq.Count > 1000) mq.TrimExcess();
                        }
                        else
                        {
                            //set to nonsignaled and block on WaitOne again
                            incomingMessageWaitHandle.Reset();
                        }
                    }

                    if (null != message)
                    {
                        if (logRead) LogRead(message);
                        return message;
                    }
                }
                else
                {
                    break; //timedout
                }
            }
            return null;
        }

        private const string DtLogFormat = "yyyyMMdd-HH-mm";

        private void LogRead(Message message)
        {
            try
            {
                var fileName = string.Format("read-{0}.log", DateTime.Now.ToString(DtLogFormat));
                var logFile = Path.Combine(this.readDir, fileName);
                var line = message.ToLine().ToFlatLine();
                File.AppendAllLines(logFile, new string[] { line });
                File.Delete(message.Filename);
            }
            catch (Exception e)
            {
                this.stateException = e;
                this.state = QueueState.Cautioned;
            }

            //cleanup every two hours
            if ((DateTime.Now - lastCleaned).TotalHours > 2.0)
            {
                lastCleaned = DateTime.Now;
                Task.Factory.StartNew(() =>
                {
                    try
                    {
                        var files = Directory.GetFiles(this.readDir);
                        foreach (var file in files)
                        {
                            var info = new FileInfo(file);
                            if ((DateTime.Now - info.LastWriteTime).TotalHours > this.hoursReadSentLogsToLive)
                            {
                                File.Delete(file);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        this.stateException = e;
                        this.state = QueueState.Cautioned;
                    }
                }, TaskCreationOptions.LongRunning);
            }
        }

        public void Acknowledge(Message message)
        {
            LogRead(message);
        }
    }
}
