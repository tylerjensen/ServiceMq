using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ServiceWire.NamedPipes;
using ServiceWire.TcpIp;

namespace ServiceMq
{
    internal class OutboundQueue
    {
        private const string DtFormat = "yyyyMMddhhmmssfff";

        private readonly Queue<OutboundMessage> mq = new Queue<OutboundMessage>();
        private readonly Dictionary<string, Queue<OutboundMessage>> retryQueues = new Dictionary<string, Queue<OutboundMessage>>();
        private readonly string msgDir;
        private readonly string outDir;
        private readonly string sentDir;
        private readonly string failDir;
        private readonly string name;

        private ushort tcount = 0;
        private volatile bool continueProcessing = true;
        private ManualResetEvent outgoingMessageWaitHandle = new ManualResetEvent(false);

        public OutboundQueue(string name, string msgDir)
        {
            this.name = name;
            this.msgDir = msgDir;
            this.outDir = Path.Combine(msgDir, "out");
            this.sentDir = Path.Combine(msgDir, "sent");
            this.failDir = Path.Combine(msgDir, "fail");
            Directory.CreateDirectory(this.outDir);
            Directory.CreateDirectory(this.sentDir);
            Directory.CreateDirectory(this.failDir);

            //kick off sending thread
            Task.Factory.StartNew(SendMessages, TaskCreationOptions.LongRunning);

            //read from out to hydrate queue
            var list = new List<string>(Directory.GetFiles(this.outDir, "*.omq"));
            if (list.Count > 0)
            {
                list.Sort();
                foreach (var msgFile in list)
                {
                    var msg = OutboundMessage.ReadFromFile(msgFile);
                    if (null != msg) mq.Enqueue(msg);
                }
                outgoingMessageWaitHandle.Set();
            }
        }

        public void Stop()
        {
            continueProcessing = false;
            outgoingMessageWaitHandle.Set();
            outgoingMessageWaitHandle.Dispose();
        }

        public void Enqueue(OutboundMessage msg)
        {
            lock (mq)
            {
                //write to q file for that address
                //yyyyMMddhhmmssfff-16bit-from-to-ipport-or-pipename
                //20140324142165412-0415-010-042-024-155-08746-pipename.omq

                var fileName = string.Format("{0}-{1}-{2}.omq", 
                    msg.Sent.ToString(DtFormat), 
                    tcount.ToString("0000"),
                    msg.To.ToFileNameString());

                msg.Filename = Path.Combine(this.outDir, fileName);

                //idguid   address-from   address-to   senttimestamp   msgtypename   bin/str   message(base64forbin)
                var line = msg.ToLine();
                File.WriteAllText(msg.Filename, line);

                mq.Enqueue(msg);

                //increment and roll tcount - max 9999
                tcount = (tcount > 9999) ? (ushort)0 : (ushort)(tcount + 1);

                //signal item to send
                outgoingMessageWaitHandle.Set();
            }
        }

        private void SendMessages()
        {
            while (continueProcessing)
            {
                outgoingMessageWaitHandle.WaitOne();
                OutboundMessage message = null;
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
                        outgoingMessageWaitHandle.Reset();
                    }
                }
                var attemptTime = DateTime.Now;
                bool fromRegularQueue = true;
                if (null == message)
                {
                    //look for oldest in retry queues
                    var keyWithOldest = string.Empty;
                    var oldest = DateTime.Now;
                    foreach (var kvp in retryQueues)
                    {
                        if (kvp.Value.Count > 0)
                        {
                            var peekMsg = kvp.Value.Peek();
                            if (peekMsg.LastSendAttempt < oldest)
                            {
                                //only choosee if it is a viable retry candidate
                                var secondsSinceLast = (attemptTime - peekMsg.LastSendAttempt).TotalSeconds;
                                if (peekMsg.SendAttempts < secondsSinceLast)
                                {
                                    oldest = peekMsg.LastSendAttempt;
                                    keyWithOldest = kvp.Key;
                                }
                            }
                        }
                    }
                    if (keyWithOldest != string.Empty)
                    {
                        message = retryQueues[keyWithOldest].Dequeue();
                        fromRegularQueue = false;
                    }
                }

                if (null != message)
                {
                    //check to see if this message is being sent to an address that is failing
                    var dest = message.To.ToFileNameString();
                    if (fromRegularQueue && retryQueues.ContainsKey(dest) && retryQueues[dest].Count > 0)
                    {
                        //put regular mq msg onto retry queue to preserve order to that address
                        retryQueues[dest].Enqueue(message);

                        //set message to null and get oldest if it is time to retry - 
                        message = null;
                        var peekOld = retryQueues[dest].Peek();
                        //should attempt if diff is more than send attempts in seconds
                        var peekSeconds = (attemptTime - peekOld.LastSendAttempt).TotalSeconds;
                        if (peekOld.SendAttempts < peekSeconds)
                        {
                            message = peekOld;
                            fromRegularQueue = false;
                        }
                    }

                    //skip if message went onto failure heap
                    if (null != message)
                    {
                        //if attempts exceed X or time since sent, add to fail
                        message.LastSendAttempt = attemptTime;
                        message.SendAttempts++;
                        try
                        {
                            SendMessage(message);
                            LogSent(message);
                            if (!fromRegularQueue) retryQueues[dest].Dequeue(); //pulls peeked obj off as success
                        }
                        catch (Exception e)
                        {
                            if ((message.LastSendAttempt - message.Sent).TotalHours > 24.0)
                            {
                                LogFailed(message);
                                if (!fromRegularQueue) retryQueues[dest].Dequeue(); //pulls peeked obj off no more trying
                            }
                            else
                            {
                                if (fromRegularQueue)
                                {
                                    if (!retryQueues.ContainsKey(dest)) retryQueues.Add(dest, new Queue<OutboundMessage>());
                                    retryQueues[dest].Enqueue(message);
                                }
                            }
                        }
                    }
                }
            }
        }

        private void SendMessage(OutboundMessage message)
        {
            NpClient<IMessageService> npClient = null;
            TcpClient<IMessageService> tcpClient = null;
            IMessageService proxy = null;
            try
            {
                var useNpClient = false;
                if (message.To.Transport == Transport.Both)
                {
                    if (message.To.ServerName == message.From.ServerName)
                    {
                        useNpClient = true;
                    }
                }
                else if (message.To.Transport == Transport.Np) useNpClient = true;

                if (useNpClient)
                {
                    npClient = new NpClient<IMessageService>(new NpEndPoint(message.To.PipeName, 250));
                    proxy = npClient.Proxy;
                }
                else
                {
                    var tokenSource = new CancellationTokenSource();
                    var token = tokenSource.Token;
                    var task = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            tcpClient = new TcpClient<IMessageService>(new IPEndPoint(IPAddress.Parse(message.To.IpAddress), message.To.Port));
                            if (token.IsCancellationRequested)
                            {
                                    tcpClient.Dispose();
                                    tcpClient = null;
                            }
                        }
                        catch { }
                    }, token);

                    //force a timeout exception if not connected in 250ms
                    if (Task.WaitAll(new Task[] {task}, 250, token))
                    {
                        proxy = tcpClient.Proxy;
                    }
                    else
                    {
                        tokenSource.Cancel();
                        throw new TimeoutException("Could not connect in less than 250ms");
                    }
                }

                if (null == message.MessageBytes)
                {
                    proxy.EnqueueMessage(message.Id, message.From.ToString(), message.Sent, message.SendAttempts,
                         message.MessageTypeName, message.MessageString);
                }
                else
                {
                    proxy.EnqueueMessage(message.Id, message.From.ToString(), message.Sent, message.SendAttempts,
                        message.MessageTypeName, message.MessageBytes);
                }
            }
            finally
            {
                if (null != tcpClient) tcpClient.Dispose();
                if (null != npClient) npClient.Dispose();
            }
        }

        private const string DtLogFormat = "yyyyMMdd-hh";

        private void LogFailed(OutboundMessage message)
        {
            var fileName = string.Format("fail-{0}.log", DateTime.Now.ToString(DtLogFormat));
            var logFile = Path.Combine(this.failDir, fileName);
            var line = message.ToLine();
            File.AppendAllLines(logFile, new string[] { line });
            File.Delete(message.Filename);
        }

        private void LogSent(OutboundMessage message)
        {
            var fileName = string.Format("sent-{0}.log", DateTime.Now.ToString(DtLogFormat));
            var logFile = Path.Combine(this.sentDir, fileName);
            var line = message.ToLine();
            File.AppendAllLines(logFile, new string[] { line });
            File.Delete(message.Filename);
        }
    }
}
