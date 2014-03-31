using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

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
        private ManualResetEvent incomingMessageWaitHandle = new ManualResetEvent(false);
        private ushort tcount = 0;

        public InboundQueue(string name, string msgDir)
        {
            this.name = name;
            this.msgDir = msgDir;
            this.inDir = Path.Combine(msgDir, "in");
            this.readDir = Path.Combine(msgDir, "read");
            Directory.CreateDirectory(this.inDir);
            Directory.CreateDirectory(this.readDir);

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
                incomingMessageWaitHandle.Set();
            }
        }

        public Message Receive(int timeoutMs)
        {
            while (true)
            {
                if (incomingMessageWaitHandle.WaitOne(timeoutMs))
                {
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
                        LogRead(message);
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

        private const string DtLogFormat = "yyyyMMdd-HH";

        private void LogRead(Message message)
        {
            var fileName = string.Format("read-{0}.log", DateTime.Now.ToString(DtLogFormat));
            var logFile = Path.Combine(this.readDir, fileName);
            var line = message.ToLine();
            File.AppendAllLines(logFile, new string[] { line });
            File.Delete(message.Filename);
        }
    }
}
