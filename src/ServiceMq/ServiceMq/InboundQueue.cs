using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    internal class InboundQueue
    {
        private object syncRoot = new object();
        private readonly Queue<Message> mq = new Queue<Message>();
        private readonly string msgDir;
        private readonly string name;

        public InboundQueue(string name, string msgDir)
        {
            this.name = name;
            this.msgDir = msgDir;

            //read from log to hydrate queue
        }

        public void Enqueue(Message msg)
        {
            lock (syncRoot)
            {
                //write to q file
                mq.Enqueue(msg);

                //signal received
            }
        }

        public Message Receive(int timeout)
        {
            //when signaled
            //deque 
            //write to received/processed file/log
            //remove from q file
            return null; //msg
        }
    }
}
