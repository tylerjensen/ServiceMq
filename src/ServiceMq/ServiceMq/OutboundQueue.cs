using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    internal class OutboundQueue
    {
        private object syncRoot = new object();
        private readonly Queue<OutboundMessage> mq = new Queue<OutboundMessage>();
        private readonly string msgDir;
        private readonly string name;

        public OutboundQueue(string name, string msgDir)
        {
            this.name = name;
            this.msgDir = msgDir;

            //read from log to hydrate queue
        }

        public void Enqueue(OutboundMessage msg)
        {
            lock (syncRoot)
            {
                //write to q file for that address
                mq.Enqueue(msg);

                //signal item to send
            }
        }

        public void Send()
        {
            //when signaled
            //deque 
            //send try
                //success write to sent and remove from q
                //fail write to fail queue and try later 
                    //(by to address to all messages to that address in order)
                    //remove from q file
        }
    }
}
