using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    internal interface IMessageService
    {
        int EnqueueMessage(Guid id, string from, DateTime sentTime, string messageTypeName, string message);
        int EnqueueMessage(Guid id, string from, DateTime sentTime, string messageTypeName, byte[] message);
    }

    internal class MessageService : IMessageService
    {
        private readonly InboundQueue inboundQueue;

        public MessageService(InboundQueue inboundQueue)
        {
            this.inboundQueue = inboundQueue;
        }

        public int EnqueueMessage(Guid id, string from, DateTime sentTime, string messageTypeName, string message)
        {
            if (string.IsNullOrWhiteSpace(message)) return 0;
            var msg = new Message
            {
                Id = id,
                From = Address.FromString(from),
                Sent = sentTime,
                Received = DateTime.Now,
                MessageTypeName = messageTypeName,
                MessageString = message
            };
            inboundQueue.Enqueue(msg);
            return message.Length;
        }

        public int EnqueueMessage(Guid id, string from, DateTime sentTime, string messageTypeName, byte[] message)
        {
            if (null == message || message.Length == 0) return 0;
            var msg = new Message
            {
                Id = id,
                From = Address.FromString(from),
                Sent = sentTime,
                Received = DateTime.Now,
                MessageTypeName = messageTypeName,
                MessageBytes = message
            };
            inboundQueue.Enqueue(msg);
            return message.Length;
        }
    }
}
