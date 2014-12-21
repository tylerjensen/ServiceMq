using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    public interface IMessageService
    {
        int EnqueueString(Guid id, string from, DateTime sentTime, int sendAttempt, string messageTypeName, string message);
        int EnqueueBytes(Guid id, string from, DateTime sentTime, int sendAttempt, string messageTypeName, byte[] message);
    }

    internal class MessageService : IMessageService
    {
        private readonly InboundQueue inboundQueue;

        public MessageService(InboundQueue inboundQueue)
        {
            this.inboundQueue = inboundQueue;
        }

        public int EnqueueString(Guid id, string from, DateTime sentTime, int sendAttempt, string messageTypeName, string message)
        {
#if (!NET35)
            if (string.IsNullOrWhiteSpace(message)) return 0;
#else
            if (string.IsNullOrEmpty(message)) return 0;
#endif
            var msg = new Message
            {
                Id = id,
                From = Address.FromString(from),
                Sent = sentTime,
                Received = DateTime.Now,
                SendAttempt = sendAttempt,
                MessageTypeName = messageTypeName,
                MessageString = message
            };
            inboundQueue.Enqueue(msg);
            return message.Length;
        }

        public int EnqueueBytes(Guid id, string from, DateTime sentTime, int sendAttempt, string messageTypeName, byte[] message)
        {
            if (null == message || message.Length == 0) return 0;
            var msg = new Message
            {
                Id = id,
                From = Address.FromString(from),
                Sent = sentTime,
                Received = DateTime.Now,
                SendAttempt = sendAttempt,
                MessageTypeName = messageTypeName,
                MessageBytes = message
            };
            inboundQueue.Enqueue(msg);
            return message.Length;
        }
    }
}
