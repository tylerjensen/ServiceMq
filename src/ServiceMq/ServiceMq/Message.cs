using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    public class Message
    {
        public Guid Id { get; set; }
        public Address From { get; set; }
        public DateTime Sent { get; set; }
        public DateTime Received { get; set; }
        public int SendAttempt { get; set; }
        public string MessageTypeName { get; set; }
        public byte[] MessageBytes { get; set; }
        public string MessageString { get; set; }

        public T To<T>()
        {
            if (string.IsNullOrWhiteSpace(MessageString)) return default(T);
            return SvcStkTxt.TypeSerializer.DeserializeFromString<T>(MessageString);
        }
    }
}
