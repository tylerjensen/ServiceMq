using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    internal class OutboundMessage
    {
        public Guid Id { get; set; }
        public Address From { get; set; }
        public Address To { get; set; }
        public DateTime Sent { get; set; }
        public string MessageTypeName { get; set; }
        public byte[] MessageBytes { get; set; }
        public string MessageString { get; set; }
    }
}
