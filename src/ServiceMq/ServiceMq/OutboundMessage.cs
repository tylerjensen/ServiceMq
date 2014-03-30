using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    internal class OutboundMessage
    {
        private const string DtFormat = "yyyyMMddhhmmssfff";

        public string Filename { get; set; }
        public Guid Id { get; set; }
        public Address From { get; set; }
        public Address To { get; set; }
        public DateTime Sent { get; set; }
        public string MessageTypeName { get; set; }
        public byte[] MessageBytes { get; set; }
        public string MessageString { get; set; }
        public int SendAttempts { get; set; }
        public DateTime LastSendAttempt { get; set; }

        public static OutboundMessage ReadFromFile(string fileName)
        {
            //idguid   address-from   address-to   senttimestamp   msgtypename   bin/str   message(base64forbin)
            var text = File.ReadAllText(fileName);
            var parts = text.Split('\t');
            if (parts.Length == 7)
            {
                var msg = new OutboundMessage()
                {
                    Filename = fileName,
                    Id = Guid.Parse(parts[0]),
                    From = Address.FromString(parts[1]),
                    To = Address.FromString(parts[2]),
                    Sent = DateTime.ParseExact(parts[3], DtFormat, DateTimeFormatInfo.InvariantInfo),
                    MessageTypeName = parts[4],
                    MessageString = parts[5] == "bin" ? null : parts[6],
                    MessageBytes = parts[5] != "bin" ? null : Convert.FromBase64String(parts[6])
                };
                return msg;
            }
            return null;
        }

        public string ToLine()
        {
            //idguid   address-from   address-to   senttimestamp   msgtypename   bin/str   message(base64forbin)
            var line = string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}",
                Id,
                From,
                To,
                Sent.ToString(DtFormat),
                MessageTypeName,
                MessageBytes == null ? "str" : "bin",
                MessageBytes == null ? MessageString : Convert.ToBase64String(MessageBytes));
            return line;
        }
    }
}
