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
        private const string DtFormat = "yyyyMMddHHmmssfff";

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

        public static OutboundMessage ReadFromFile(string fileName, FastFile fastFile)
        {
            //idguid   address-from   address-to   senttimestamp   msgtypename   bin/str   message(base64forbin)
            var text = fastFile.ReadAllText(fileName);
            return Deserialize(fileName, text);
        }

        internal static OutboundMessage Deserialize(string key, string text)
        {
            var parts = text.Split('\t');
            if (parts.Length == 10 && parts[0] == "v2")
            {
                return new OutboundMessage
                {
                    Filename = key,
                    Id = Guid.Parse(parts[1]),
                    From = Address.FromString(Decode(parts[2])),
                    To = Address.FromString(Decode(parts[3])),
                    Sent = DateTime.FromBinary(Convert.ToInt64(parts[4], CultureInfo.InvariantCulture)),
                    MessageTypeName = Decode(parts[5]),
                    MessageString = parts[6] == "bin" ? null : Decode(parts[7]),
                    MessageBytes = parts[6] != "bin" ? null : Convert.FromBase64String(parts[7]),
                    SendAttempts = Convert.ToInt32(parts[8], CultureInfo.InvariantCulture),
                    LastSendAttempt = DateTime.FromBinary(Convert.ToInt64(parts[9], CultureInfo.InvariantCulture))
                };
            }
            if (parts.Length == 7)
            {
                var msg = new OutboundMessage()
                {
                    Filename = key,
#if (!NET35)
                    Id = Guid.Parse(parts[0]),
#else
                    Id = new Guid(parts[0]),
#endif
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

        public override string ToString()
        {
            //idguid   address-from   address-to   senttimestamp   msgtypename   bin/str   message(base64forbin)
            var line = string.Format("v2\t{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}",
                Id,
                Encode(From.ToString()),
                Encode(To.ToString()),
                Sent.ToBinary(),
                Encode(MessageTypeName),
                MessageBytes == null ? "str" : "bin",
                MessageBytes == null ? Encode(MessageString) : Convert.ToBase64String(MessageBytes),
                SendAttempts,
                LastSendAttempt.ToBinary());
            return line;
        }

        private static string Encode(string value)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(value ?? string.Empty));
        }

        private static string Decode(string value)
        {
            return Encoding.UTF8.GetString(Convert.FromBase64String(value));
        }
    }
}
