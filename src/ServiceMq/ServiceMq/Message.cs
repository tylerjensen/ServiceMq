using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace ServiceMq
{
    public class Message
    {
        private const string DtFormat = "yyyyMMddHHmmssfff";

        internal string Filename { get; set; }
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

        //id   from   sentts   receivedts   sentattempts   msgtypename   bin/str   message(binbase64)
        public override string ToString()
        {
            return string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}",
                Id,
                From,
                Sent.ToString(DtFormat),
                Received.ToString(DtFormat),
                SendAttempt,
                MessageTypeName,
                MessageBytes == null ? "str" : "bin",
                MessageBytes == null ? MessageString : Convert.ToBase64String(MessageBytes));
        }

        public static Message ReadFromFile(string fileName)
        {
            //id   from   sentts   receivedts   sentattempts   msgtypename   bin/str   message(binbase64)
            var text = File.ReadAllText(fileName);
            var parts = text.Split('\t');
            if (parts.Length == 8)
            {
                var msg = new Message()
                {
                    Filename = fileName,
                    Id = Guid.Parse(parts[0]),
                    From = Address.FromString(parts[1]),
                    Sent = DateTime.ParseExact(parts[2], DtFormat, DateTimeFormatInfo.InvariantInfo),
                    Received = DateTime.ParseExact(parts[3], DtFormat, DateTimeFormatInfo.InvariantInfo),
                    SendAttempt = Convert.ToInt32(parts[4]),
                    MessageTypeName = parts[5],
                    MessageString = parts[6] == "bin" ? null : parts[7],
                    MessageBytes = parts[6] != "bin" ? null : Convert.FromBase64String(parts[7])
                };
                return msg;
            }
            return null;
        }


    }
}
