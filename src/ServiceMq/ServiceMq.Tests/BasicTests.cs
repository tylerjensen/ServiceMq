using System;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;

namespace ServiceMq.Tests
{
    [TestClass]
    public class BasicTests
    {
        [TestMethod] 
        public void SimpleTest()
        {
            var q1Address = new Address("q1pipe");
            var q2Address = new Address("q2pipe");
            using (var q2 = new MessageQueue("q2", q2Address, @"c:\temp\q2"))
            using (var q1 = new MessageQueue("q1", q1Address, @"c:\temp\q1"))
            {
                q1.Send(q2Address, "hello world");
                var msg = q2.Receive();
                Assert.IsNotNull(msg);
                Assert.AreEqual(msg.To<string>(), "hello world");
            }
        }

        [TestMethod]
        public void SimpleTestNoLog()
        {
            var q1Address = new Address("q1npipe");
            var q2Address = new Address("q2npipe");
            using (var q2 = new MessageQueue("qn2", q2Address, @"c:\temp\qn2", 
                persistMessagesReadLogs: false, persistMessagesSentLogs: false))
            using (var q1 = new MessageQueue("qn1", q1Address, @"c:\temp\qn1"))
            {
                q1.Send(q2Address, "hello world");
                var msg = q2.Receive();
                Assert.IsNotNull(msg);
                Assert.AreEqual(msg.To<string>(), "hello world");
            }
            var read = Directory.GetFiles(@"c:\temp\qn2\read", "*.log");
            var sent = Directory.GetFiles(@"c:\temp\qn2\sent", "*.log");
            Assert.IsTrue(read.Length == 0);
            Assert.IsTrue(sent.Length == 0);
            read = Directory.GetFiles(@"c:\temp\qn1\read", "*.log");
            sent = Directory.GetFiles(@"c:\temp\qn1\sent", "*.log");
            Assert.IsTrue(read.Length == 0);
            Assert.IsTrue(sent.Length > 0);
        }

        [TestMethod]
        public void SimpleTcpTest()
        {
            var q1Address = new Address(Dns.GetHostName(), 8967);
            var q2Address = new Address(Dns.GetHostName(), 8968);
            using (var q2 = new MessageQueue("q2", q2Address, @"c:\temp\q2"))
            using (var q1 = new MessageQueue("q1", q1Address, @"c:\temp\q1"))
            {
                q1.Send(q2Address, "hello world");
                var msg = q2.Receive();
                Assert.IsNotNull(msg);
                Assert.AreEqual(msg.To<string>(), "hello world");
            }
        }

        [TestMethod]
        public void SimpleObjectTest()
        {
            var q1Address = new Address("q6pipe");
            var q2Address = new Address("q8pipe");
            using (var q2 = new MessageQueue("q8", q2Address, @"c:\temp\q8"))
            using (var q1 = new MessageQueue("q6", q1Address, @"c:\temp\q6"))
            {
                int[] data = new int[] { 4, 8, 9, 24 };
                q1.Send(q2Address, data);
                Message msg = q2.Receive();
                Assert.IsNotNull(msg);
                var data2 = msg.To<int[]>();
                Assert.AreEqual(data[1], data2[1]);
            }
        }

        [TestMethod]
        public void SimpleBinaryTest()
        {
            var q1Address = new Address("q3pipe");
            var q2Address = new Address("q4pipe");
            using (var q2 = new MessageQueue("q4", q2Address, @"c:\temp\q4"))
            using (var q1 = new MessageQueue("q3", q1Address, @"c:\temp\q3"))
            {
                byte[] data = new byte[] { 4, 8, 9, 24 };
                q1.SendBytes(q2Address, data, "mybytestest");
                Message msg = null;
                while (true)
                {
                    msg = q2.Receive();
                    if (msg.MessageBytes != null) break;
                }
                Assert.IsNotNull(msg);
                Assert.AreEqual(msg.MessageBytes.Length, 4);
                Assert.AreEqual(msg.MessageBytes[2], (byte)9);
                Assert.AreEqual(msg.MessageTypeName, "mybytestest");
            }
        }

    }
}
