using System;
using System.Net;
using System.IO;
using Xunit;

namespace ServiceMq.Tests
{
    public class BasicTests
    {
        private readonly string _testFilesRoot = @"c:\temp\basic";

        [Fact] 
        public void SimpleTest()
        {
            var q1Address = new Address("q1pipe6");
            var q2Address = new Address("q2pipe6");
            using (var q2 = new MessageQueue("q2", q2Address, _testFilesRoot + @"\q2"))
            using (var q1 = new MessageQueue("q1", q1Address, _testFilesRoot + @"\q1"))
            {
                q1.Send(q2Address, "hello world");
                var msg = q2.Receive();
                Assert.NotNull(msg);
                Assert.Equal("hello world", msg.To<string>());
            }
        }

        [Fact]
        public void SimpleTestNoLog()
        {
            var q1Address = new Address("q1npipe7");
            var q2Address = new Address("q2npipe7");
            using (var q2 = new MessageQueue("qn2", q2Address, _testFilesRoot + @"\qn2", 
                persistMessagesReadLogs: false, persistMessagesSentLogs: false))
            using (var q1 = new MessageQueue("qn1", q1Address, _testFilesRoot + @"\qn1"))
            {
                q1.Send(q2Address, "hello world");
                var msg = q2.Receive();
                Assert.NotNull(msg);
                Assert.Equal("hello world", msg.To<string>());
            }
            var read = Directory.GetFiles(_testFilesRoot + @"\qn2\read", "*.log");
            var sent = Directory.GetFiles(_testFilesRoot + @"\qn2\sent", "*.log");
            Assert.True(read.Length == 0);
            Assert.True(sent.Length == 0);
            read = Directory.GetFiles(_testFilesRoot + @"\qn1\read", "*.log");
            sent = Directory.GetFiles(_testFilesRoot + @"\qn1\sent", "*.log");
            Assert.True(read.Length == 0);
            Assert.True(sent.Length > 0);
        }

        [Fact]
        public void SimpleTcpTest()
        {
            var q1Address = new Address(Dns.GetHostName(), 8967);
            var q2Address = new Address(Dns.GetHostName(), 8968);
            using (var q2 = new MessageQueue("q2", q2Address, _testFilesRoot + @"\q2"))
            using (var q1 = new MessageQueue("q1", q1Address, _testFilesRoot + @"\q1"))
            {
                q1.Send(q2Address, "hello world");
                var msg = q2.Receive();
                Assert.NotNull(msg);
                Assert.Equal("hello world", msg.To<string>());
            }
        }

        [Fact]
        public void SimpleObjectTest()
        {
            var q1Address = new Address("q6pipe8");
            var q2Address = new Address("q8pipe8");
            using (var q2 = new MessageQueue("q8", q2Address, _testFilesRoot + @"\q8"))
            using (var q1 = new MessageQueue("q6", q1Address, _testFilesRoot + @"\q6"))
            {
                int[] data = new int[] { 4, 8, 9, 24 };
                q1.Send(q2Address, data);
                Message msg = q2.Receive();
                Assert.NotNull(msg);
                var data2 = msg.To<int[]>();
                Assert.Equal(data[1], data2[1]);
            }
        }

        [Fact]
        public void SimpleBinaryTest()
        {
            var q1Address = new Address("q3pipe9");
            var q2Address = new Address("q4pipe9");
            using (var q2 = new MessageQueue("q4", q2Address, _testFilesRoot + @"\q4"))
            using (var q1 = new MessageQueue("q3", q1Address, _testFilesRoot + @"\q3"))
            {
                byte[] data = new byte[] { 4, 8, 9, 24 };
                q1.SendBytes(q2Address, data, "mybytestest");
                Message msg = null;
                while (true)
                {
                    msg = q2.Receive();
                    if (msg.MessageBytes != null) break;
                }
                Assert.NotNull(msg);
                Assert.Equal(4, msg.MessageBytes.Length);
                Assert.Equal((byte)9, msg.MessageBytes[2]);
                Assert.Equal("mybytestest", msg.MessageTypeName);
            }
        }
    }
}
