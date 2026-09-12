using System;
using System.Net;
using System.Threading;
using System.IO;
using Xunit;

namespace ServiceMq.Tests
{
    public class AdvancedTests
    {
        private readonly string _testFilesRoot = Path.Combine(Path.GetTempPath(), "ServiceMq.Tests", "advanced");

        [Fact]
        public void MultiLineTest()
        {
            var q1Address = new Address("qm1pipe1");
            var q2Address = new Address("qm2pipe1");
            using (var q2 = new MessageQueue("qm2", q2Address, _testFilesRoot + @"\qm2"))
            using (var q1 = new MessageQueue("qm1", q1Address, _testFilesRoot + @"\qm1"))
            {
                q1.Send(q2Address, "hello\r\nworld");
                var msg = q2.Receive();
                Assert.NotNull(msg);
                Assert.Equal("hello\r\nworld", msg.To<string>());
            }
        }

        [Fact]
        public void DestDownTest()
        {
            var q1Address = new Address("qd1pipe2");
            var q2Address = new Address("qd2pipe2");
            using (var q1 = new MessageQueue("qd1", q1Address, _testFilesRoot + @"\qd1"))
            {
                q1.Send(q2Address, "hello world 1");
                Settle(200); //destination not available
                q1.Send(q2Address, "hello world 2");
                using (var q2 = new MessageQueue("qd2", q2Address, _testFilesRoot + @"\qd2"))
                {
                    var msg = q2.Receive();
                    Assert.NotNull(msg);
                    Assert.Equal("hello world 1", msg.To<string>());
                    msg = q2.Receive();
                    Assert.NotNull(msg);
                    Assert.Equal("hello world 2", msg.To<string>());
                }
            }
        }

        [Fact]
        public void BroadcastTest()
        {
            var q1Address = new Address("qb1pipe3");
            var q2Address = new Address("qb2pipe3");
            var q3Address = new Address("qb3pipe3");
            var q4Address = new Address("qb4pipe3");
            using (var q4 = new MessageQueue("qb4", q4Address, _testFilesRoot + @"\qb4"))
            using (var q3 = new MessageQueue("qb3", q3Address, _testFilesRoot + @"\qb3"))
            using (var q2 = new MessageQueue("qb2", q2Address, _testFilesRoot + @"\qb2"))
            using (var q1 = new MessageQueue("qb1", q1Address, _testFilesRoot + @"\qb1"))
            {
                q1.Broadcast(new [] 
                    { 
                        q2Address, 
                        q3Address,
                        q4Address
                    }, "hello\r\nworld");
                var msg2 = q2.Receive();
                Assert.NotNull(msg2);
                Assert.Equal("hello\r\nworld", msg2.To<string>());
                var msg3 = q3.Receive();
                Assert.NotNull(msg3);
                Assert.Equal("hello\r\nworld", msg3.To<string>());
                var msg4 = q4.Receive();
                Assert.NotNull(msg4);
                Assert.Equal("hello\r\nworld", msg4.To<string>());

                Assert.Equal(msg2.Id, msg3.Id);
                Assert.Equal(msg3.Id, msg4.Id);
                Assert.Equal(msg2.Sent, msg3.Sent);
                Assert.Equal(msg3.Sent, msg4.Sent);
            }
        }

        [Fact]
        public void FlashDestDownTest()
        {
            if (Directory.Exists(_testFilesRoot + @"\qfa1")) Directory.Delete(_testFilesRoot + @"\qfa1", true);
            if (Directory.Exists(_testFilesRoot + @"\qfa2")) Directory.Delete(_testFilesRoot + @"\qfa2", true);

            var qfrom = new Address("qfaFrom4");
            var q1Address = new Address("qfa1pipe4");
            var q2Address = new Address("qfa2pipe4");
            using (var flash = new Flasher(qfrom))
            {
                using (var q2 = new MessageQueue("qfa2", q2Address, _testFilesRoot + @"\qfa2"))
                {
                    var id = flash.Send(q1Address, "my test message", q2Address);
                    var msg = q2.Receive();
                    Assert.True(msg.Id == id);
                }

                using (var q1 = new MessageQueue("qfa1", q1Address, _testFilesRoot + @"\qfa1"))
                {
                    var id = flash.Send(q1Address, "my test message", q2Address);
                    var msg = q1.Receive();
                    Assert.True(msg.Id == id);
                }

                try
                {
                    var id = flash.Send(q1Address, "my test message", q2Address);
                }
                catch (Exception e)
                {
                    Assert.True(e is System.Net.WebException);
                }
            }
        }

        [Fact]
        public void FlashDestDownTcpTest()
        {
            if (Directory.Exists(_testFilesRoot + @"\qf1")) Directory.Delete(_testFilesRoot + @"\qf1", true);
            if (Directory.Exists(_testFilesRoot + @"\qf2")) Directory.Delete(_testFilesRoot + @"\qf2", true);

            var qfrom = new Address(Dns.GetHostName(), 8976);
            var q1Address = new Address(Dns.GetHostName(), 8977);
            var q2Address = new Address(Dns.GetHostName(), 8978);

            using (var flash = new Flasher(qfrom))
            {
                using (var q2 = new MessageQueue("qf2", q2Address, _testFilesRoot + @"\qf2"))
                {
                    var id = flash.Send(q1Address, "my test message", q2Address);
                    var msg = q2.Receive();
                    Assert.True(msg.Id == id);
                }

                Settle(200);

                using (var q1 = new MessageQueue("qf1", q1Address, _testFilesRoot + @"\qf1"))
                {
                    var id = flash.Send(q1Address, "my test message", q2Address);
                    var msg = q1.Receive();
                    Assert.True(msg.Id == id);
                }

                try
                {
                    var id = flash.Send(q1Address, "my test message", q2Address);
                }
                catch (Exception e)
                {
                    Assert.True(e is System.Net.WebException);
                }
            }
        }

        [Fact]
        public void SimpleBulkReceiveTest()
        {
            var q1Address = new Address("qbr1pipe5");
            var q2Address = new Address("qbr2pipe5");
            using (var q2 = new MessageQueue("qbr2", q2Address, _testFilesRoot + @"\qbr2"))
            using (var q1 = new MessageQueue("qbr1", q1Address, _testFilesRoot + @"\qbr1"))
            {
                q1.Send(q2Address, "hello world 1");
                q1.Send(q2Address, "hello world 2");
                q1.Send(q2Address, "hello world 3");
                q1.Send(q2Address, "hello world 4");
                WaitUntil(() => q2.CountInbound == 4, 3000);
                var msg = q2.ReceiveBulk(2);
                Assert.NotNull(msg);
                Assert.Equal(2, msg.Count);
                Assert.Equal("hello world 1", msg[0].To<string>());
                Assert.Equal("hello world 2", msg[1].To<string>());
                WaitUntil(() => q2.CountInbound == 2, 3000);
                msg = q2.ReceiveBulk(2);
                Assert.NotNull(msg);
                Assert.Equal(2, msg.Count);
                Assert.Equal("hello world 3", msg[0].To<string>());
                Assert.Equal("hello world 4", msg[1].To<string>());
            }
        }

        private static bool WaitUntil(Func<bool> condition, int timeoutMs)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (condition()) return true;
                Thread.Sleep(10);
            }
            return condition();
        }

        private static void Settle(int milliseconds)
        {
            // A fixed settle period is required here: the async delivery attempt to a
            // destination that is down has no externally observable state to poll for, so
            // we wait for the retry to be scheduled before proceeding.
            Thread.Sleep(milliseconds);
        }
    }
}
