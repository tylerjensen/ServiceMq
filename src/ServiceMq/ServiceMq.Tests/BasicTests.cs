using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
                Assert.AreEqual(msg.MessageString, "hello world");
            }
        }
    }
}
