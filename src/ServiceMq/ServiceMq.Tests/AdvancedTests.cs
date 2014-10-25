﻿using System;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;

namespace ServiceMq.Tests
{
    [TestClass]
    public class AdvancedTests
    {
        [TestMethod]
        public void MultiLineTest()
        {
            var q1Address = new Address("qm1pipe");
            var q2Address = new Address("qm2pipe");
            using (var q2 = new MessageQueue("qm2", q2Address, @"c:\temp\qm2"))
            using (var q1 = new MessageQueue("qm1", q1Address, @"c:\temp\qm1"))
            {
                q1.Send(q2Address, "hello\r\nworld");
                var msg = q2.Receive();
                Assert.IsNotNull(msg);
                Assert.AreEqual(msg.MessageString, "hello\r\nworld");
            }
        }

        [TestMethod]
        public void DestDownTest()
        {
            var q1Address = new Address("qd1pipe");
            var q2Address = new Address("qd2pipe");
            using (var q1 = new MessageQueue("qd1", q1Address, @"c:\temp\qd1"))
            {
                q1.Send(q2Address, "hello world 1");
                Thread.Sleep(200); //destination not available
                q1.Send(q2Address, "hello world 2");
                using (var q2 = new MessageQueue("qd2", q2Address, @"c:\temp\qd2"))
                {
                    var msg = q2.Receive();
                    Assert.IsNotNull(msg);
                    Assert.AreEqual(msg.MessageString, "hello world 1");
                    msg = q2.Receive();
                    Assert.IsNotNull(msg);
                    Assert.AreEqual(msg.MessageString, "hello world 2");
                }
            }
        }

        [TestMethod]
        public void BroadcastTest()
        {
            var q1Address = new Address("qb1pipe");
            var q2Address = new Address("qb2pipe");
            var q3Address = new Address("qb3pipe");
            var q4Address = new Address("qb4pipe");
            using (var q4 = new MessageQueue("qb4", q4Address, @"c:\temp\qb4"))
            using (var q3 = new MessageQueue("qb3", q3Address, @"c:\temp\qb3"))
            using (var q2 = new MessageQueue("qb2", q2Address, @"c:\temp\qb2"))
            using (var q1 = new MessageQueue("qb1", q1Address, @"c:\temp\qb1"))
            {
                q1.Broadcast(new [] 
                    { 
                        q2Address, 
                        q3Address,
                        q4Address
                    }, "hello\r\nworld");
                var msg2 = q2.Receive();
                Assert.IsNotNull(msg2);
                Assert.AreEqual(msg2.MessageString, "hello\r\nworld");
                var msg3 = q3.Receive();
                Assert.IsNotNull(msg3);
                Assert.AreEqual(msg3.MessageString, "hello\r\nworld");
                var msg4 = q4.Receive();
                Assert.IsNotNull(msg4);
                Assert.AreEqual(msg4.MessageString, "hello\r\nworld");

                Assert.AreEqual(msg2.Id, msg3.Id);
                Assert.AreEqual(msg3.Id, msg4.Id);
                Assert.AreEqual(msg2.Sent, msg3.Sent);
                Assert.AreEqual(msg3.Sent, msg4.Sent);
            }
        }
    }
}