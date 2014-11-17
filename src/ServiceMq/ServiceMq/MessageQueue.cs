using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using ServiceWire;
using ServiceWire.NamedPipes;
using ServiceWire.TcpIp;

namespace ServiceMq
{
    public class MessageQueue : IDisposable
    {
        private readonly Address address;
        private readonly NpEndPoint npEndPoint = null;
        private readonly IPEndPoint ipEndPoint = null;
        private readonly string msgDir = null;
        private readonly string name = null;
        private readonly OutboundQueue outboundQueue = null;
        private readonly InboundQueue inboundQueue = null;
        private readonly IMessageService messageService = null;
        private readonly NpHost npHost = null;
        private readonly TcpHost tcpHost = null;
        private readonly int connectTimeOutMs = 500;
        private readonly int maxMessagesInMemory;
        private readonly int reorderLevel;

        private Exception stateExceptionOutbound = null;
        private QueueState stateOutbound = QueueState.Running;
        private Exception stateExceptionInbound = null;
        private QueueState stateInbound = QueueState.Running;

        public MessageQueue(string name, 
            Address address, 
            string msgDir = null, 
            ILog log = null, 
            IStats stats = null,
            double hoursReadSentLogsToLive = 48.0,
            int connectTimeOutMs = 500,
            bool persistMessagesSentLogs = true,
            bool persistMessagesReadLogs = true,
            int maxMessagesInMemory = 8192, int reorderLevel = 4096)
        {
            this.name = name;
            this.address = address;
            this.msgDir = msgDir ?? GetExecutablePathDirectory();
            this.maxMessagesInMemory = maxMessagesInMemory;
            this.reorderLevel = reorderLevel;

            Directory.CreateDirectory(this.msgDir);

            this.connectTimeOutMs = connectTimeOutMs;

            //create inbound and outbound queues
            this.outboundQueue = new OutboundQueue(this.name, this.msgDir, 
                hoursReadSentLogsToLive, connectTimeOutMs, persistMessagesSentLogs, 
                maxMessagesInMemory, reorderLevel);
            this.inboundQueue = new InboundQueue(this.name, this.msgDir, 
                hoursReadSentLogsToLive, persistMessagesReadLogs, 
                maxMessagesInMemory, reorderLevel);

            //create message service singleton
            this.messageService = new MessageService(this.inboundQueue);

            //create and open hosts
            if (this.address.Transport == Transport.Both || this.address.Transport == Transport.Tcp)
            {
                this.ipEndPoint = new IPEndPoint(IPAddress.Parse(this.address.IpAddress), this.address.Port);
                this.tcpHost = new TcpHost(this.ipEndPoint, log, stats);
                this.tcpHost.AddService<IMessageService>(this.messageService);
                this.tcpHost.Open();
            }

            if (this.address.Transport == Transport.Both || this.address.Transport == Transport.Np)
            {
                this.npEndPoint = new NpEndPoint(this.address.ServerName, this.address.PipeName);
                this.npHost = new NpHost(this.npEndPoint.PipeName, log, stats);
                this.npHost.AddService<IMessageService>(this.messageService);
                this.npHost.Open();
            }
        }

        public long CountOutbound
        {
            get
            {
                return outboundQueue.Count;
            }
        }

        public int CountInbound
        {
            get
            {
                return inboundQueue.Count;
            }
        }

        public Exception StateExceptionOutbound { get { return stateExceptionOutbound; } }
        public QueueState StateOutbound { get { return stateOutbound; } }
        public Exception StateExceptionInbound { get { return stateExceptionInbound; } }
        public QueueState StateInbound { get { return stateInbound; } }

        public void ClearState()
        {
            this.inboundQueue.ClearState();
            this.outboundQueue.ClearState();
        }

        public Guid Send<T>(Address dest, T message)
        {
            var addr = GetOptimalAddress(dest);
            string msg = SvcStkTxt.TypeSerializer.SerializeToString(message);
            return SendMsg(msg, typeof(T).FullName, addr);
        }

        public Guid Send(Address dest, string messageType, string message)
        {
            var addr = GetOptimalAddress(dest);
            return SendMsg(message, messageType, addr);
        }

        public Guid SendBytes(Address dest, byte[] message, string messageType)
        {
            var addr = GetOptimalAddress(dest);
            return SendMsg(message, messageType, addr);
        }

        private Guid SendMsg(string msg, string messageType, Address dest)
        {
            if (this.outboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Outbound queue exception state. See inner exception.", 
                    this.outboundQueue.StateException);
            }
            var message = new OutboundMessage()
            {
                From = this.address,
                To = dest,
                Id = Guid.NewGuid(),
                MessageString = msg,
                MessageTypeName = messageType,
                Sent = DateTime.Now
            };
            this.outboundQueue.Enqueue(message);
            return message.Id;
        }

        private Guid SendMsg(byte[] msg, string messageType, Address dest)
        {
            if (this.outboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Outbound queue exception state. See inner exception.", 
                    this.outboundQueue.StateException);
            }
            var message = new OutboundMessage()
            {
                From = this.address,
                To = dest,
                Id = Guid.NewGuid(),
                MessageBytes = msg,
                MessageTypeName = messageType,
                Sent = DateTime.Now
            };
            this.outboundQueue.Enqueue(message);
            return message.Id;
        }

        public Guid Broadcast<T>(IEnumerable<Address> dests, T message)
        {
            var addrs = new List<Address>();
            foreach(var dAddr in dests)
            {
                addrs.Add(GetOptimalAddress(dAddr));
            }
            string msg = SvcStkTxt.TypeSerializer.SerializeToString(message);
            return BroadcastMsg(msg, typeof(T).FullName, addrs);
        }

        public Guid Broadcast(IEnumerable<Address> dests, string messageType, string message)
        {
            var addrs = new List<Address>();
            foreach (var dAddr in dests)
            {
                addrs.Add(GetOptimalAddress(dAddr));
            }
            return BroadcastMsg(message, messageType, addrs);
        }

        public Guid BroadcastBytes(IEnumerable<Address> dests, byte[] message, string messageType)
        {
            var addrs = new List<Address>();
            foreach (var dAddr in dests)
            {
                addrs.Add(GetOptimalAddress(dAddr));
            }
            return BroadcastMsg(message, messageType, addrs);
        }

        private Guid BroadcastMsg(string msg, string messageType, IEnumerable<Address> dests)
        {
            if (this.outboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Outbound queue exception state. See inner exception.", 
                    this.outboundQueue.StateException);
            }
            var id = Guid.NewGuid();
            var now = DateTime.Now;
            foreach (var dest in dests)
            {
                var message = new OutboundMessage()
                {
                    From = this.address,
                    To = dest,
                    Id = id,
                    MessageString = msg,
                    MessageTypeName = messageType,
                    Sent = now
                };
                this.outboundQueue.Enqueue(message);
            }
            return id;
        }

        private Guid BroadcastMsg(byte[] msg, string messageType, IEnumerable<Address> dests)
        {
            if (this.outboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Outbound queue exception state. See inner exception.", 
                    this.outboundQueue.StateException);
            }
            var id = Guid.NewGuid();
            var now = DateTime.Now;
            foreach (var dest in dests)
            {
                var message = new OutboundMessage()
                {
                    From = this.address,
                    To = dest,
                    Id = Guid.NewGuid(),
                    MessageBytes = msg,
                    MessageTypeName = messageType,
                    Sent = DateTime.Now
                };
                this.outboundQueue.Enqueue(message);
            }
            return id;
        } 

        /// <summary>
        /// Get one message in order received and removes it from the inbox and logs it to the read log. 
        /// Blocking if timeoutMs = -1.
        /// </summary>
        /// <param name="timeoutMs">Specify milliseconds timeout. Returns null if timed out.</param>
        /// <returns></returns>
        public Message Receive(int timeoutMs = -1)
        {
            if (this.inboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Inbound queue exception state. See inner exception.", 
                    this.inboundQueue.StateException);
            }
            return this.inboundQueue.Receive(timeoutMs);
        }

        /// <summary>
        /// Get one to many messages in order received. Removes all returned from the inbox and 
        /// logs it to the read log. Blocking if timeoutMs = -1.
        /// </summary>
        /// <param name="maxMessagesToReceive">Indicate the maximum messages to pull from the queue.</param>
        /// <param name="timeoutMs">Specify milliseconds timeout. Returns null if timed out.</param>
        /// <returns></returns>
        public IList<Message> ReceiveBulk(int maxMessagesToReceive, int timeoutMs = -1)
        {
            if (this.inboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Inbound queue exception state. See inner exception.",
                    this.inboundQueue.StateException);
            }
            return this.inboundQueue.ReceiveBulk(maxMessagesToReceive, timeoutMs);
        }

        /// <summary>
        /// Get one message in order received without removing it from the inbox. Blocking if timeoutMs = -1.
        /// If Acknowledge is not called, the message will remain in the inbox and be queued again when
        /// the MessageQueue is next constructed.
        /// </summary>
        /// <param name="timeoutMs">Specify milliseconds timeout. Returns null if timed out.</param>
        /// <returns></returns>
        public Message Accept(int timeoutMs = -1)
        {
            if (this.inboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Inbound queue exception state. See inner exception.", 
                    this.inboundQueue.StateException);
            }
            return this.inboundQueue.Receive(timeoutMs, logRead: false);
        }

        /// <summary>
        /// Get one to many messages in order received without removing it from the inbox. 
        /// Blocking if timeoutMs = -1. If Acknowledge is not called, the message will 
        /// remain in the inbox and be queued again when the MessageQueue is next constructed.
        /// </summary>
        /// <param name="maxMessagesToReceive">Indicate the maximum messages to pull from the queue.</param>
        /// <param name="timeoutMs">Specify milliseconds timeout. Returns null if timed out.</param>
        /// <returns></returns>
        public IList<Message> AcceptBulk(int maxMessagesToReceive, int timeoutMs = -1)
        {
            if (this.inboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Inbound queue exception state. See inner exception.",
                    this.inboundQueue.StateException);
            }
            return this.inboundQueue.ReceiveBulk(maxMessagesToReceive, timeoutMs, logRead: false);
        }

        /// <summary>
        /// Signal message handled to be deleted from inbox and logged to read log.
        /// </summary>
        /// <param name="message"></param>
        public void Acknowledge(Message message)
        {
            if (this.inboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Inbound queue exception state. See inner exception.", 
                    this.inboundQueue.StateException);
            }
            this.inboundQueue.Acknowledge(message);
        }

        /// <summary>
        /// Signal message could not be handled at this time. Adds it back into the 
        /// in-process queue out of order. This allows reprocessing after processing
        /// the current queued messages.
        /// </summary>
        /// <param name="message"></param>
        public void ReEnqueue(Message message)
        {
            if (this.inboundQueue.State == QueueState.Failed)
            {
                throw new IOException("Inbound queue exception state. See inner exception.", 
                    this.inboundQueue.StateException);
            }
            this.inboundQueue.ReEnqueue(message);
        }

        private Address GetOptimalAddress(Address dest)
        {
            bool chooseTcp = (dest.Transport == Transport.Both 
                                && this.address.ServerName != dest.ServerName);
            if (chooseTcp || dest.Transport == Transport.Tcp)
            {
                if (null == this.ipEndPoint) throw new ArgumentException("Cannot send to a IP endpoint if queue does not have an IP endpoint.", "destEndPoint");
                return new Address(dest.ServerName, dest.Port);
            }
            else
            {
                if (null == this.npEndPoint) throw new ArgumentException("Cannot send to a named pipe endpoint if queue does not have named pipe endpoint.", "destEndPoint");
                return new Address(dest.PipeName);
            }
        }
        
        private string GetExecutablePathDirectory()
        {
            return Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) ?? string.Empty, "msg");
        }


        #region IDisposable 

        private bool _disposed = false;

        public void Dispose()
        {
            //MS recommended dispose pattern - prevents GC from disposing again
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                _disposed = true; //prevent second cleanup
                if (disposing)
                {
                    //cleanup here
                    FastFile.WriteAll(); //complete writing of queued up writes
                    FastFile.DeleteAll(); //clean marked for deletion
                    this.outboundQueue.Stop();
                    this.inboundQueue.Stop();
                    if (null != npHost) npHost.Dispose();
                    if (null != tcpHost) tcpHost.Dispose();
                }
            }
        }

        #endregion
    }
}
