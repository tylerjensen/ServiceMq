using System.IO;
using ServiceWire;
using ServiceWire.NamedPipes;
using ServiceWire.TcpIp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceMq
{
    /// <summary>
    /// Use to send immediate to one destination with optional backup destinations.
    /// </summary>
    public sealed class Flasher : IDisposable
    {
        private readonly int _connectTimeOutMs = 500;
        private readonly Address _from;
        private readonly PooledDictionary<string, NpClient<IMessageService>> npClientPool = null;
        private readonly PooledDictionary<string, TcpClient<IMessageService>> tcpClientPool = null;

        public Flasher(Address from, int connectTimeOutMs = 500)
        {
            _from = from;
            _connectTimeOutMs = connectTimeOutMs;
            this.npClientPool = new PooledDictionary<string, NpClient<IMessageService>>();
            this.tcpClientPool = new PooledDictionary<string, TcpClient<IMessageService>>();
        }

        public Guid Send<T>(Address dest, T message, params Address[] altDests)
        {
            var addr = GetOptimalAddress(dest);
            string msg = SvcStkTxt.TypeSerializer.SerializeToString(message);
            try
            {
                return SendMsg(msg, typeof(T).FullName, addr);
            }
            catch (Exception e)
            {
                if (null == altDests || altDests.Length == 0) throw new WebException("Send to destination failed", e);
            }
            Exception altEx = null;
            foreach (var altAddr in altDests)
            {
                try
                {
                    return SendMsg(msg, typeof(T).FullName, altAddr);
                }
                catch (Exception ex) 
                {
                    altEx = ex;
                }
            }
            throw new WebException("Alternative destination send failed", altEx);
        }

        public Guid Send(Address dest, string messageType, string message, params Address[] altDests)
        {
            var addr = GetOptimalAddress(dest);
            try
            {
                return SendMsg(message, messageType, addr);
            }
            catch (Exception e)
            {
                if (null == altDests || altDests.Length == 0)
                {
                    throw new WebException("Send to destination failed", e);
                }
            }
            Exception altEx = null;
            foreach (var altAddr in altDests)
            {
                try
                {
                    return SendMsg(message, messageType, altAddr);
                }
                catch (Exception ex) 
                {
                    altEx = ex;
                }
            }
            throw new WebException("Alternative destination send failed", altEx);
        }

        public Guid SendBytes(Address dest, byte[] message, string messageType, params Address[] altDests)
        {
            var addr = GetOptimalAddress(dest);
            try
            {
                return SendMsg(message, messageType, addr);
            }
            catch (Exception e)
            {
                if (null == altDests || altDests.Length == 0)
                {
                    throw new WebException("Send to destination failed", e);
                }
            }
            Exception altEx = null;
            foreach (var altAddr in altDests)
            {
                try
                {
                    return SendMsg(message, messageType, altAddr);
                }
                catch (Exception ex) 
                {
                    altEx = ex;
                }
            }
            throw new WebException("Alternative destination send failed", altEx);
        }

        private Guid SendMsg(string msg, string messageType, Address dest)
        {
            var message = new OutboundMessage()
            {
                From = _from,
                To = dest,
                Id = Guid.NewGuid(),
                MessageString = msg,
                MessageTypeName = messageType,
                Sent = DateTime.Now
            };
            SendMessage(message);
            return message.Id;
        }

        private Guid SendMsg(byte[] msg, string messageType, Address dest)
        {
            var message = new OutboundMessage()
            {
                From = _from,
                To = dest,
                Id = Guid.NewGuid(),
                MessageBytes = msg,
                MessageTypeName = messageType,
                Sent = DateTime.Now
            };
            SendMessage(message);
            return message.Id;
        }

        private void SendMessage(OutboundMessage message)
        {
            NpClient<IMessageService> npClient = null;
            TcpClient<IMessageService> tcpClient = null;
            IMessageService proxy = null;
            var poolKey = message.To.ToString();
            try
            {
                var useNpClient = false;
                if (message.To.Transport == Transport.Both)
                {
                    if (message.To.ServerName == message.From.ServerName)
                    {
                        useNpClient = true;
                    }
                }
                else if (message.To.Transport == Transport.Np) useNpClient = true;

                if (useNpClient)
                {
                    npClient = npClientPool.Request(poolKey,
                        () => new NpClient<IMessageService>(
                            new NpEndPoint(message.To.PipeName, _connectTimeOutMs)));
                    proxy = npClient.Proxy;
                }
                else
                {
                    tcpClient = tcpClientPool.Request(poolKey,
                        () => new TcpClient<IMessageService>(new TcpEndPoint(
                            new IPEndPoint(IPAddress.Parse(message.To.IpAddress),
                                message.To.Port), _connectTimeOutMs)));
                    proxy = tcpClient.Proxy;
                }

                if (null == proxy)
                {
                    throw new IOException("Unable to get or create proxy.");
                }
                if (null == message.MessageBytes)
                {
                    proxy.EnqueueString(message.Id, message.From.ToString(), message.Sent, 1,
                        message.MessageTypeName, message.MessageString);
                }
                else
                {
                    proxy.EnqueueBytes(message.Id, message.From.ToString(), message.Sent, 1,
                        message.MessageTypeName, message.MessageBytes);
                }
            }
            catch
            {
                //assure failed client is properly disposed and not returned to pool
                if (null != tcpClient)
                {
                    tcpClient.Dispose();
                    tcpClient = null;
                }
                if (null != npClient)
                {
                    npClient.Dispose();
                    npClient = null;
                }
                throw;
            }
            finally
            {
                //return client to pool
                if (null != tcpClient) tcpClientPool.Release(poolKey, tcpClient);
                if (null != npClient) npClientPool.Release(poolKey, npClient);
            }
        }

        private Address GetOptimalAddress(Address destAddress)
        {
            bool chooseTcp = (destAddress.Transport == Transport.Both && _from.ServerName != destAddress.ServerName);
            if (chooseTcp || destAddress.Transport == Transport.Tcp)
            {
                if (null == _from.IpAddress)
                {
                    throw new ArgumentException("Cannot send to a IP endpoint if queue does not have an IP endpoint.", "destAddress");
                }
                return new Address(destAddress.ServerName, destAddress.Port);
            }
            if (null == _from.PipeName)
            {
                throw new ArgumentException("Cannot send to a named pipe endpoint if queue does not have named pipe endpoint.", "destAddress");
            }
            return new Address(destAddress.PipeName);
        }

        #region IDisposable Members

        private bool _disposed = false;

        public void Dispose()
        {
            //MS recommended dispose pattern - prevents GC from disposing again
            if (!_disposed)
            {
                _disposed = true;
                npClientPool.Dispose();
                tcpClientPool.Dispose();
                GC.SuppressFinalize(this);
            }
        }

        #endregion
    }
}
