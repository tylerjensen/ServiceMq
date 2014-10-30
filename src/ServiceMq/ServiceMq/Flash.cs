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
    public static class Flash
    {
        public static Guid Send<T>(Address from, Address dest, T message, params Address[] altDests)
        {
            var addr = GetOptimalAddress(from, dest);
            string msg = SvcStkTxt.TypeSerializer.SerializeToString(message);
            try
            {
                return SendMsg(msg, typeof(T).FullName, from, addr);
            }
            catch (Exception e)
            {
                if (null == altDests) throw new WebException("Send to destination failed", e);
            }
            Exception altEx = null;
            foreach (var alt in altDests)
            {
                try
                {
                    return SendMsg(msg, typeof(T).FullName, from, alt);
                }
                catch (Exception ex) 
                {
                    altEx = ex;
                }
            }
            throw new WebException("Alternative destination send failed", altEx);
        }

        public static Guid Send(Address from, Address dest, string messageType, string message, params Address[] altDests)
        {
            var addr = GetOptimalAddress(from, dest);
            try
            {
                return SendMsg(message, messageType, from, addr);
            }
            catch (Exception e)
            {
                if (null == altDests || altDests.Length == 0)
                {
                    throw new WebException("Send to destination failed", e);
                }
            }
            Exception altEx = null;
            foreach (var alt in altDests)
            {
                try
                {
                    return SendMsg(message, messageType, from, addr);
                }
                catch (Exception ex) 
                {
                    altEx = ex;
                }
            }
            throw new WebException("Alternative destination send failed", altEx);
        }

        public static Guid SendBytes(Address from, Address dest, byte[] message, string messageType, params Address[] altDests)
        {
            var addr = GetOptimalAddress(from, dest);
            try
            {
                return SendMsg(message, messageType, from, addr);
            }
            catch (Exception e)
            {
                if (null == altDests || altDests.Length == 0)
                {
                    throw new WebException("Send to destination failed", e);
                }
            }
            Exception altEx = null;
            foreach (var alt in altDests)
            {
                try
                {
                    return SendMsg(message, messageType, from, addr);
                }
                catch (Exception ex) 
                {
                    altEx = ex;
                }
            }
            throw new WebException("Alternative destination send failed", altEx);
        }

        private static Guid SendMsg(string msg, string messageType, Address from, Address dest)
        {
            var message = new OutboundMessage()
            {
                From = from,
                To = dest,
                Id = Guid.NewGuid(),
                MessageString = msg,
                MessageTypeName = messageType,
                Sent = DateTime.Now
            };
            SendMessage(message);
            return message.Id;
        }

        private static Guid SendMsg(byte[] msg, string messageType, Address from, Address dest)
        {
            var message = new OutboundMessage()
            {
                From = from,
                To = dest,
                Id = Guid.NewGuid(),
                MessageBytes = msg,
                MessageTypeName = messageType,
                Sent = DateTime.Now
            };
            SendMessage(message);
            return message.Id;
        }

        private static void SendMessage(OutboundMessage message)
        {
            NpClient<IMessageService> npClient = null;
            TcpClient<IMessageService> tcpClient = null;
            IMessageService proxy = null;
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
                    npClient = new NpClient<IMessageService>(new NpEndPoint(message.To.PipeName, 250));
                    proxy = npClient.Proxy;
                }
                else
                {
                    var tokenSource = new CancellationTokenSource();
                    var token = tokenSource.Token;
                    var task = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            tcpClient = new TcpClient<IMessageService>(new IPEndPoint(IPAddress.Parse(message.To.IpAddress), message.To.Port));
                            if (token.IsCancellationRequested)
                            {
                                tcpClient.Dispose();
                                tcpClient = null;
                            }
                        }
                        catch { }
                    }, token);

                    //force a timeout exception if not connected in 250ms
                    if (Task.WaitAll(new Task[] { task }, 250, token))
                    {
                        proxy = tcpClient.Proxy;
                    }
                    else
                    {
                        tokenSource.Cancel();
                        throw new TimeoutException("Could not connect in less than 250ms");
                    }
                }

                if (null == message.MessageBytes)
                {
                    proxy.EnqueueString(message.Id, message.From.ToString(), message.Sent, message.SendAttempts,
                         message.MessageTypeName, message.MessageString);
                }
                else
                {
                    proxy.EnqueueBytes(message.Id, message.From.ToString(), message.Sent, message.SendAttempts,
                        message.MessageTypeName, message.MessageBytes);
                }
            }
            finally
            {
                if (null != tcpClient) tcpClient.Dispose();
                if (null != npClient) npClient.Dispose();
            }
        }

        private static Address GetOptimalAddress(Address from, Address dest)
        {
            bool chooseTcp = (dest.Transport == Transport.Both
                                && from.ServerName != dest.ServerName);
            if (chooseTcp || dest.Transport == Transport.Tcp)
            {
                if (null == from.IpAddress) throw new ArgumentException("Cannot send to a IP endpoint if queue does not have an IP endpoint.", "destEndPoint");
                return new Address(dest.ServerName, dest.Port);
            }
            else
            {
                if (null == from.PipeName) throw new ArgumentException("Cannot send to a named pipe endpoint if queue does not have named pipe endpoint.", "destEndPoint");
                return new Address(dest.PipeName);
            }
        }

    }
}
