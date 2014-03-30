using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;

namespace ServiceMq
{
    [Serializable]
    public class Address
    {
        public string ServerName { get; private set; }
        public string PipeName { get; private set; }
        public string IpAddress { get; private set; }
        public int Port { get; private set; }
        public Transport Transport { get; private set; }

        public override string ToString()
        {
            return string.Format("{0},{1},{2},{3},{4}", ServerName, PipeName, IpAddress, Port, Transport);
        }

        public string ToFileNameString()
        {
            //010-042-024-155-08746-pipename
            switch (Transport)
            {
                case Transport.Np:
                    return string.Format("{0}", PipeName);
                case Transport.Tcp:
                    return string.Format("{0}-{1}", IpAddress.Replace(".", "-"), Port);
                default:
                    return string.Format("{0}-{1}-{2}", IpAddress.Replace(".", "-"), Port, PipeName);
            }
        }

        public static Address FromString(string addr)
        {
            var parts = addr.Split(',');
            if (parts.Length != 5) throw new ArgumentException("cannot deserialize to Address", "addr");
            var result = new Address(parts[0], Convert.ToInt32(parts[3]));
            result.PipeName = parts[1];
            result.IpAddress = parts[2];
            result.Transport = (Transport) Enum.Parse(typeof (Transport), parts[4]);
            return result;
        }

        /// <summary>
        /// Create a TCP address pointing to first IPv4 address for serverName.
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="port"></param>
        public Address(string serverName, int port)
        {
            this.ServerName = serverName;
            this.PipeName = null;

            var ipAddress = Dns.GetHostAddresses(this.ServerName)
                                .FirstOrDefault(x => x.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(x));

            if (null == ipAddress) throw new ArgumentException("Unable to get IP address for serverName", "serverName");
                
            this.IpAddress = ipAddress.ToString();
            this.Port = port;
            this.Transport = Transport.Tcp;
        }

        /// <summary>
        /// Create a NamedPipe and TCP address using host name to get local IPv4 address.
        /// </summary>
        /// <param name="port"></param>
        /// <param name="pipeName"></param>
        public Address(int port, string pipeName)
        {
            this.ServerName = Dns.GetHostName();
            this.PipeName = pipeName;
            var ipAddress = Dns.GetHostAddresses(this.ServerName)
                                .FirstOrDefault(x => x.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(x));

            if (null == ipAddress) throw new Exception("Unable to get IP address for host");

            this.IpAddress = ipAddress.ToString();
            this.Port = port;
            this.Transport = Transport.Both;
        }

        /// <summary>
        /// Create a NamedPipe and TCP address using specified ipAddress.
        /// </summary>
        /// <param name="ipAddress"></param>
        /// <param name="port"></param>
        /// <param name="pipeName"></param>
        public Address(string ipAddress, int port, string pipeName)
        {
            this.ServerName = Dns.GetHostName();
            this.PipeName = pipeName;
            this.IpAddress = ipAddress;
            this.Port = port;
            this.Transport = Transport.Both;
        }

        /// <summary>
        /// Create a NamedPipe and TCP address using specified ipAddress.
        /// </summary>
        /// <param name="ipAddress"></param>
        /// <param name="port"></param>
        /// <param name="pipeName"></param>
        public Address(IPAddress ipAddress, int port, string pipeName)
        {
            this.ServerName = Dns.GetHostName();
            this.PipeName = pipeName;
            this.IpAddress = ipAddress.ToString();
            this.Port = port;
            this.Transport = Transport.Both;
        }

        /// <summary>
        /// Create a NamedPipe address.
        /// </summary>
        /// <param name="pipeName"></param>
        public Address(string pipeName)
        {
            this.ServerName = Dns.GetHostName();
            this.PipeName = pipeName;
            this.IpAddress = null;
            this.Port = 0;
            this.Transport = Transport.Np;
        }
    }

    public enum Transport
    {
        Np,
        Tcp,
        Both
    }
}
