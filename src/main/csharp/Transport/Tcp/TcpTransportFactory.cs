/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Net;
using System.Net.Sockets;
using System.Collections.Specialized;
using Apache.NMS.ActiveMQ.OpenWire;
using Apache.NMS.ActiveMQ.Transport.Stomp;
using Apache.NMS;
using Apache.NMS.Util;
using System.Threading;

namespace Apache.NMS.ActiveMQ.Transport.Tcp
{
	public class TcpTransportFactory : ITransportFactory
	{
		public TcpTransportFactory()
		{
		}

		#region Properties

		private bool useLogging = false;
		public bool UseLogging
		{
			get { return useLogging; }
			set { useLogging = value; }
		}

		/// <summary>
		/// Size in bytes of the receive buffer.
		/// </summary>
		private int receiveBufferSize = 8192;
		public int ReceiveBufferSize
		{
			get { return receiveBufferSize; }
			set { receiveBufferSize = value; }
		}

		/// <summary>
		/// Size in bytes of send buffer.
		/// </summary>
		private int sendBufferSize = 8192;
		public int SendBufferSize
		{
			get { return sendBufferSize; }
			set { sendBufferSize = value; }
		}

		/// <summary>
		/// The time-out value, in milliseconds. The default value is 0, which indicates
		/// an infinite time-out period. Specifying -1 also indicates an infinite time-out period.
		/// </summary>
		private int receiveTimeout = 0;
		public int ReceiveTimeout
		{
			get { return receiveTimeout; }
			set { receiveTimeout = value; }
		}

		/// <summary>
		/// The time-out value, in milliseconds. If you set the property with a value between 1 and 499,
		/// the value will be changed to 500. The default value is 0, which indicates an infinite
		/// time-out period. Specifying -1 also indicates an infinite time-out period.
		/// </summary>
		private int sendTimeout = 0;
		public int SendTimeout
		{
			get { return sendTimeout; }
			set { sendTimeout = value; }
		}

		private string wireFormat = "OpenWire";
		public string WireFormat
		{
			get { return wireFormat; }
			set { wireFormat = value; }
		}

		private TimeSpan requestTimeout = NMSConstants.defaultRequestTimeout;
		public int RequestTimeout
		{
			get { return (int) requestTimeout.TotalMilliseconds; }
			set { requestTimeout = TimeSpan.FromMilliseconds(value); }
		}

		#endregion

		#region ITransportFactory Members

		public ITransport CreateTransport(Uri location)
		{
			// Extract query parameters from broker Uri
			StringDictionary map = URISupport.ParseQuery(location.Query);

			// Set transport. properties on this (the factory)
			URISupport.SetProperties(this, map, "transport.");

			Tracer.Debug("Opening socket to: " + location.Host + " on port: " + location.Port);
			Socket socket = Connect(location.Host, location.Port);

#if !NETCF
			socket.ReceiveBufferSize = ReceiveBufferSize;
			socket.SendBufferSize = SendBufferSize;
			socket.ReceiveTimeout = ReceiveTimeout;
			socket.SendTimeout = SendTimeout;
#endif

			IWireFormat wireformat = CreateWireFormat(location, map);
			ITransport transport = new TcpTransport(socket, wireformat);

			wireformat.Transport = transport;

			if(UseLogging)
			{
				transport = new LoggingTransport(transport);
			}

			if(wireformat is OpenWireFormat)
			{
				transport = new WireFormatNegotiator(transport, (OpenWireFormat) wireformat);
			}

			transport = new MutexTransport(transport);
			transport = new ResponseCorrelator(transport);
			transport.RequestTimeout = this.requestTimeout;

			return transport;
		}

		#endregion

		protected Socket Connect(string host, int port)
		{
			// Looping through the AddressList allows different type of connections to be tried
			// (IPv4, IPv6 and whatever else may be available).
#if NET_1_1 || NET_1_0
			// The following GetHostByName() API has been obsoleted in .NET 2.0.  It has been
			// superceded by GetHostEntry().  At some point, it will probably be removed
			// from the Mono class library, and this #if statement can be modified.

			IPHostEntry hostEntry = Dns.GetHostByName(host);
#else
			IPHostEntry hostEntry = Dns.GetHostEntry(host);
#endif

			foreach(IPAddress address in hostEntry.AddressList)
			{
				Socket socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
				socket.Connect(new IPEndPoint(address, port));
				if(socket.Connected)
				{
					return socket;
				}
			}
			throw new SocketException();
		}

		protected IWireFormat CreateWireFormat(Uri location, StringDictionary map)
		{
			object properties = null;
			IWireFormat wireFormatItf = null;

			// Detect STOMP etc
			if(String.Compare(location.Scheme, "stomp", true) == 0)
			{
				this.wireFormat = "STOMP";
			}

			if(String.Compare(this.wireFormat, "stomp", true) == 0)
			{
				wireFormatItf = new StompWireFormat();
				properties = wireFormatItf;
			}
			else
			{
				OpenWireFormat openwireFormat = new OpenWireFormat();

				wireFormatItf = openwireFormat;
				properties = openwireFormat.PreferedWireFormatInfo;
			}

			if(null != properties)
			{
				// Set wireformat. properties on the wireformat owned by the tcpTransport
				URISupport.SetProperties(properties, map, "wireFormat.");
			}

			return wireFormatItf;
		}
	}
}
