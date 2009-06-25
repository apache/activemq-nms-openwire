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
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Apache.NMS.ActiveMQ.Transport.Tcp;

namespace Apache.NMS.ActiveMQ.Transport.Discovery.Multicast
{
	internal delegate void NewBrokerServiceFound(string brokerName, string serviceName);
	internal delegate void BrokerServiceRemoved(string brokerName, string serviceName);

	internal class MulticastDiscoveryAgent : IDisposable
	{
		public const int MAX_SOCKET_CONNECTION_RETRY_ATTEMPS = 3;
		public const int DEFAULT_BACKOFF_MILLISECONDS = 100;
		public const int BACKOFF_MULTIPLIER = 2;
		public const string DEFAULT_DISCOVERY_URI_STRING = "multicast://239.255.2.3:6155";
		private const string TYPE_SUFFIX = "ActiveMQ-4.";
		private const string ALIVE = "alive";
		private const string DEAD = "dead";
		private const char DELIMITER = '%';
		private const int BUFF_SIZE = 8192;
		private const string DEFAULT_GROUP = "default";
		private const int EXPIRATION_OFFSET_IN_SECONDS = 2;
		private const int WORKER_KILL_TIME_SECONDS = 10;
		private const int SOCKET_TIMEOUT_MILLISECONDS = 500;

		private string group;
		private object stopstartSemaphore = new object();
		private bool isStarted = false;
		private Uri discoveryUri;
		private Socket multicastSocket;
		private IPEndPoint endPoint;
		private Thread worker;

		private event NewBrokerServiceFound onNewServiceFound;
		private event BrokerServiceRemoved onServiceRemoved;

		/// <summary>
		/// Indexed by service name
		/// </summary>
		private readonly Dictionary<string, RemoteBrokerData> remoteBrokers;

		public MulticastDiscoveryAgent()
		{
			group = DEFAULT_GROUP;
			remoteBrokers = new Dictionary<string, RemoteBrokerData>();
		}

		public void Start()
		{
			lock(stopstartSemaphore)
			{
				if (discoveryUri == null)
				{
					discoveryUri = new Uri(DEFAULT_DISCOVERY_URI_STRING);
				}

				if(multicastSocket == null)
				{
					int numFailedAttempts = 0;
					int backoffTime = DEFAULT_BACKOFF_MILLISECONDS;

					Tracer.Info("Connecting to multicast discovery socket.");
					while(!TryToConnectSocket())
					{
						numFailedAttempts++;
						if(numFailedAttempts > MAX_SOCKET_CONNECTION_RETRY_ATTEMPS)
						{
							throw new ApplicationException(
								"Could not open the socket in order to discover advertising brokers.");
						}

						Thread.Sleep(backoffTime);
						backoffTime *= BACKOFF_MULTIPLIER;
					}
				}

				if(worker == null)
				{
					Tracer.Info("Starting multicast discovery agent worker thread");
					worker = new Thread(new ThreadStart(worker_DoWork));
					worker.IsBackground = true;
					worker.Start();
					isStarted = true;
				}
			}
		}

		public void Stop()
		{
			Thread localThread = null;

			lock(stopstartSemaphore)
			{
				Tracer.Info("Stopping multicast discovery agent worker thread");
				localThread = worker;
				worker = null;
				// Changing the isStarted flag will signal the thread that it needs to shut down.
				isStarted = false;
			}

			if(localThread != null)
			{
				// wait for the worker to stop.
				if(!localThread.Join(WORKER_KILL_TIME_SECONDS))
				{
					Tracer.Info("!! Timeout waiting for multicast discovery agent localThread to stop");
					localThread.Abort();
				}
			}

			Tracer.Info("Multicast discovery agent worker thread joined");
		}

		private bool TryToConnectSocket()
		{
			bool hasSucceeded = false;

			try
			{
				multicastSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
				endPoint = new IPEndPoint(IPAddress.Any, discoveryUri.Port);

				//We have to allow reuse in the multicast socket. Otherwise, we would be unable to use multiple clients on the same machine.
				multicastSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, 1);
				multicastSocket.Bind(endPoint);

				IPAddress ipaddress;

				if(!TcpTransportFactory.TryParseIPAddress(discoveryUri.Host, out ipaddress))
				{
					ipaddress = TcpTransportFactory.GetIPAddress(discoveryUri.Host, AddressFamily.InterNetwork);
					if(null == ipaddress)
					{
						throw new NMSConnectionException("Invalid host address.");
					}
				}

				multicastSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.AddMembership,
												 new MulticastOption(ipaddress, IPAddress.Any));
#if !NETCF
				multicastSocket.ReceiveTimeout = SOCKET_TIMEOUT_MILLISECONDS;
#endif
				hasSucceeded = true;
			}
			catch(SocketException)
			{
			}

			return hasSucceeded;
		}

		private void worker_DoWork()
		{
			Thread.CurrentThread.Name = "Discovery Agent Thread.";
			byte[] buffer = new byte[BUFF_SIZE];
			string receivedInfoRaw;
			string receivedInfo;

			while(isStarted)
			{
				try
				{
					int numBytes = multicastSocket.Receive(buffer);
					receivedInfoRaw = System.Text.Encoding.UTF8.GetString(buffer, 0, numBytes);
					// We have to remove all of the null bytes if there are any otherwise we just
					// take the whole string as is.
					if (receivedInfoRaw.IndexOf("\0") != -1)
					{
						receivedInfo = receivedInfoRaw.Substring(0, receivedInfoRaw.IndexOf("\0"));
					}
					else
					{
						receivedInfo = receivedInfoRaw;
					}

					ProcessBrokerMessage(receivedInfo);
				}
				catch(SocketException)
				{
					// There was no multicast message sent before the timeout expired...Let us try again.
				}

				//We need to clear the buffer.
				buffer[0] = 0x0;
				ExpireOldServices();
			}
		}

		private void ProcessBrokerMessage(string message)
		{
			string payload;
			string brokerName;
			string serviceName;

			if(message.StartsWith(MulticastType))
			{
				payload = message.Substring(MulticastType.Length);
				brokerName = GetBrokerName(payload);
				serviceName = GetServiceName(payload);

				if(payload.StartsWith(ALIVE))
				{
					ProcessAliveBrokerMessage(brokerName, serviceName);
				}
				else if(payload.StartsWith(DEAD))
				{
					ProcessDeadBrokerMessage(brokerName, serviceName);
				}
				else
				{
					//Malformed Payload
				}
			}
		}

		private void ProcessDeadBrokerMessage(string brokerName, string serviceName)
		{
			if(remoteBrokers.ContainsKey(serviceName))
			{
				remoteBrokers.Remove(serviceName);
				if(onServiceRemoved != null)
				{
					onServiceRemoved(brokerName, serviceName);
				}
			}
		}

		private void ProcessAliveBrokerMessage(string brokerName, string serviceName)
		{
			if(remoteBrokers.ContainsKey(serviceName))
			{
				remoteBrokers[serviceName].UpdateHeartBeat();
			}
			else
			{
				remoteBrokers.Add(serviceName, new RemoteBrokerData(brokerName, serviceName));

				if(onNewServiceFound != null)
				{
					onNewServiceFound(brokerName, serviceName);
				}
			}
		}

		private static string GetBrokerName(string payload)
		{
			string[] results = payload.Split(DELIMITER);
			return results[1];
		}

		private static string GetServiceName(string payload)
		{
			string[] results = payload.Split(DELIMITER);
			return results[2];
		}

		private void ExpireOldServices()
		{
			DateTime expireTime;
			List<RemoteBrokerData> deadServices = new List<RemoteBrokerData>();

			foreach(KeyValuePair<string, RemoteBrokerData> brokerService in remoteBrokers)
			{
				expireTime = brokerService.Value.lastHeartBeat.AddSeconds(EXPIRATION_OFFSET_IN_SECONDS);
				if(DateTime.Now > expireTime)
				{
					deadServices.Add(brokerService.Value);
				}
			}

			// Remove all of the dead services
			for(int i = 0; i < deadServices.Count; i++)
			{
				ProcessDeadBrokerMessage(deadServices[i].brokerName, deadServices[i].serviceName);
			}
		}

		#region Properties

		/// <summary>
		/// This property indicates whether or not async send is enabled.
		/// </summary>
		public Uri DiscoveryURI
		{
			get { return discoveryUri; }
			set { discoveryUri = value; }
		}

		public bool IsStarted
		{
			get { return isStarted; }
		}

		public string Group
		{
			get { return group; }
			set { group = value; }
		}

		#endregion

		internal string MulticastType
		{
			get { return group + "." + TYPE_SUFFIX; }
		}

		internal event NewBrokerServiceFound OnNewServiceFound
		{
			add { onNewServiceFound += value; }
			remove { onNewServiceFound -= value; }
		}

		internal event BrokerServiceRemoved OnServiceRemoved
		{
			add { onServiceRemoved += value; }
			remove { onServiceRemoved += value; }
		}

		public void Dispose()
		{
			if(isStarted)
			{
				Stop();
			}

			multicastSocket.Shutdown(SocketShutdown.Both);
			multicastSocket = null;
		}

		internal class RemoteBrokerData
		{
			internal string brokerName;
			internal string serviceName;
			internal DateTime lastHeartBeat;

			internal RemoteBrokerData(string brokerName, string serviceName)
			{
				this.brokerName = brokerName;
				this.serviceName = serviceName;
				this.lastHeartBeat = DateTime.Now;
			}

			internal void UpdateHeartBeat()
			{
				this.lastHeartBeat = DateTime.Now;
			}
		}
	}
}
