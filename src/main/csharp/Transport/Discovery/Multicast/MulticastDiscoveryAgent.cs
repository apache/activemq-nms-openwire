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
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Threads;
using Apache.NMS.ActiveMQ.Transport.Tcp;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.Transport.Discovery.Multicast
{
	internal delegate void NewBrokerServiceFound(string brokerName, string serviceName);
	internal delegate void BrokerServiceRemoved(string brokerName, string serviceName);

	internal class MulticastDiscoveryAgent : IDiscoveryAgent, IDisposable
	{
        public const string DEFAULT_DISCOVERY_URI_STRING = "multicast://239.255.2.3:6155";
        public const string DEFAULT_HOST_STR = "default"; 
        public const string DEFAULT_HOST_IP = "239.255.2.3";
        public const int DEFAULT_PORT = 6155;

		public const int DEFAULT_INITIAL_RECONNECT_DELAY = 1000 * 5;
        public const int DEFAULT_BACKOFF_MULTIPLIER = 2;
        public const int DEFAULT_MAX_RECONNECT_DELAY = 1000 * 30;

		private const string TYPE_SUFFIX = "ActiveMQ-4.";
		private const string ALIVE = "alive";
		private const string DEAD = "dead";
		private const char DELIMITER = '%';
		private const int BUFF_SIZE = 8192;
		private const int HEARTBEAT_MISS_BEFORE_DEATH = 10;
		private const int DEFAULT_IDLE_TIME = 500;
        private const string DEFAULT_GROUP = "default";
        private const int WORKER_KILL_TIME_SECONDS = 1000;

        private const int MAX_SOCKET_CONNECTION_RETRY_ATTEMPS = 3;
        private const int SOCKET_CONNECTION_BACKOFF_TIME = 500;

        private long initialReconnectDelay = DEFAULT_INITIAL_RECONNECT_DELAY;
        private long maxReconnectDelay = DEFAULT_MAX_RECONNECT_DELAY;
        private long backOffMultiplier = DEFAULT_BACKOFF_MULTIPLIER;
        private bool useExponentialBackOff;
        private int maxReconnectAttempts;

        private int timeToLive = 1;
        private string group = DEFAULT_GROUP;
        private bool loopBackMode;
        private Dictionary<String, RemoteBrokerData> brokersByService = 
            new Dictionary<String, RemoteBrokerData>();
        private readonly object servicesLock = new object();
        private String selfService;
        private long keepAliveInterval = DEFAULT_IDLE_TIME;
        private string mcInterface;
        private string mcNetworkInterface;
        private string mcJoinNetworkInterface;
        private DateTime lastAdvertizeTime;
        private bool reportAdvertizeFailed = true;
        private readonly Atomic<bool> started = new Atomic<bool>(false);

        private Uri discoveryUri;
		private Socket multicastSocket;
		private IPEndPoint endPoint;
		private Thread worker;
        private readonly ThreadPoolExecutor executor = new ThreadPoolExecutor();

        private ServiceAddHandler serviceAddHandler;
        private ServiceRemoveHandler serviceRemoveHandler;

        #region Property Setters and Getters

        public bool LoopBackMode
        {
            get { return this.loopBackMode; }
            set { this.loopBackMode = value; }
        }

        public int TimeToLive
        {
            get { return this.timeToLive; }
            set { this.timeToLive = value; }
        }

        public long KeepAliveInterval
        {
            get { return this.keepAliveInterval; }
            set { this.keepAliveInterval = value; }
        }

        public string Interface
        {
            get { return this.mcInterface; }
            set { this.mcInterface = value; }
        }

        public string NetworkInterface
        {
            get { return this.mcNetworkInterface; }
            set { this.mcNetworkInterface = value; }
        }

        public string JoinNetworkInterface
        {
            get { return this.mcJoinNetworkInterface; }
            set { this.mcJoinNetworkInterface = value; }
        }

        public string Type
        {
            get { return this.group + "." + TYPE_SUFFIX; }
        }

        public long BackOffMultiplier
        {
            get { return this.backOffMultiplier; }
            set { this.backOffMultiplier = value; }
        }

        public long InitialReconnectDelay
        {
            get { return this.initialReconnectDelay; }
            set { this.initialReconnectDelay = value; }
        }

        public int MaxReconnectAttempts
        {
            get { return this.maxReconnectAttempts; }
            set { this.maxReconnectAttempts = value; }
        }

        public long MaxReconnectDelay
        {
            get { return this.maxReconnectDelay; }
            set { this.maxReconnectDelay = value; }
        }

        public bool UseExponentialBackOff
        {
            get { return this.useExponentialBackOff; }
            set { this.useExponentialBackOff = value; }
        }

        public string Group
        {
            get { return this.group; }
            set { this.group = value; }
        }

        public ServiceAddHandler ServiceAdd
        {
            get { return serviceAddHandler; }
            set { this.serviceAddHandler = value; }
        }

        public ServiceRemoveHandler ServiceRemove
        {
            get { return serviceRemoveHandler; }
            set { this.serviceRemoveHandler = value; }
        }

        public Uri DiscoveryURI
        {
            get { return discoveryUri; }
            set { discoveryUri = value; }
        }

        public bool IsStarted
        {
            get { return started.Value; }
        }

        #endregion

        public override String ToString()
        {
            return "MulticastDiscoveryAgent-" + (selfService != null ? "advertise:" + selfService : "");
        }

        public void RegisterService(String name)
        {
            this.selfService = name;
            if (started.Value)
            {
                DoAdvertizeSelf();
            }
        }

        public void ServiceFailed(DiscoveryEvent failedEvent)
        {
            RemoteBrokerData data = brokersByService[failedEvent.ServiceName];
            if (data != null && data.MarkFailed()) 
            {
                FireServiceRemoveEvent(data);
            }
        }

		public void Start()
		{
            if (started.CompareAndSet(false, true)) {           
                            
                if (String.IsNullOrEmpty(group)) 
                {
                    throw new IOException("You must specify a group to discover");
                }
                String type = Type;
                if (!type.EndsWith(".")) 
                {
                    Tracer.Warn("The type '" + type + "' should end with '.' to be a valid Discovery type");
                    type += ".";
                }
                
                if (discoveryUri == null) 
                {
                    discoveryUri = new Uri(DEFAULT_DISCOVERY_URI_STRING);
                }

                if (Tracer.IsDebugEnabled) 
                {
                    Tracer.Debug("start - discoveryURI = " + discoveryUri);                              
                }

                String targetHost = discoveryUri.Host;
                int targetPort = discoveryUri.Port;
                     
                if (DEFAULT_HOST_STR.Equals(targetHost)) 
                {
                    targetHost = DEFAULT_HOST_IP;                     
                }

                if (targetPort < 0) 
                {
                    targetPort = DEFAULT_PORT;              
                }
                  
                if (Tracer.IsDebugEnabled) 
                {
                    Tracer.DebugFormat("start - myHost = {0}", targetHost); 
                    Tracer.DebugFormat("start - myPort = {0}", targetPort);    
                    Tracer.DebugFormat("start - group  = {0}", group);                    
                    Tracer.DebugFormat("start - interface  = {0}", mcInterface);
                    Tracer.DebugFormat("start - network interface  = {0}", mcNetworkInterface);
                    Tracer.DebugFormat("start - join network interface  = {0}", mcJoinNetworkInterface);
                } 

                int numFailedAttempts = 0;
                int backoffTime = SOCKET_CONNECTION_BACKOFF_TIME;

                Tracer.Info("Connecting to multicast discovery socket.");
                while (!TryToConnectSocket(targetHost, targetPort))
                {
                    numFailedAttempts++;
                    if (numFailedAttempts > MAX_SOCKET_CONNECTION_RETRY_ATTEMPS)
                    {
                        throw new ApplicationException(
                            "Could not open the socket in order to discover advertising brokers.");
                    }

                    Thread.Sleep(backoffTime);
                    backoffTime = (int)(backoffTime * BackOffMultiplier);
                }

                if(worker == null)
                {
                    Tracer.Info("Starting multicast discovery agent worker thread");
                    worker = new Thread(new ThreadStart(DiscoveryAgentRun));
                    worker.IsBackground = true;
                    worker.Start();
                }

                DoAdvertizeSelf();
            }
		}

		public void Stop()
		{
            // Changing the isStarted flag will signal the thread that it needs to shut down.
            if (started.CompareAndSet(true, false))
			{
				Tracer.Info("Stopping multicast discovery agent worker thread");
                if (multicastSocket != null)
                {
                    multicastSocket.Close();
                }
                if(worker != null)
                {
                    // wait for the worker to stop.
                    if(!worker.Join(WORKER_KILL_TIME_SECONDS))
                    {
                        Tracer.Info("!! Timeout waiting for multicast discovery agent localThread to stop");
                        worker.Abort();
                    }
                    worker = null;
                    Tracer.Debug("Multicast discovery agent worker thread stopped");
                }
                executor.Shutdown();
                if (!executor.AwaitTermination(TimeSpan.FromMinutes(1)))
                {
                    Tracer.DebugFormat("Failed to properly shutdown agent executor {0}", this);
                }
			}
		}

		private bool TryToConnectSocket(string targetHost, int targetPort)
		{
			bool hasSucceeded = false;

			try
			{
				multicastSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
				endPoint = new IPEndPoint(IPAddress.Any, targetPort);

				// We have to allow reuse in the multicast socket. Otherwise, we would be unable to
                // use multiple clients on the same machine.
				multicastSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, 1);
				multicastSocket.Bind(endPoint);

				IPAddress ipaddress;

				if(!TcpTransportFactory.TryParseIPAddress(targetHost, out ipaddress))
				{
					ipaddress = TcpTransportFactory.GetIPAddress(targetHost, AddressFamily.InterNetwork);
					if(null == ipaddress)
					{
						throw new NMSConnectionException("Invalid host address.");
					}
				}

                if (LoopBackMode)
                {
                    multicastSocket.MulticastLoopback = true;
                }
                if (TimeToLive != 0)
                {
				    multicastSocket.SetSocketOption(SocketOptionLevel.IP, 
                                                    SocketOptionName.MulticastTimeToLive, timeToLive);
                }
                if (!String.IsNullOrEmpty(mcJoinNetworkInterface))
                {
                    // TODO figure out how to set this.
                    throw new NotSupportedException("McJoinNetworkInterface not yet implemented.");
                }
                else 
                {
                    multicastSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.AddMembership,
                                                     new MulticastOption(ipaddress, IPAddress.Any));
                }

                if (!String.IsNullOrEmpty(mcNetworkInterface))
                {
                    // TODO figure out how to set this.
                    throw new NotSupportedException("McNetworkInterface not yet implemented.");
                }

				multicastSocket.ReceiveTimeout = (int)keepAliveInterval;

				hasSucceeded = true;
			}
			catch(SocketException)
			{
			}

			return hasSucceeded;
		}

		private void DiscoveryAgentRun()
		{
			Thread.CurrentThread.Name = "Discovery Agent Thread.";
			byte[] buffer = new byte[BUFF_SIZE];
			string receivedInfoRaw;
			string receivedInfo;

			while (started.Value)
			{
                DoTimeKeepingServices();
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

					ProcessServiceAdvertisement(receivedInfo);
				}
				catch(SocketException)
				{
					// There was no multicast message sent before the timeout expired...Let us try again.
				}

				// We need to clear the buffer.
				buffer[0] = 0x0;
			}
		}

		private void ProcessServiceAdvertisement(string message)
		{
			string payload;
			string brokerName;
			string serviceName;

			if (message.StartsWith(Type))
			{
				payload = message.Substring(Type.Length);
				brokerName = GetBrokerName(payload);
				serviceName = GetServiceName(payload);

				if (payload.StartsWith(ALIVE))
				{
					ProcessAlive(brokerName, serviceName);
				}
				else if (payload.StartsWith(DEAD))
				{
					ProcessDead(serviceName);
				}
				else
				{
					// Malformed Payload
				}
			}
		}

        private void DoTimeKeepingServices() 
        {
            if (started.Value)
            {
                DateTime currentTime = DateTime.Now;
                if (currentTime < lastAdvertizeTime || 
                    ((currentTime - TimeSpan.FromMilliseconds(keepAliveInterval)) > lastAdvertizeTime))
                {
                    DoAdvertizeSelf();
                    lastAdvertizeTime = currentTime;
                }
                DoExpireOldServices();
            }
        }

        private void DoAdvertizeSelf()
        {
            if (!String.IsNullOrEmpty(selfService)) 
            {
                String payload = Type;
                payload += started.Value ? ALIVE : DEAD;
                payload += DELIMITER + "localhost" + DELIMITER;
                payload += selfService;
                try 
                {
                    byte[] data = System.Text.Encoding.UTF8.GetBytes(payload);
                    multicastSocket.Send(data);
                } 
                catch (Exception e) 
                {
                    // If a send fails, chances are all subsequent sends will fail
                    // too.. No need to keep reporting the
                    // same error over and over.
                    if (reportAdvertizeFailed) 
                    {
                        reportAdvertizeFailed = false;
                        Tracer.ErrorFormat("Failed to advertise our service: {0} cause: {1}", payload, e.Message);
                    }
                }
            }
        }

        private void ProcessAlive(string brokerName, string service)
        {
            if (selfService == null || !service.Equals(selfService)) 
            {
                RemoteBrokerData remoteBroker = null;
                lock (servicesLock)
                {
                    brokersByService.TryGetValue(service, out remoteBroker);
                }
                if (remoteBroker == null) 
                {
                    remoteBroker = new RemoteBrokerData(this, brokerName, service);
                    brokersByService.Add(service, remoteBroker);      
                    FireServiceAddEvent(remoteBroker);
                    DoAdvertizeSelf();
                } 
                else 
                {
                    remoteBroker.UpdateHeartBeat();
                    if (remoteBroker.IsTimeForRecovery()) 
                    {
                        FireServiceAddEvent(remoteBroker);
                    }
                }
            }
        }

        private void ProcessDead(string service) 
        {
            if (!service.Equals(selfService)) 
            {
                RemoteBrokerData remoteBroker = null;
                lock (servicesLock)
                {
                    brokersByService.TryGetValue(service, out remoteBroker);
                    if (remoteBroker != null)
                    {
                        brokersByService.Remove(service);
                    }
                }
                if (remoteBroker != null && !remoteBroker.Failed) 
                {
                    FireServiceRemoveEvent(remoteBroker);
                }
            }
        }

        private void DoExpireOldServices() 
        {
            DateTime expireTime = DateTime.Now - TimeSpan.FromMilliseconds(keepAliveInterval * HEARTBEAT_MISS_BEFORE_DEATH); 

            RemoteBrokerData[] services = null;
            lock (servicesLock)
            {
                services = new RemoteBrokerData[this.brokersByService.Count];
                this.brokersByService.Values.CopyTo(services, 0);
            }

            foreach(RemoteBrokerData service in services)
            {
                if (service.LastHeartBeat < expireTime) 
                {
                    ProcessDead(service.ServiceName);
                }
            }
        }

		private static string GetBrokerName(string payload)
		{
			string[] results = payload.Split(DELIMITER);
            if (results.Length >= 2)
            {
			    return results[1];
            }
            return null;
		}

		private static string GetServiceName(string payload)
		{
			string[] results = payload.Split(DELIMITER);
            if (results.Length >= 3)
            {
                return results[2];
            }
            return null;
		}

		public void Dispose()
		{
			if(started.Value)
			{
				Stop();
			}
		}

        private void FireServiceRemoveEvent(RemoteBrokerData data) 
        {
            if (serviceRemoveHandler != null && started.Value) 
            {
                // Have the listener process the event async so that
                // he does not block this thread since we are doing time sensitive
                // processing of events.
                executor.QueueUserWorkItem(ServiceRemoveCallback, data);
            }
        }

        private void ServiceRemoveCallback(object data)
        {
            RemoteBrokerData serviceData = data as RemoteBrokerData;
            this.serviceRemoveHandler(serviceData);
        }

        private void FireServiceAddEvent(RemoteBrokerData data) 
        {
            if (serviceAddHandler != null && started.Value) 
            {
                // Have the listener process the event async so that
                // he does not block this thread since we are doing time sensitive
                // processing of events.
                executor.QueueUserWorkItem(ServiceAddCallback, data);
            }
        }

        private void ServiceAddCallback(object data)
        {
            RemoteBrokerData serviceData = data as RemoteBrokerData;
            this.serviceAddHandler(serviceData);
        }

		internal class RemoteBrokerData : DiscoveryEvent
		{
            internal DateTime recoveryTime = DateTime.MinValue;
            internal int failureCount;
            internal bool failed;
			internal DateTime lastHeartBeat;

            private readonly object syncRoot = new object();
            private readonly MulticastDiscoveryAgent parent;

			internal RemoteBrokerData(MulticastDiscoveryAgent parent, string brokerName, string serviceName) : base()
			{
                this.parent = parent;
				this.BrokerName = brokerName;
				this.ServiceName = serviceName;
				this.lastHeartBeat = DateTime.Now;
			}

            internal bool Failed
            {
                get { return this.failed; }
            }

            internal DateTime LastHeartBeat
            {
                get 
                { 
                    lock(syncRoot)
                    {
                        return this.lastHeartBeat; 
                    }
                }
            }

			internal void UpdateHeartBeat()
			{
                lock (syncRoot)
                {
                    this.lastHeartBeat = DateTime.Now;

                    // Consider that the broker recovery has succeeded if it has not
                    // failed in 60 seconds.
                    if (!failed && failureCount > 0 && 
                        (lastHeartBeat - recoveryTime) > TimeSpan.FromMilliseconds(1000 * 60)) {

                        Tracer.DebugFormat("I now think that the {0} service has recovered.", ServiceName);

                        failureCount = 0;
                        recoveryTime = DateTime.MinValue;
                    }
                }
			}

            internal bool MarkFailed()
            {
                lock (syncRoot)
                {
                    if (!failed) 
                    {
                        failed = true;
                        failureCount++;

                        long reconnectDelay;
                        if (!parent.UseExponentialBackOff) 
                        {
                            reconnectDelay = parent.InitialReconnectDelay;
                        } 
                        else 
                        {
                            reconnectDelay = (long)Math.Pow(parent.BackOffMultiplier, failureCount);
                            reconnectDelay = Math.Min(reconnectDelay, parent.MaxReconnectDelay);
                        }

                        Tracer.DebugFormat("Remote failure of {0} while still receiving multicast advertisements.  " +
                                           "Advertising events will be suppressed for {1} ms, the current " +
                                           "failure count is: {2}", ServiceName, reconnectDelay, failureCount);

                        recoveryTime = DateTime.Now + TimeSpan.FromMilliseconds(reconnectDelay);
                        return true;
                    }
                }
                return false;            
            }

            /// <summary>
            /// Returns true if this Broker has been marked as failed and it is now time to
            /// start a recovery attempt.
            /// </summary>
            public bool IsTimeForRecovery() 
            {
                lock (syncRoot)
                {
                    if (!failed) 
                    {
                        return false;
                    }

                    int maxReconnectAttempts = parent.MaxReconnectAttempts;

                    // Are we done trying to recover this guy?
                    if (maxReconnectAttempts > 0 && failureCount > maxReconnectAttempts) 
                    {
                        Tracer.DebugFormat("Max reconnect attempts of the {0} service has been reached.", ServiceName);
                        return false;
                    }

                    // Is it not yet time?
                    if (DateTime.Now < recoveryTime) 
                    {
                        return false;
                    }

                    Tracer.DebugFormat("Resuming event advertisement of the {0} service.", ServiceName);

                    failed = false;
                    return true;
                }
            }
		}
	}
}
