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
using System.Threading;
using Apache.NMS.ActiveMQ.Transport.Discovery.Multicast;
using Apache.NMS.ActiveMQ.Transport.Tcp;

namespace Apache.NMS.ActiveMQ.Transport.Discovery
{
	public class DiscoveryTransportFactory : ITransportFactory
	{
		private const int TIMEOUT_IN_SECONDS = 20;

		private static Uri discoveredUri;
		private static MulticastDiscoveryAgent agent;
		private static string currentServiceName;
		private static readonly object uriLock = new object();
		public static event ExceptionListener OnException;

		public DiscoveryTransportFactory()
		{
			currentServiceName = String.Empty;
		}

		private static void agent_OnNewServiceFound(string brokerName, string serviceName)
		{
			lock(uriLock)
			{
				if(discoveredUri == null)
				{
					currentServiceName = serviceName;
					discoveredUri = new Uri(currentServiceName);
				}

				// This will end the wait in the CreateTransport method.
				Monitor.Pulse(uriLock);
			}
		}

		private static void agent_OnServiceRemoved(string brokerName, string serviceName)
		{
			if(serviceName == currentServiceName)
			{
				lock(uriLock)
				{
					discoveredUri = null;
				}

				if(OnException != null)
				{
					OnException(new Exception("Broker is dead!"));
				}
			}
		}

		private static MulticastDiscoveryAgent Agent
		{
			get
			{
				if(agent == null)
				{
					agent = new MulticastDiscoveryAgent();
					agent.OnNewServiceFound += agent_OnNewServiceFound;
					agent.OnServiceRemoved += agent_OnServiceRemoved;
				}

				return agent;
			}
		}

		#region Overloaded FailoverTransportFactory Members

		public ITransport CreateTransport(Uri location)
		{
			if(!Agent.IsStarted)
			{
				Agent.Start();
			}

			DateTime expireTime = DateTime.Now.AddSeconds(TIMEOUT_IN_SECONDS);

			// If a new broker is found the agent will fire an event which will result in discoveredUri being set.
			lock(uriLock)
			{
				while(discoveredUri == null)
				{
					if(expireTime < DateTime.Now)
					{
						throw new NMSConnectionException(
							"Unable to find a connection before the timeout period expired.");
					}

					Monitor.Wait(uriLock, TIMEOUT_IN_SECONDS * 1000);
				}
			}

			ITransport transport;

			lock(uriLock)
			{
				TcpTransportFactory tcpTransFactory = new TcpTransportFactory();

				transport = tcpTransFactory.CreateTransport(new Uri(discoveredUri + location.Query));
			}

			return transport;
		}

		public ITransport CompositeConnect(Uri location)
		{
			throw new NMSConnectionException("Composite connection not supported with Discovery transport.");
		}

		#endregion
	}
}
