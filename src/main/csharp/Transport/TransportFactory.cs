/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;

using Apache.NMS.ActiveMQ.Transport.Discovery;
using Apache.NMS.ActiveMQ.Transport.Failover;
using Apache.NMS.ActiveMQ.Transport.Tcp;

namespace Apache.NMS.ActiveMQ.Transport
{
	public class TransportFactory
	{
		public static event ExceptionListener OnException;

		public static void HandleException(Exception ex)
		{
			if(TransportFactory.OnException != null)
			{
				TransportFactory.OnException(ex);
			}
		}

		/// <summary>
		/// Creates a normal transport. 
		/// </summary>
		/// <param name="location"></param>
		/// <returns>the transport</returns>
		public static ITransport CreateTransport(Uri location)
		{
			ITransportFactory tf = TransportFactory.CreateTransportFactory(location);
			return tf.CreateTransport(location);
		}

		public static ITransport CompositeConnect(Uri location)
		{
			ITransportFactory tf = TransportFactory.CreateTransportFactory(location);
			return tf.CompositeConnect(location);
		}

		/// <summary>
		/// Create a transport factory for the scheme.  If we do not support the transport protocol,
		/// an NMSConnectionException will be thrown.
		/// </summary>
		/// <param name="location"></param>
		/// <returns></returns>
		private static ITransportFactory CreateTransportFactory(Uri location)
		{
			string scheme = location.Scheme;

			if(null == scheme || 0 == scheme.Length)
			{
				throw new NMSConnectionException(String.Format("Transport scheme invalid: [{0}]", location.ToString()));
			}

			ITransportFactory factory = null;

			try
			{
				switch(scheme.ToLower())
				{
					case "tcp":
						factory = new TcpTransportFactory();
						break;
					case "discovery":
						factory = new DiscoveryTransportFactory();
						break;
					case "failover":
						factory = new FailoverTransportFactory();
						break;
					default:
						throw new NMSConnectionException(String.Format("The transport {0} is not supported.", scheme));
				}
			}
			catch(NMSConnectionException)
			{
				throw;
			}
			catch
			{
				throw new NMSConnectionException("Error creating transport.");
			}

			if(null == factory)
			{
				throw new NMSConnectionException("Unable to create a transport.");
			}

			return factory;
		}
	}
}
