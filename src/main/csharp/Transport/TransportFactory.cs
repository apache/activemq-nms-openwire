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
		private static readonly Dictionary<String, ITransportFactory> factoryCache;
		public static event ExceptionListener OnException;

		static TransportFactory()
		{
			TransportFactory.factoryCache = new Dictionary<string, ITransportFactory>();
		}

		private static void HandleException(Exception ex)
		{
			if(TransportFactory.OnException != null)
			{
				TransportFactory.OnException(ex);
			}
		}

		private static ITransportFactory AddTransportFactory(string scheme)
		{
			ITransportFactory factory;

			switch(scheme)
			{
				case "tcp":
					factory = new TcpTransportFactory();
					break;
				case "discovery":
					factory = new DiscoveryTransportFactory();
					DiscoveryTransportFactory.OnException += TransportFactory.HandleException;
					break;
				case "failover":
					factory = new FailoverTransportFactory();
					break;
				default:
					throw new ApplicationException("The transport " + scheme + " is not supported.");
			}

			if(null == factory)
			{
				throw new ApplicationException("Unable to create a transport.");
			}

			TransportFactory.factoryCache.Add(scheme, factory);
			return factory;
		}

		/// <summary>
		/// Creates a normal transport. 
		/// </summary>
		/// <param name="location"></param>
		/// <returns>the transport</returns>
		public static ITransport CreateTransport(Uri location)
		{
			ITransportFactory tf = TransportFactory.findTransportFactory(location);
			return tf.CreateTransport(location);
		}

		public static ITransport CompositeConnect(Uri location)
		{
			ITransportFactory tf = TransportFactory.findTransportFactory(location);
			return tf.CompositeConnect(location);
		}

		/// <summary>
		/// Find the transport factory for the scheme.  We will cache the transport
		/// factory in a lookup table.  If we do not support the transport protocol,
		/// an ApplicationException will be thrown.
		/// </summary>
		/// <param name="location"></param>
		/// <returns></returns>
		private static ITransportFactory findTransportFactory(Uri location)
		{
			string scheme = location.Scheme;

			if(null == scheme)
			{
				throw new IOException("Transport not scheme specified: [" + location + "]");
			}

			ITransportFactory tf;

			scheme = scheme.ToLower();
			if(!TransportFactory.factoryCache.TryGetValue(scheme, out tf))
			{
			    // missing in the cache - go add request it if it exists
			    tf = TransportFactory.AddTransportFactory(scheme);
			}

			return tf;
		}
	}
}
