/**
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

using Apache.NMS.ActiveMQ.Transport.Failover;
using Apache.NMS.ActiveMQ.Transport.Tcp;

namespace Apache.NMS.ActiveMQ.Transport
{
	public class TransportFactory
	{

		private static Dictionary<String, ITransportFactory> TRANSPORT_FACTORYS = new Dictionary<String, ITransportFactory>();

		static TransportFactory()
		{
			TRANSPORT_FACTORYS.Add("tcp", new TcpTransportFactory());
			TRANSPORT_FACTORYS.Add("failover", new FailoverTransportFactory());
		}

		/// <summary>
		/// Creates a normal transport. 
		/// </summary>
		/// <param name="location"></param>
		/// <returns>the transport</returns>
		public static ITransport CreateTransport(Uri location)
		{
			ITransportFactory tf = findTransportFactory(location);
			return tf.CreateTransport(location);
		}

		public static ITransport CompositeConnect(Uri location)
		{
			ITransportFactory tf = findTransportFactory(location);
			return tf.CompositeConnect(location);
		}

		/// <summary>
		/// </summary>
		/// <param name="location"></param>
		/// <returns></returns>
		private static ITransportFactory findTransportFactory(Uri location)
		{
			String scheme = location.Scheme;
			if(scheme == null)
			{
				throw new IOException("Transport not scheme specified: [" + location + "]");
			}
			ITransportFactory tf = TRANSPORT_FACTORYS[scheme];
			if(tf == null)
			{
				throw new ApplicationException("Transport Factory for " + scheme + " does not exist.");
			}
			return tf;
		}
	}
}
