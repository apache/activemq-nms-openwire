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
using System.Reflection;
using System.Collections.Generic;
using Apache.NMS.ActiveMQ.Transport.Discovery;
using Apache.NMS.ActiveMQ.Transport.Failover;
using Apache.NMS.ActiveMQ.Transport.Mock;
using Apache.NMS.ActiveMQ.Transport.Tcp;

namespace Apache.NMS.ActiveMQ.Transport
{
	public class TransportFactory
	{
		public static event ExceptionListener OnException;

        private static IDictionary<String, String> schemeToClassName = new Dictionary<String, String>();
        private static IDictionary<String, Type> schemeToTypeName = new Dictionary<String, Type>();

		public static void HandleException(Exception ex)
		{
			if(TransportFactory.OnException != null)
			{
				TransportFactory.OnException(ex);
			}
		}

        public static void RegisterTransport(string scheme, string className)
        {
            if(String.IsNullOrEmpty(scheme))
            {
                throw new NMSException("Cannot register a transport with an empty scheme.");
            }

            if(String.IsNullOrEmpty(className))
            {
                throw new NMSException("Cannot register a transport with an empty className.");
            }

            TransportFactory.schemeToClassName[scheme.ToLower()] = className;
        }

        public static void RegisterTransport(string scheme, Type factoryType)
        {
            if(String.IsNullOrEmpty(scheme))
            {
                throw new NMSException("Cannot register a transport with an empty scheme.");
            }

            if(factoryType == null)
            {
                throw new NMSException("Cannot register a transport with an empty type name.");
            }

            TransportFactory.schemeToTypeName[scheme.ToLower()] = factoryType;
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

		public static ITransport AsyncCompositeConnect(Uri location, SetTransport setTransport)
		{
			ITransportFactory tf = TransportFactory.CreateTransportFactory(location);
			return tf.CompositeConnect(location, setTransport);
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

			if(string.IsNullOrEmpty(scheme))
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
                case "ssl":
                    factory = new SslTransportFactory();
                    break;
                case "discovery":
					factory = new DiscoveryTransportFactory();
					break;
				case "failover":
					factory = new FailoverTransportFactory();
					break;
				case "mock":
					factory = new MockTransportFactory();
					break;
				default:

                    // Types are easier lets check them first.
                    if(TransportFactory.schemeToTypeName.ContainsKey(scheme))
                    {
                        Type objectType = schemeToTypeName[scheme];
                        factory = CreateTransportByObjectType(objectType);
                        break;
                    }

                    // Now we can look for Class names, may have to search lots of assemblies
                    // for this one.
                    if(TransportFactory.schemeToClassName.ContainsKey(scheme))
                    {
                        String className = schemeToClassName[scheme];
                        factory = CreateTransportByClassName(className);
                        break;
                    }

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

        private static ITransportFactory CreateTransportByObjectType(Type objectType)
        {
            Tracer.Debug("Attempting to create a Transport by its Object Type: " + objectType.FullName);

            try
            {
                return Activator.CreateInstance(objectType) as ITransportFactory;
            }
            catch
            {
                throw new NMSConnectionException("Could not create the transport with type name: " + objectType);
            }
        }

        private static ITransportFactory CreateTransportByClassName(String className)
        {
            Tracer.Debug("Attempting to create a Transport by its Class Name: " + className);

            try
            {
                Assembly assembly = Assembly.GetExecutingAssembly();
                Type type = assembly.GetType(className, false);

                if(type == null)
                {
                    Assembly[] loadedAssemblies = AppDomain.CurrentDomain.GetAssemblies();

                    foreach(Assembly dll in loadedAssemblies)
                    {
                        Tracer.DebugFormat("Checking assembly {0} for class named {1}.", dll.FullName, className);
                        type = dll.GetType(className, false);

                        if(type != null)
                        {
                            break;
                        }
                    }
                }

                return Activator.CreateInstance(type) as ITransportFactory;
            }
            catch
            {
                throw new NMSConnectionException("Could not create the transport with type name: " + className);
            }
        }
	}
}
