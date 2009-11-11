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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.Policies;

namespace Apache.NMS.ActiveMQ
{
	/// <summary>
	/// Represents a connection with a message broker
	/// </summary>
	public class ConnectionFactory : IConnectionFactory
	{
		public const string DEFAULT_BROKER_URL = "failover:tcp://localhost:61616";
		public const string ENV_BROKER_URL = "ACTIVEMQ_BROKER_URL";

		private static event ExceptionListener onException;
		private Uri brokerUri;
		private string connectionUserName;
		private string connectionPassword;
		private string clientId;
        private bool useCompression;
        
        private IRedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
        private PrefetchPolicy prefetchPolicy = new PrefetchPolicy();
        private ICompressionPolicy compressionPolicy = new CompressionPolicy();

		static ConnectionFactory()
		{
			TransportFactory.OnException += ConnectionFactory.ExceptionHandler;
		}

		public static string GetDefaultBrokerUrl()
		{
#if (PocketPC||NETCF||NETCF_2_0)
			return DEFAULT_BROKER_URL;
#else
			return Environment.GetEnvironmentVariable(ENV_BROKER_URL) ?? DEFAULT_BROKER_URL;
#endif
		}

		public ConnectionFactory()
			: this(GetDefaultBrokerUrl())
		{
		}

		public ConnectionFactory(string brokerUri)
			: this(brokerUri, null)
		{
		}

		public ConnectionFactory(string brokerUri, string clientID)
			: this(new Uri(brokerUri), clientID)
		{
		}

		public ConnectionFactory(Uri brokerUri)
			: this(brokerUri, null)
		{
		}

		public ConnectionFactory(Uri brokerUri, string clientID)
		{
			this.brokerUri = brokerUri;
			this.clientId = clientID;
		}

		public IConnection CreateConnection()
		{
			return CreateConnection(connectionUserName, connectionPassword);
		}

		public IConnection CreateConnection(string userName, string password)
		{
			// Strip off the activemq prefix, if it exists.
			Uri uri = new Uri(URISupport.stripPrefix(brokerUri.OriginalString, "activemq:"));

			Tracer.InfoFormat("Connecting to: {0}", uri.ToString());

			ConnectionInfo info = CreateConnectionInfo(userName, password);
			ITransport transport = TransportFactory.CreateTransport(uri);
			Connection connection = new Connection(uri, transport, info);

            // Set the Factory level configuration to the Connection, this can be overriden by
            // the params on the Connection URI so we do this before applying the params.
            connection.UseCompression = this.useCompression;
            connection.RedeliveryPolicy = this.redeliveryPolicy.Clone() as IRedeliveryPolicy;
            connection.PrefetchPolicy = this.prefetchPolicy.Clone() as PrefetchPolicy;
            connection.CompressionPolicy = this.compressionPolicy.Clone() as ICompressionPolicy;
            
			// Set properties on connection using parameters prefixed with "connection."
			// Since this could be a composite Uri, assume the connection-specific parameters
			// are associated with the outer-most specification of the composite Uri. What's nice
			// is that this works with simple Uri as well.
			URISupport.CompositeData c = URISupport.parseComposite(uri);
			URISupport.SetProperties(connection, c.Parameters, "connection.");
            
			connection.ITransport.Start();
			return connection;
		}

		// Properties

		/// <summary>
		/// Get/or set the broker Uri.
		/// </summary>
		public Uri BrokerUri
		{
			get { return brokerUri; }
			set { brokerUri = value; }
		}

		public string UserName
		{
			get { return connectionUserName; }
			set { connectionUserName = value; }
		}

		public string Password
		{
			get { return connectionPassword; }
			set { connectionPassword = value; }
		}

		public string ClientId
		{
			get { return clientId; }
			set { clientId = value; }
		}

        public bool UseCompression
        {
            get { return this.useCompression; }
            set { this.useCompression = value; }
        }

        public IRedeliveryPolicy RedeliveryPolicy
        {
            get { return this.redeliveryPolicy; }
            set 
            {
                if(value != null)
                {
                    this.redeliveryPolicy = value; 
                }
            }
        }

        public ICompressionPolicy CompressionPolicy
        {
            get { return this.compressionPolicy; }
            set 
            {
                if(value != null)
                {
                    this.compressionPolicy = value; 
                }
            }
        }        

		public event ExceptionListener OnException
		{
			add { onException += value; }
			remove
			{
				if(onException != null)
				{
					onException -= value;
				}
			}
		}

		protected virtual ConnectionInfo CreateConnectionInfo(string userName, string password)
		{
			ConnectionInfo answer = new ConnectionInfo();
			ConnectionId connectionId = new ConnectionId();
			connectionId.Value = CreateNewGuid();

			answer.ConnectionId = connectionId;
			answer.UserName = userName;
			answer.Password = password;
			answer.ClientId = clientId ?? CreateNewGuid();

			return answer;
		}

		protected static string CreateNewGuid()
		{
			return Guid.NewGuid().ToString();
		}

		protected static void ExceptionHandler(Exception ex)
		{
			if(ConnectionFactory.onException != null)
			{
				ConnectionFactory.onException(ex);
			}
		}
	}
}
