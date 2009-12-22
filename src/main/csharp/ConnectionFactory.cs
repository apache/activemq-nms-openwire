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
using Apache.NMS.ActiveMQ.Util;
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
        private string clientIdPrefix;
        private IdGenerator clientIdGenerator;

        private bool useCompression;
        private bool copyMessageOnSend = true;
        private bool dispatchAsync=true;
        private bool asyncSend;
        private bool asyncClose;
        private bool alwaysSyncSend;
        private bool sendAcksAsync=true;
        private AcknowledgementMode acknowledgementMode = AcknowledgementMode.AutoAcknowledge;

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
			this.BrokerUri = brokerUri;
			this.ClientId = clientID;
		}

		public IConnection CreateConnection()
		{
			return CreateConnection(connectionUserName, connectionPassword);
		}

		public IConnection CreateConnection(string userName, string password)
		{
            Connection connection = null;

            try
            {
    			// Strip off the activemq prefix, if it exists.
    			Uri uri = new Uri(URISupport.stripPrefix(brokerUri.OriginalString, "activemq:"));
    
    			Tracer.InfoFormat("Connecting to: {0}", uri.ToString());
    
                ITransport transport = TransportFactory.CreateTransport(uri);
    
                connection = new Connection(uri, transport, this.ClientIdGenerator);
    
                ConfigureConnection(connection);
    
                connection.UserName = this.connectionUserName;
                connection.Password = this.connectionPassword;
    
                if(this.clientId != null)
                {
                    connection.DefaultClientId = this.clientId;
                }
    
    			connection.ITransport.Start();
    
    			return connection;
            }
            catch(NMSException e)
            {
                try
                {
                    connection.Close();
                }
                catch
                {
                }

                throw e;
            }
            catch(Exception e)
            {
                try
                {
                    connection.Close();
                }
                catch
                {
                }

                throw NMSExceptionSupport.Create("Could not connect to broker URL: " + this.brokerUri + ". Reason: " + e.Message, e);
            }
		}

        #region ConnectionFactory Properties

		/// <summary>
		/// Get/or set the broker Uri.
		/// </summary>
		public Uri BrokerUri
		{
			get { return brokerUri; }
			set
            {
                brokerUri = value;

                Uri uri = new Uri(URISupport.stripPrefix(brokerUri.OriginalString, "activemq:"));

                URISupport.CompositeData c = URISupport.parseComposite(uri);
                URISupport.SetProperties(this, c.Parameters, "connection.");
                URISupport.SetProperties(this.PrefetchPolicy, c.Parameters, "nms.PrefetchPolicy.");
                URISupport.SetProperties(this.RedeliveryPolicy, c.Parameters, "nms.RedeliveryPolicy.");
            }
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

        public string ClientIdPrefix
        {
            get { return clientIdPrefix; }
            set { clientIdPrefix = value; }
        }

        public bool UseCompression
        {
            get { return this.useCompression; }
            set { this.useCompression = value; }
        }

        public bool CopyMessageOnSend
        {
            get { return copyMessageOnSend; }
            set { copyMessageOnSend = value; }
        }

        public bool AlwaysSyncSend
        {
            get { return alwaysSyncSend; }
            set { alwaysSyncSend = value; }
        }

        public bool AsyncClose
        {
            get { return asyncClose; }
            set { asyncClose = value; }
        }

        public bool SendAcksAsync
        {
            get { return sendAcksAsync; }
            set { sendAcksAsync = value; }
        }

        public bool AsyncSend
        {
            get { return asyncSend; }
            set { asyncSend = value; }
        }

        public bool DispatchAsync
        {
            get { return this.dispatchAsync; }
            set { this.dispatchAsync = value; }
        }

        public string AckMode
        {
            set { this.acknowledgementMode = NMSConvert.ToAcknowledgementMode(value); }
        }

        public AcknowledgementMode AcknowledgementMode
        {
            get { return acknowledgementMode; }
            set { this.acknowledgementMode = value; }
        }

        public PrefetchPolicy PrefetchPolicy
        {
            get { return this.prefetchPolicy; }
            set { this.prefetchPolicy = value; }
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

        public IdGenerator ClientIdGenerator
        {
            set { this.clientIdGenerator = value; }
            get
            {
                lock(this)
                {
                    if(this.clientIdGenerator == null)
                    {
                        if(this.clientIdPrefix != null)
                        {
                            this.clientIdGenerator = new IdGenerator(this.clientIdPrefix);
                        }
                        else
                        {
                            this.clientIdGenerator = new IdGenerator();
                        }
                    }

                    return this.clientIdGenerator;
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

        #endregion

        protected virtual void ConfigureConnection(Connection connection)
        {
            connection.AsyncClose = this.AsyncClose;
            connection.AsyncSend = this.AsyncSend;
            connection.CopyMessageOnSend = this.CopyMessageOnSend;
            connection.AlwaysSyncSend = this.AlwaysSyncSend;
            connection.DispatchAsync = this.DispatchAsync;
            connection.SendAcksAsync = this.SendAcksAsync;
            connection.AcknowledgementMode = this.acknowledgementMode;
            connection.UseCompression = this.useCompression;
            connection.RedeliveryPolicy = this.redeliveryPolicy.Clone() as IRedeliveryPolicy;
            connection.PrefetchPolicy = this.prefetchPolicy.Clone() as PrefetchPolicy;
            connection.CompressionPolicy = this.compressionPolicy.Clone() as ICompressionPolicy;
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
