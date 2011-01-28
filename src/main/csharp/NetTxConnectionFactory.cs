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
using System.Collections.Specialized;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Transport;

namespace Apache.NMS.ActiveMQ
{
    public class NetTxConnectionFactory : ConnectionFactory, INetTxConnectionFactory
    {
        private NetTxRecoveryPolicy txRecoveryPolicy;

        public NetTxConnectionFactory() : base(GetDefaultBrokerUrl())
        {
        }

        public NetTxConnectionFactory(string brokerUri) : base(brokerUri, null)
        {
        }

        public NetTxConnectionFactory(string brokerUri, string clientID)
            : base(brokerUri, clientID)
        {
        }

        public NetTxConnectionFactory(Uri brokerUri)
            : base(brokerUri, null)
        {
        }

        public NetTxConnectionFactory(Uri brokerUri, string clientID)
            : base(brokerUri, clientID)
        {
        }

        public INetTxConnection CreateNetTxConnection()
        {
            return (INetTxConnection) base.CreateActiveMQConnection();
        }

        public INetTxConnection CreateNetTxConnection(string userName, string password)
        {
            return (INetTxConnection) base.CreateActiveMQConnection(userName, password);
        }

        protected override Connection CreateActiveMQConnection(ITransport transport)
        {
            NetTxConnection connection = new NetTxConnection(this.BrokerUri, transport, this.ClientIdGenerator);

            Uri brokerUri = this.BrokerUri;

            // Set properties on the Receovery Policy using parameters prefixed with "nms.RecoveryPolicy."
            if(!String.IsNullOrEmpty(brokerUri.Query) && !brokerUri.OriginalString.EndsWith(")"))
            {
                string query = brokerUri.Query.Substring(brokerUri.Query.LastIndexOf(")") + 1);
                StringDictionary options = URISupport.ParseQuery(query);
                options = URISupport.GetProperties(options, "nms.RecoveryPolicy.");
                URISupport.SetProperties(this.txRecoveryPolicy, options);
            }

            return connection;
        }

        public NetTxRecoveryPolicy TxRecoveryPolicy
        {
            get { return this.txRecoveryPolicy; }
            set { this.txRecoveryPolicy = value; }
        }
    }
}

