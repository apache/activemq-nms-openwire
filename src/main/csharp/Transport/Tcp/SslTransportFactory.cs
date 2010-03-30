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
using System.Net;
using System.Net.Sockets;

namespace Apache.NMS.ActiveMQ.Transport.Tcp
{
	public class SslTransportFactory : TcpTransportFactory
	{
        private string serverName;
        private string clientCertLocation;
        private string clientCertPassword;
        private bool acceptInvalidBrokerCert = false;
        
        public SslTransportFactory() : base()
        {
        }

        public string ServerName
        {
            get { return this.serverName; }
            set { this.serverName = value; }
        }

        public string ClientCertLocation
        {
            get { return this.clientCertLocation; }
            set { this.clientCertLocation = value; }
        }

        public string ClientCertPassword
        {
            get { return this.clientCertPassword; }
            set { this.clientCertPassword = value; }
        }        

        public bool AcceptInvalidBrokerCert
        {
            get { return this.acceptInvalidBrokerCert; }
            set { this.acceptInvalidBrokerCert = value; }
        }
        
		protected override ITransport DoCreateTransport(Uri location, Socket socket, IWireFormat wireFormat )
		{
            Tracer.Debug("Creating new instance of the SSL Transport.");
			SslTransport transport = new SslTransport(location, socket, wireFormat);
            
            transport.ClientCertLocation = ClientCertLocation;
            transport.ClientCertPassword = ClientCertPassword;
            transport.AcceptInvalidBrokerCert = AcceptInvalidBrokerCert;
            
            return transport;
		}		
	}
}
