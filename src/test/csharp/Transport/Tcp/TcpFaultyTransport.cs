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
using System.IO;
using System.Net.Sockets;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.Transport.Tcp
{
    public class TcpFaultyTransport : TcpTransport
    {		
		private CommandHandler onewayCommandPreProcessor;
		private CommandHandler onewayCommandPostProcessor;
				
        public TcpFaultyTransport(Uri location, Socket socket, IWireFormat wireFormat) :
            base(location, socket, wireFormat)
        {
        }

        ~TcpFaultyTransport()
        {
            Dispose(false);
        }		

		public CommandHandler OnewayCommandPreProcessor 
		{
			get { return this.onewayCommandPreProcessor; }
			set { this.onewayCommandPreProcessor = value; }
		}

		public CommandHandler OnewayCommandPostProcessor 
		{
			get { return this.onewayCommandPostProcessor; }
			set { this.onewayCommandPostProcessor = value; }
		}

        public override void Oneway(Command command)
        {
            if(this.onewayCommandPreProcessor != null)
            {
                this.onewayCommandPreProcessor(this, command);
            }

            base.Oneway(command);

            if(this.onewayCommandPostProcessor != null)
            {
                this.onewayCommandPostProcessor(this, command);
            }
        }
    }
}
