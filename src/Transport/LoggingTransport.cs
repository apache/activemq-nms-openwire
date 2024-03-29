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
using System.Threading.Tasks;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Util.Synchronization;

namespace Apache.NMS.ActiveMQ.Transport
{
	
	/// <summary>
	/// A Transport filter that is used to log the commands sent and received.
	/// </summary>
	public class LoggingTransport : TransportFilter
    {
		public LoggingTransport(ITransport next) : base(next) {
		}
		
		protected override async Task OnCommand(ITransport sender, Command command) {
			Tracer.Info("RECEIVED: " + command);
			await this.commandHandlerAsync(sender, command).Await();
		}
		
		protected override void OnException(ITransport sender, Exception error) {
			Tracer.Error("RECEIVED Exception: " + error);
			this.exceptionHandler(sender, error);
		}
		
		public override void Oneway(Command command)
		{
			Tracer.Info("SENDING: " + command);
			this.next.Oneway(command);
		}
				
    }
}

