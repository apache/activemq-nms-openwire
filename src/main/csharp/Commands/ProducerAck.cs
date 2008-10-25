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
using Apache.NMS.ActiveMQ.State;

namespace Apache.NMS.ActiveMQ.Commands
{
	/// <summary>
	/// A ProducerAck command is sent by a broker to a producer to let it know it has
	/// received and processed messages that it has produced. The producer will be
	/// flow controlled if it does not receive ProducerAck commands back from the
	/// broker.
	/// </summary>
	public class ProducerAck : BaseCommand
	{
		protected ProducerId myProducerId;
		protected int mySize;

		public ProducerAck()
		{
		}

		public ProducerAck(ProducerId producerId, int size)
		{
			this.myProducerId = producerId;
			this.mySize = size;
		}

		public override Response visit(ICommandVisitor visitor)
		{
			return visitor.processProducerAck(this);
		}

		/// <summary>
		/// The producer id that this ack message is destined for.
		/// </summary>
		public ProducerId ProducerId
		{
			get
			{
				return myProducerId;
			}
			set
			{
				myProducerId = value;
			}
		}

		/// <summary>
		/// The number of bytes that are being acked.
		/// </summary>
		public int Size
		{
			get
			{
				return mySize;
			}
			set
			{
				mySize = value;
			}
		}
	}
}
