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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.State
{
	public class SessionState
	{
		SessionInfo info;

		private SynchronizedDictionary<ProducerId, ProducerState> producers = new SynchronizedDictionary<ProducerId, ProducerState>();
		private SynchronizedDictionary<ConsumerId, ConsumerState> consumers = new SynchronizedDictionary<ConsumerId, ConsumerState>();
		private AtomicBoolean _shutdown = new AtomicBoolean(false);

		public SessionState(SessionInfo info)
		{
			this.info = info;
		}

		public override String ToString()
		{
			return info.ToString();
		}

		public void addProducer(ProducerInfo info)
		{
			checkShutdown();
			producers.Add(info.ProducerId, new ProducerState(info));
		}

		public ProducerState removeProducer(ProducerId id)
		{
			ProducerState ret = producers[id];
			producers.Remove(id);
			return ret;
		}

		public void addConsumer(ConsumerInfo info)
		{
			checkShutdown();
			consumers.Add(info.ConsumerId, new ConsumerState(info));
		}

		public ConsumerState removeConsumer(ConsumerId id)
		{
			ConsumerState ret = consumers[id];
			consumers.Remove(id);
			return ret;
		}

		public SessionInfo Info
		{
			get
			{
				return info;
			}
		}

		public SynchronizedCollection<ConsumerId> ConsumerIds
		{
			get
			{
				return consumers.Keys;
			}
		}

		public SynchronizedCollection<ProducerId> ProducerIds
		{
			get
			{
				return producers.Keys;
			}
		}

		public SynchronizedCollection<ProducerState> ProducerStates
		{
			get
			{
				return producers.Values;
			}
		}

		public ProducerState getProducerState(ProducerId producerId)
		{
			return producers[producerId];
		}

		public ProducerState this[ProducerId producerId]
		{
			get
			{
				return producers[producerId];
			}
		}

		public SynchronizedCollection<ConsumerState> ConsumerStates
		{
			get
			{
				return consumers.Values;
			}
		}

		public ConsumerState getConsumerState(ConsumerId consumerId)
		{
			return consumers[consumerId];
		}

		public ConsumerState this[ConsumerId consumerId]
		{
			get
			{
				return consumers[consumerId];
			}
		}

		private void checkShutdown()
		{
			if(_shutdown.Value)
			{
				throw new ApplicationException("Disposed");
			}
		}

		public void shutdown()
		{
			_shutdown.Value = false;
		}

	}
}
