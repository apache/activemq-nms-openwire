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
using System.Threading;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.Util;
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture()]
	public class AMQNET375Test : NMSTestSupport
	{
		#region Constants

		private static readonly String BrokerUri = "activemq:failover:(tcp://${activemqhost}:61616?nms.PrefetchPolicy.queuePrefetch=0&keepAlive=true&wireFormat.TightEncodingEnabled=false&wireFormat.CacheEnabled=false&wireFormat.MaxInactivityDuration=300000)";
		private const string Queue = "TestQueue?consumer.prefetchSize=0";
		private const string TextMessage = "The quick brown fox jumps over the lazy dog.";
		
		private readonly int COUNT = 10;
		
		private int sent = 0;
		private int received = 0;
		
		#endregion

		[TestCase]
		public void TestZeroPrefetchConsumerGetsAllMessages()
		{
			Send(COUNT);
			Receive(COUNT);
			
			Assert.AreEqual(sent, received);
		}

		private void Receive(int numberOfMessages)
		{
            IConnectionFactory connectionFactory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(BrokerUri));
			using (IConnection connection = connectionFactory.CreateConnection())
			{
				connection.Start();

				connection.ConnectionInterruptedListener += OnConnectionInterrupted;
				connection.ConnectionResumedListener += OnConnectionResumed;

				using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
				{
					IQueue queue = session.GetQueue(Queue);

					using (IMessageConsumer consumer = session.CreateConsumer(queue))
					{
						for (int i = 0; i < numberOfMessages; i++)
						{
							IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(5000));
							Assert.IsNotNull(message);
							Tracer.Debug("Received message.");
							received++;
						}
					}
				}

				connection.ConnectionInterruptedListener -= OnConnectionInterrupted;
				connection.ConnectionResumedListener -= OnConnectionResumed;
			}
		}

		private void Send(int numberOfMessages)
		{
            IConnectionFactory connectionFactory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(BrokerUri));
			using (IConnection connection = connectionFactory.CreateConnection())
			{
				connection.Start();

				using (ISession session = connection.CreateSession())
				{
					IQueue queue = session.GetQueue(Queue);

					using (IMessageProducer producer = session.CreateProducer(queue))
					{
						producer.DeliveryMode = MsgDeliveryMode.Persistent;

						ITextMessage message = producer.CreateTextMessage(TextMessage);

						for (int i=0; i < numberOfMessages; i++)
						{
							producer.Send(message);
							Tracer.Debug("Sent message.");
							sent++;
						}
					}
				}
			}
		}

		private static void OnConnectionInterrupted()
		{
			Tracer.Debug("AMQ event: Connection was interrupted on the receiver end.");
		}

		/// <summary>
		/// Called when connection is resumed.
		/// </summary>
		private static void OnConnectionResumed()
		{
			Tracer.Debug("AMQ event: Connection was resumed on the receiver end.");
		}
	}
}

