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
using Apache.NMS.Test;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class PrefetchSizeZeroTest : NMSTestSupport
	{	
        protected static string DESTINATION_NAME = "TEST.PrefetchSizeZero";
			
		[Test]
		public void TestZeroPrefetchSize()
		{
			using(IConnection connection = CreateConnection())
			{
				(connection as Apache.NMS.ActiveMQ.Connection).PrefetchPolicy.All = 0;

				connection.Start();
				using(Session session = (Session)connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
				{
					IDestination destination = SessionUtil.GetDestination(session, DESTINATION_NAME);
					using(IMessageConsumer consumer = session.CreateConsumer(destination))
					using(IMessageProducer producer = session.CreateProducer(destination))
					{
						SendMessage(producer);
                        SendMessage(producer);

						IMessage receivedMsg = consumer.Receive(TimeSpan.FromSeconds(5));
                        Assert.IsNotNull(receivedMsg);
                        receivedMsg = consumer.Receive(TimeSpan.FromSeconds(5));
                        Assert.IsNotNull(receivedMsg);
						receivedMsg = consumer.Receive(TimeSpan.FromSeconds(5));
                        Assert.IsNull(receivedMsg);

						// Send another message.
						SendMessage(producer);
                        receivedMsg = consumer.Receive(TimeSpan.FromSeconds(5));
						Assert.IsNotNull(receivedMsg);
                   
					}
				}
			}
		}
	
		protected void SendMessage(IMessageProducer producer)
		{
			IMessage request = producer.CreateMessage();
			request.NMSType = "Test";
			producer.Send(request);
		}
	}
}
