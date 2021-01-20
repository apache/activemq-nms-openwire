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
using System.Timers;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class MessageProducerTest
	{
		[Test]
		public void TestProducerSendWithTimeout()
		{
			int timeout = 1500;
			Uri uri = new Uri(string.Format("mock://localhost:61616?connection.RequestTimeout={0}&transport.respondToMessages=false", timeout));

			ConnectionFactory factory = new ConnectionFactory(uri);
			using(IConnection connection = factory.CreateConnection())
			using(ISession session = connection.CreateSession())
			{
				IDestination destination = session.GetTopic("Test");
				using(IMessageProducer producer = session.CreateProducer(destination))
				{
					ITextMessage message = session.CreateTextMessage("Hello World");

					for(int i = 0; i < 10; ++i)
					{
						DateTime start = DateTime.Now;

						try
						{
							producer.Send(message);
							Assert.Fail("Expected a RequestTimedOutException");
						}
						catch(RequestTimedOutException)
						{
						}
						
						TimeSpan elapsed = DateTime.Now - start;
						// Make sure we timed out.
						Assert.GreaterOrEqual((int) elapsed.TotalMilliseconds, timeout - 75, "Did not reach timeout limit.");
					}
				}
			}
		}

		[Test]
		public void TestCopyOnSend()
		{
			Uri uri = new Uri("mock://localhost:61616?connection.CopyMessageOnSend=true");

			ConnectionFactory factory = new ConnectionFactory(uri);
			using(IConnection connection = factory.CreateConnection())
			using(ISession session = connection.CreateSession())
			{
				IDestination destination = session.GetTopic("Test");
				using(IMessageProducer producer = session.CreateProducer(destination))
				{
					ITextMessage message = session.CreateTextMessage();

					for(int i = 0; i < 10; ++i)
					{
						message.Properties["TribbleName"] = "Tribble" + i.ToString();
						message.Text = "The Trouble with Tribbles - " + i.ToString();
						producer.Send(message);
					}
				}
			}
		}

		[Test]
		public void TestNoCopyOnSend()
		{
			Uri uri = new Uri("mock://localhost:61616?connection.CopyMessageOnSend=false");

			ConnectionFactory factory = new ConnectionFactory(uri);
			using(IConnection connection = factory.CreateConnection())
			using(ISession session = connection.CreateSession())
			{
				IDestination destination = session.GetTopic("Test");
				using(IMessageProducer producer = session.CreateProducer(destination))
				{
					ITextMessage message = session.CreateTextMessage();

					for(int i = 0; i < 10; ++i)
					{
						try
						{
							message.Properties["TribbleName"] = "Tribble" + i.ToString();
							message.Text = "The Trouble with Tribbles - " + i.ToString();
							producer.Send(message);
						}
						catch(MessageNotWriteableException)
						{
							Assert.Greater(i, 0);
							Assert.Less(i, 10);
						}
					}
				}
			}
		}
	}
}
