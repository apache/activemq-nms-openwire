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
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test.Async
{
	[TestFixture]
	public class MessageProducerTestAsync
	{
	
	        [Test, Timeout(20_000)]
    		public async Task TestProducerSendWithExpiry()
    		{
    			// Uri uri = new Uri(string.Format("tcp://localhost"));
    			// Uris uri = new Uri(string.Format("mock://localhost:61616?transport.respondToMessages=false"));
                string uri = "tcp://${activemqhost}:61616?transport.useLogging=true";
                IConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri));
                
    			// ConnectionFactory factory = new ConnectionFactory(uri);
    			using(IConnection connection = await factory.CreateConnectionAsync("guest", "guest"))
    			using(ISession session = await connection.CreateSessionAsync())
    			{
    				IDestination destination = await session.GetTopicAsync("Test");
    				using(IMessageProducer producer = await session.CreateProducerAsync(destination))
    				{
    					ITextMessage message = await session.CreateTextMessageAsync("Hello World");
    					message.NMSTimeToLive = TimeSpan.FromSeconds(175);
                        await producer.SendAsync(message);
                        
                        // ITextMessage message2 = await session.CreateTextMessageAsync("Hello World");
                        // // message2.NMSTimeToLive = TimeSpan.FromSeconds(175);
                        // await producer.SendAsync(message2);
    				}
    			}
    		}
	
	
		[Test, Timeout(20_000)]
		public async Task TestProducerSendWithTimeout()
		{
			int timeout = 1500;
			Uri uri = new Uri(string.Format("mock://localhost:61616?connection.RequestTimeout={0}&transport.respondToMessages=false", timeout));

			ConnectionFactory factory = new ConnectionFactory(uri);
			using(IConnection connection = await factory.CreateConnectionAsync())
			using(ISession session = await connection.CreateSessionAsync())
			{
				IDestination destination = await session.GetTopicAsync("Test");
				using(IMessageProducer producer = await session.CreateProducerAsync(destination))
				{
					ITextMessage message = await session.CreateTextMessageAsync("Hello World");

					for(int i = 0; i < 10; ++i)
					{
						DateTime start = DateTime.Now;

						try
						{
							await producer.SendAsync(message);
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

		[Test, Timeout(20_000)]
		public async Task TestCopyOnSend()
		{
			Uri uri = new Uri("mock://localhost:61616?connection.CopyMessageOnSend=true");

			ConnectionFactory factory = new ConnectionFactory(uri);
			using(IConnection connection = await factory.CreateConnectionAsync())
			using(ISession session = await connection.CreateSessionAsync())
			{
				IDestination destination = await session.GetTopicAsync("Test");
				using(IMessageProducer producer = await session.CreateProducerAsync(destination))
				{
					ITextMessage message = await session.CreateTextMessageAsync();

					for(int i = 0; i < 10; ++i)
					{
						message.Properties["TribbleName"] = "Tribble" + i.ToString();
						message.Text = "The Trouble with Tribbles - " + i.ToString();
						await producer.SendAsync(message);
					}
				}
			}
		}

		[Test, Timeout(20_000)]
		public async Task TestNoCopyOnSend()
		{
			Uri uri = new Uri("mock://localhost:61616?connection.CopyMessageOnSend=false");

			ConnectionFactory factory = new ConnectionFactory(uri);
			using(IConnection connection = await factory.CreateConnectionAsync())
			using(ISession session = await connection.CreateSessionAsync())
			{
				IDestination destination = await session.GetTopicAsync("Test");
				using(IMessageProducer producer = await session.CreateProducerAsync(destination))
				{
					ITextMessage message = await session.CreateTextMessageAsync();

					for(int i = 0; i < 10; ++i)
					{
						try
						{
							message.Properties["TribbleName"] = "Tribble" + i.ToString();
							message.Text = "The Trouble with Tribbles - " + i.ToString();
							await producer.SendAsync(message);
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
