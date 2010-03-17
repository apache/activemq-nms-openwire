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
using System.Collections;
using System.Diagnostics;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class QueueBrowserTests : NMSTestSupport
	{
		[Test]
		public void TestReceiveBrowseReceive()
		{
			using (IConnection connection = CreateConnection())
			{
				using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
				{
                    IDestination destination = session.GetQueue("TestReceiveBrowseReceive");
					IMessageProducer producer = session.CreateProducer(destination);
					IMessageConsumer consumer = session.CreateConsumer(destination);
					connection.Start();

					IMessage[] outbound = new IMessage[]{session.CreateTextMessage("First Message"),
                                                         session.CreateTextMessage("Second Message"),
                                                         session.CreateTextMessage("Third Message")};

					// lets consume any outstanding messages from previous test runs
					while (consumer.Receive(TimeSpan.FromMilliseconds(1000)) != null)
					{
					}

					producer.Send(outbound[0]);
					producer.Send(outbound[1]);
					producer.Send(outbound[2]);

					IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));

					// Get the first.
					Assert.AreEqual(((ITextMessage)outbound[0]).Text, ((ITextMessage)msg).Text);
					consumer.Close();

					IQueueBrowser browser = session.CreateBrowser((IQueue)destination);
					IEnumerator enumeration = browser.GetEnumerator();

					// browse the second
					Assert.IsTrue(enumeration.MoveNext(), "should have received the second message");
					Assert.AreEqual(((ITextMessage)outbound[1]).Text, ((ITextMessage)enumeration.Current).Text);

					// browse the third.
					Assert.IsTrue(enumeration.MoveNext(), "Should have received the third message");
					Assert.AreEqual(((ITextMessage)outbound[2]).Text, ((ITextMessage)enumeration.Current).Text);

					// There should be no more.
					bool tooMany = false;
					while (enumeration.MoveNext())
					{
						Debug.WriteLine("Got extra message: " + ((ITextMessage)enumeration.Current).Text);
						tooMany = true;
					}
					Assert.IsFalse(tooMany);

					//Reset should take us back to the start.
					enumeration.Reset();

					// browse the second
					Assert.IsTrue(enumeration.MoveNext(), "should have received the second message");
					Assert.AreEqual(((ITextMessage)outbound[1]).Text, ((ITextMessage)enumeration.Current).Text);

					// browse the third.
					Assert.IsTrue(enumeration.MoveNext(), "Should have received the third message");
					Assert.AreEqual(((ITextMessage)outbound[2]).Text, ((ITextMessage)enumeration.Current).Text);

					// There should be no more.
					tooMany = false;
					while (enumeration.MoveNext())
					{
						Debug.WriteLine("Got extra message: " + ((ITextMessage)enumeration.Current).Text);
						tooMany = true;
					}
					Assert.IsFalse(tooMany);

					browser.Close();

					// Re-open the consumer.
					consumer = session.CreateConsumer(destination);

					// Receive the second.
					Assert.AreEqual(((ITextMessage)outbound[1]).Text, ((ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000))).Text);
					// Receive the third.
					Assert.AreEqual(((ITextMessage)outbound[2]).Text, ((ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000))).Text);
					consumer.Close();
				}
			}
		}

		[Test]
		public void TestBrowseReceive()
		{
			using (IConnection connection = CreateConnection())
			{
				using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
				{
                    IDestination destination = session.GetQueue("TestBrowseReceive");

					connection.Start();

                    using(IMessageConsumer purger = session.CreateConsumer(destination))
                    {
                        // lets consume any outstanding messages from previous test runs
                        while(purger.Receive(TimeSpan.FromMilliseconds(1000)) != null)
                        {
                        }
                        
                        purger.Close();
                    }
                    
					IMessage[] outbound = new IMessage[]{session.CreateTextMessage("First Message"),
	                                                     session.CreateTextMessage("Second Message"),
	                                                     session.CreateTextMessage("Third Message")};

					IMessageProducer producer = session.CreateProducer(destination);
					producer.Send(outbound[0]);

					// create browser first
					IQueueBrowser browser = session.CreateBrowser((IQueue)destination);
					IEnumerator enumeration = browser.GetEnumerator();

					// create consumer
					IMessageConsumer consumer = session.CreateConsumer(destination);

					// browse the first message
					Assert.IsTrue(enumeration.MoveNext(), "should have received the first message");
					Assert.AreEqual(((ITextMessage)outbound[0]).Text, ((ITextMessage)enumeration.Current).Text);

					// Receive the first message.
					Assert.AreEqual(((ITextMessage)outbound[0]).Text, ((ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000))).Text);
					consumer.Close();
					browser.Close();
					producer.Close();
				}
			}
		}
	}
}
