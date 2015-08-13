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
using System.Collections;
using System.Collections.Generic;
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
                    IDestination destination = session.GetQueue("TEST.ReceiveBrowseReceive");
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
        public void TestBroserIteratively()
        {
            using (IConnection connection = CreateConnection())
            using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                connection.Start();

                IQueue queue = session.CreateTemporaryQueue();
                // enqueue a message
                using (IMessageProducer producer = session.CreateProducer(queue))
                {
                    IMessage message = producer.CreateMessage();
                    producer.Send(message);
                }

                Thread.Sleep(2000);

                // browse queue several times
                for (int j = 0; j < 1000; j++)
                {
                    using(QueueBrowser browser = session.CreateBrowser(queue) as QueueBrowser)
                    {
                        Tracer.DebugFormat("Running Iterative QueueBrowser sample #{0}", j);
                        IEnumerator enumeration = browser.GetEnumerator();
                        Assert.IsTrue(enumeration.MoveNext(), "should have received the second message");
                    }
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
                    IDestination destination = session.GetQueue("TEST.BrowseReceive");

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
		
        [Test]
        [ExpectedException("Apache.NMS.NMSException")]
        public void TestCreateBrowserFailsWithZeroPrefetch()
        {
            using (Connection connection = CreateConnection() as Connection)
            using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            {
                connection.PrefetchPolicy.QueueBrowserPrefetch = 0;
                IQueue queue = session.CreateTemporaryQueue();
                IQueueBrowser browser = session.CreateBrowser(queue);
				browser.Close();
            }
        }

        [Test]
        public void TestBrowsingExpiration()
        {
            const int MESSAGES_TO_SEND = 50;
            const string QUEUE_NAME = "TEST.TestBrowsingExpiration";

            // Browse the queue.
            using (Connection connection = CreateConnection() as Connection)
            using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            using (IQueue queue = session.GetQueue(QUEUE_NAME))
            {
                session.DeleteDestination(queue);

                SendTestMessages(MESSAGES_TO_SEND, QUEUE_NAME);

                connection.Start();
                int browsed = Browse(QUEUE_NAME, connection);

                // The number of messages browsed should be equal to the number of
                // messages sent.
                Assert.AreEqual(MESSAGES_TO_SEND, browsed);

                // Broker expired message period is 30 seconds by default
                for (int i = 0; i < 12; ++i)
                {
                    Thread.Sleep(5000);
                    browsed = Browse(QUEUE_NAME, connection);
                }

                session.DeleteDestination(session.GetQueue(QUEUE_NAME));

                Assert.AreEqual(0, browsed);
            }
        }

        private int Browse(String queueName, Connection connection)
        {
            int browsed = 0;

            using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            using (IQueue queue = session.GetQueue(queueName))
            using (IQueueBrowser browser = session.CreateBrowser(queue))
            {
                IEnumerator enumeration = browser.GetEnumerator();
                while (enumeration.MoveNext())
                {
                    ITextMessage message = enumeration.Current as ITextMessage;
                    Tracer.DebugFormat("Browsed message: {0}", message.NMSMessageId);
                    browsed++;
                }
            }

            return browsed;
        }

        protected void SendTestMessages(int count, String queueName)
        {
            // Send the messages to the Queue.
            using (Connection connection = CreateConnection() as Connection)
            using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            using (IQueue queue = session.GetQueue(queueName))
            using (IMessageProducer producer = session.CreateProducer(queue))
            {
                for (int i = 1; i <= count; i++) 
                {
                    String msgStr = "Message: " + i;
                    producer.Send(session.CreateTextMessage(msgStr), 
                                  MsgDeliveryMode.NonPersistent, 
                                  MsgPriority.Normal, 
                                  TimeSpan.FromMilliseconds(1500));
                }
            }
        }
	}
}
