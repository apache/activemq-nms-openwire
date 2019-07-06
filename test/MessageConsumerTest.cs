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

using System.Threading;
using Apache.NMS.Test;
using NUnit.Framework;
using Apache.NMS.ActiveMQ.Commands;
using System;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.Test
{
    public enum ExpirationOptions
    {
        DEFAULT,
        IGNORE,
        DO_NOT_IGNORE
    }

    [TestFixture]
    public class MessageConsumerTest : NMSTestSupport
    {
        protected static string DESTINATION_NAME = "queue://TEST.MessageConsumerTestDestination";
        protected static string TEST_CLIENT_ID = "MessageConsumerTestClientId";

        private CountDownLatch doneLatch;
        private int counter;
        private String errorMessage;

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            this.doneLatch = new CountDownLatch(1);
            this.counter = 0;
            this.errorMessage = null;
        }

        [Test]
        public void TestBadSelectorDoesNotCloseConnection()
        {
            using (IConnection connection = CreateConnection(TEST_CLIENT_ID))
            {
                using (ISession sender = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                {
					IDestination destination = sender.CreateTemporaryQueue();

					IMessageProducer producer = sender.CreateProducer(destination);
            		ITextMessage goodMsg = sender.CreateTextMessage("testGood");
            		producer.Send(goodMsg);

            		IMessageConsumer consumer = session.CreateConsumer(destination);
            		connection.Start();            
					Assert.NotNull(consumer.Receive(TimeSpan.FromMilliseconds(5000)));

		            try
		            {
		                ISession badListenerSession = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
		                badListenerSession.CreateConsumer(destination, "badSelector;too");
		                Assert.Fail("Exception expected.");
		            }
		            catch(Exception e)
		            {
						Tracer.DebugFormat("Caught Ex: {0}", e);
		            }

		            ITextMessage failMsg = sender.CreateTextMessage("testFail");
		            producer.Send(failMsg);
					Assert.NotNull(consumer.Receive(TimeSpan.FromMilliseconds(5000)));
	        	}
			}
		}

        [Test]
        public void TestAsyncDispatchExceptionRedelivers()
        {
            using (IConnection connection = CreateConnection(TEST_CLIENT_ID))
            {
                using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                {
                    IQueue queue = SessionUtil.GetDestination(session, DESTINATION_NAME) as IQueue;

                    using (IMessageProducer producer = session.CreateProducer(queue))
                    {
                        producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
                        producer.Send(producer.CreateTextMessage("First"));
                        producer.Send(producer.CreateTextMessage("Second"));
                    }

                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        consumer.Listener += OnTestAsynchRedliversMessage;

                        connection.Start();

                        if (doneLatch.await(TimeSpan.FromSeconds(10)))
                        {
                            if (!String.IsNullOrEmpty(errorMessage))
                            {
                                Assert.Fail(errorMessage);
                            }
                        }
                        else
                        {
                            Assert.Fail("Timeout waiting for async message delivery to complete.");
                        }
                    }
                }
            }
        }

        private void OnTestAsynchRedliversMessage(IMessage msg)
        {
            counter++;
            try
            {
                ITextMessage message = msg as ITextMessage;
                switch (counter)
                {
                case 1:
                    Tracer.Debug("Got first Message: " + message.Text);
                    Assert.AreEqual("First", message.Text);
                    Assert.IsFalse(message.NMSRedelivered);
                    break;
                case 2:
                    Tracer.Debug("Got Second Message: " + message.Text);
                    Assert.AreEqual("Second", message.Text);
                    Assert.IsFalse(message.NMSRedelivered);
                    throw new Exception("Ignore Me");
                case 3:
                    Tracer.Debug("Got Third Message: " + message.Text);
                    Assert.AreEqual("Second", message.Text);
                    Assert.IsTrue(message.NMSRedelivered);
                    doneLatch.countDown();
                    break;
                default:
                    errorMessage = "Got too many messages: " + counter;
                    Tracer.Debug(errorMessage);
                    doneLatch.countDown();
                    break;
                }
            }
            catch (Exception e)
            {
                if (e.Message.Equals("Ignore Me"))
                {
                    throw;
                }
                errorMessage = "Got exception: " + e.Message;
                Tracer.Warn("Exception on Message Receive: " + e.Message);
                doneLatch.countDown();
            }
        }

        [Test]
        public void ConsumeInTwoThreads()
        {
            ParameterizedThreadStart threadStart =
                delegate(object o)
                {
                    IMessageConsumer consumer = (IMessageConsumer)o;
                    IMessage message = consumer.Receive(TimeSpan.FromSeconds(2));
                    Assert.IsNotNull(message);
                };

            using (IConnection connection = CreateConnection(TEST_CLIENT_ID))
            {
                connection.Start();
                using (ISession session = connection.CreateSession(AcknowledgementMode.Transactional))
                {
                    IQueue queue = SessionUtil.GetDestination(session, DESTINATION_NAME) as IQueue;

                    // enqueue 2 messages
                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    using (IMessageProducer producer = session.CreateProducer(queue))
                    {
                        producer.DeliveryMode = MsgDeliveryMode.Persistent;
                        producer.Send(producer.CreateMessage());
                        producer.Send(producer.CreateMessage());
                        session.Commit();

                        // receive first using a dedicated thread. This works
                        Thread thread = new Thread(threadStart);
                        thread.Start(consumer);
                        thread.Join();
                        session.Commit();

                        // receive second using main thread. This FAILS
                        IMessage message = consumer.Receive(TimeSpan.FromSeconds(2)); // throws System.Threading.AbandonedMutexException
                        Assert.IsNotNull(message);
                        session.Commit();
                    }
                }
            }
        }

        [Test]
        public void TestReceiveIgnoreExpirationMessage(
            [Values(AcknowledgementMode.AutoAcknowledge, AcknowledgementMode.ClientAcknowledge,
                AcknowledgementMode.DupsOkAcknowledge, AcknowledgementMode.Transactional)]
            AcknowledgementMode ackMode,
            [Values(MsgDeliveryMode.NonPersistent, MsgDeliveryMode.Persistent)]
            MsgDeliveryMode deliveryMode,
            [Values(ExpirationOptions.DEFAULT, ExpirationOptions.IGNORE, ExpirationOptions.DO_NOT_IGNORE)]
            ExpirationOptions expirationOption)
        {
            using(IConnection connection = CreateConnection(TEST_CLIENT_ID))
            {
                connection.Start();
                using(Session session = connection.CreateSession(ackMode) as Session)
                {
                    string destinationName = DESTINATION_NAME;

                    if(ExpirationOptions.IGNORE == expirationOption)
                    {
                        destinationName += "?consumer.nms.ignoreExpiration=true";
                    }
                    else if(ExpirationOptions.DO_NOT_IGNORE == expirationOption)
                    {
                        destinationName += "?consumer.nms.ignoreExpiration=false";
                    }

                    try
                    {
                        IDestination destination = SessionUtil.GetDestination(session, destinationName);

                        using(IMessageConsumer consumer = session.CreateConsumer(destination))
                        using(IMessageProducer producer = session.CreateProducer(destination))
                        {
                            producer.DeliveryMode = deliveryMode;

                            string msgText = string.Format("ExpiredMessage: {0}", Guid.NewGuid().ToString());

                            ActiveMQTextMessage msg = session.CreateTextMessage(msgText) as ActiveMQTextMessage;

                            // Give it two seconds to live.
                            msg.NMSTimeToLive = TimeSpan.FromMilliseconds(2000);

                            producer.Send(msg);

                            if(AcknowledgementMode.Transactional == ackMode)
                            {
                                session.Commit();
                            }

                            // Wait for four seconds before processing it.  The broker will have sent it to our local
                            // client dispatch queue, but we won't attempt to process the message until it has had
                            // a chance to expire within our internal queue system.
                            Thread.Sleep(4000);

                            ActiveMQTextMessage rcvMsg = consumer.ReceiveNoWait() as ActiveMQTextMessage;

                            if(ExpirationOptions.IGNORE == expirationOption)
                            {
                                Assert.IsNotNull(rcvMsg, "Did not receive expired message.");
                                rcvMsg.Acknowledge();

                                Assert.AreEqual(msgText, rcvMsg.Text, "Message text does not match.");
                                Assert.IsTrue(rcvMsg.IsExpired());

                                if(AcknowledgementMode.Transactional == ackMode)
                                {
                                    session.Commit();
                                }
                            }
                            else
                            {
                                // Should not receive a message.
                                Assert.IsNull(rcvMsg, "Received an expired message!");
                            }

                            consumer.Close();
                            producer.Close();
                        }
                    }
                    finally
                    {
                        try
                        {
                            // Ensure that Session resources on the Broker release transacted Consumers.
                            session.Close();
                            // Give the Broker some time to remove the subscriptions.
                            Thread.Sleep(2000);
                            SessionUtil.DeleteDestination(session, destinationName);
                        }
                        catch
                        {
                        }
                    }
                }
            }
        }
    }
}
