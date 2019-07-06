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

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class VirtualTopicTest : NMSTestSupport
	{
		protected static string DESTINATION_NAME = "TEST.VTopicDestination";
		protected static string PRODUCER_DESTINATION_NAME = "VirtualTopic." + DESTINATION_NAME;
		protected static string CONSUMER_A_DESTINATION_NAME = "Consumer.A." + PRODUCER_DESTINATION_NAME;
		protected static string CONSUMER_B_DESTINATION_NAME = "Consumer.B." + PRODUCER_DESTINATION_NAME;
		protected static string TEST_CLIENT_ID = "VirtualTopicTestClientId";

		protected const int totalMsgs = 5;

		[Test]
		public void SendReceiveVirtualTopicMessage(
			[Values(AcknowledgementMode.AutoAcknowledge, AcknowledgementMode.ClientAcknowledge,
				AcknowledgementMode.DupsOkAcknowledge, AcknowledgementMode.Transactional)]
			AcknowledgementMode ackMode,
			[Values(MsgDeliveryMode.NonPersistent, MsgDeliveryMode.Persistent)]
			MsgDeliveryMode deliveryMode)
		{
			using(IConnection connection = CreateConnection(TEST_CLIENT_ID))
			{
				connection.Start();
				using(ISession session = connection.CreateSession(ackMode))
				{
					using(IMessageConsumer consumerA = session.CreateConsumer(session.GetQueue(CONSUMER_A_DESTINATION_NAME)))
					using(IMessageConsumer consumerB = session.CreateConsumer(session.GetQueue(CONSUMER_B_DESTINATION_NAME)))
					using(IMessageProducer producer = session.CreateProducer(session.GetTopic(PRODUCER_DESTINATION_NAME)))
					{
						producer.DeliveryMode = deliveryMode;

						for(int index = 0; index < totalMsgs; index++)
						{
							string msgText = "Message #" + index;
							Tracer.Info("Sending: " + msgText);
							producer.Send(session.CreateTextMessage(msgText));
						}

						if(AcknowledgementMode.Transactional == ackMode)
						{
							session.Commit();
						}

						for(int index = 0; index < totalMsgs; index++)
						{
							string msgText = "Message #" + index;
							ITextMessage messageA = consumerA.Receive(receiveTimeout) as ITextMessage;
							Assert.IsNotNull(messageA, "Did not receive message for consumer A.");
							messageA.Acknowledge();
							Tracer.Info("Received A: " + msgText);

							ITextMessage messageB = consumerB.Receive(receiveTimeout) as ITextMessage;
							Assert.IsNotNull(messageB, "Did not receive message for consumer B.");
							messageB.Acknowledge();
							Tracer.Info("Received B: " + msgText);

							Assert.AreEqual(msgText, messageA.Text, "Message text A does not match.");
							Assert.AreEqual(msgText, messageB.Text, "Message text B does not match.");
						}

						if(AcknowledgementMode.Transactional == ackMode)
						{
							session.Commit();
						}
					}

                    // Give the Broker some time to remove the subscriptions.
                    Thread.Sleep(2000);

                    try
                    {
                        ((Session) session).DeleteDestination(session.GetQueue(CONSUMER_A_DESTINATION_NAME));
                        ((Session) session).DeleteDestination(session.GetQueue(CONSUMER_B_DESTINATION_NAME));
                    }
                    catch
                    {
                    }
				}
			}
		}

		protected int receivedA;
		protected int receivedB;

		[Test]
		// Do not use listeners with transactional processing.
		public void AsyncSendReceiveVirtualTopicMessage(
			[Values(AcknowledgementMode.AutoAcknowledge, AcknowledgementMode.ClientAcknowledge, AcknowledgementMode.DupsOkAcknowledge)]
			AcknowledgementMode ackMode,
			[Values(MsgDeliveryMode.NonPersistent, MsgDeliveryMode.Persistent)]
			MsgDeliveryMode deliveryMode)
		{
			receivedA = 0;
			receivedB = 0;

			using(IConnection connection = CreateConnection(TEST_CLIENT_ID))
			{
				connection.Start();
				using(ISession session = connection.CreateSession(ackMode))
				{
					using(IMessageConsumer consumerA = session.CreateConsumer(session.GetQueue(CONSUMER_A_DESTINATION_NAME)))
					using(IMessageConsumer consumerB = session.CreateConsumer(session.GetQueue(CONSUMER_B_DESTINATION_NAME)))
					using(IMessageProducer producer = session.CreateProducer(session.GetTopic(PRODUCER_DESTINATION_NAME)))
					{
						producer.DeliveryMode = deliveryMode;

						consumerA.Listener += MessageListenerA;
						consumerB.Listener += MessageListenerB;

						for(int index = 0; index < totalMsgs; index++)
						{
							string msgText = "Message #" + index;
							Tracer.Info("Sending: " + msgText);
							producer.Send(session.CreateTextMessage(msgText));
						}

						int waitCount = 0;
						while(receivedA < totalMsgs && receivedB < totalMsgs)
						{
							if(waitCount++ > 50)
							{
								Assert.Fail("Timed out waiting for message consumers.  A = " + receivedA + ", B = " + receivedB);
							}

							Tracer.Info("Waiting... Received A = " + receivedA + ", Received B = " + receivedB);
							Thread.Sleep(250);
						}
					}
                    
                    // Give the Broker some time to remove the subscriptions.
                    Thread.Sleep(2000);

                    try
                    {
                        ((Session) session).DeleteDestination(session.GetQueue(CONSUMER_A_DESTINATION_NAME));
                        ((Session) session).DeleteDestination(session.GetQueue(CONSUMER_B_DESTINATION_NAME));
				    }
                    catch
                    {
                    }
                }
			}
		}

		private void MessageListenerA(IMessage message)
		{
			message.Acknowledge();
			ITextMessage messageA = message as ITextMessage;
			string msgText = "Message #" + receivedA;
			Assert.AreEqual(msgText, messageA.Text, "Message text A does not match.");
			Tracer.Info("Received Listener A: " + msgText);
			receivedA++;
		}

		private void MessageListenerB(IMessage message)
		{
			message.Acknowledge();
			ITextMessage messageB = message as ITextMessage;
			string msgText = "Message #" + receivedB;
			Assert.AreEqual(msgText, messageB.Text, "Message text B does not match.");
			Tracer.Info("Received Listener B: " + msgText);
			receivedB++;
		}
	}
}
