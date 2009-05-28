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

using Apache.NMS.Util;
using NUnit.Framework;
using NUnit.Framework.Extensions;
using System.Threading;

namespace Apache.NMS.Test
{
	[TestFixture]
	public class VirtualTopicTest : NMSTestSupport
	{
		protected static string PRODUCER_DESTINATION_NAME = "topic://VirtualTopic.TestDestination";
		protected static string CONSUMER_A_DESTINATION_NAME = "queue://Consumer.A.VirtualTopic.TestDestination";
		protected static string CONSUMER_B_DESTINATION_NAME = "queue://Consumer.B.VirtualTopic.TestDestination";
		protected static string TEST_CLIENT_ID = "VirtualTopicClientId";

		protected const int totalMsgs = 5;
		protected AcknowledgementMode currentAckMode;
		protected int receivedA;
		protected int receivedB;

#if !NET_1_1
		[RowTest]
		[Row(AcknowledgementMode.AutoAcknowledge, false)]
        [Row(AcknowledgementMode.ClientAcknowledge, false)]
        [Row(AcknowledgementMode.Transactional, false)]

        [Row(AcknowledgementMode.AutoAcknowledge, true)]
        [Row(AcknowledgementMode.ClientAcknowledge, true)]
		// Do not use listeners with transactional processing.
#endif
		public void SendReceiveVirtualTopicMessage(AcknowledgementMode ackMode, bool useListeners)
		{
			currentAckMode = ackMode;
			receivedA = 0;
			receivedB = 0;

			using(IConnection connection = CreateConnection(TEST_CLIENT_ID))
			{
				connection.Start();
				using(ISession session = connection.CreateSession(currentAckMode))
				{
					using(IMessageConsumer consumerA = session.CreateConsumer(SessionUtil.GetDestination(session, CONSUMER_A_DESTINATION_NAME)))
					using(IMessageConsumer consumerB = session.CreateConsumer(SessionUtil.GetDestination(session, CONSUMER_B_DESTINATION_NAME)))
					using(IMessageProducer producer = session.CreateProducer(SessionUtil.GetDestination(session, PRODUCER_DESTINATION_NAME)))
					{
						producer.RequestTimeout = receiveTimeout;
						if(useListeners)
						{
							consumerA.Listener += MessageListenerA;
							consumerB.Listener += MessageListenerB;
						}

                        for (int index = 0; index < totalMsgs; index++)
                        {
                            producer.Send(session.CreateTextMessage("Message #" + index));
                        }

                        if (AcknowledgementMode.Transactional == currentAckMode)
                        {
                            session.Commit();
                        }

                        if (!useListeners)
                        {
                            for (int index = 0; index < totalMsgs; index++)
                            {
                                IMessage messageA = consumerA.Receive(receiveTimeout);
                                IMessage messageB = consumerB.Receive(receiveTimeout);

                                Assert.IsNotNull(messageA, "Did not receive message for consumer A.");
                                Assert.IsNotNull(messageB, "Did not receive message for consumer B.");

                                if (AcknowledgementMode.ClientAcknowledge == currentAckMode)
                                {
                                    messageA.Acknowledge();
                                    messageB.Acknowledge();
                                }
                            }
                        }
                        else
                        {
                            int waitCount = 0;
                            while (receivedA < totalMsgs && receivedB < totalMsgs)
                            {
                                if (waitCount++ > 50)
                                {
                                    Assert.Fail("Timed out waiting for message consumers.  A = " + receivedA + ", B = " + receivedB);
                                }

                                Thread.Sleep(250);
                            }
                        }
					}

                    if (AcknowledgementMode.Transactional == currentAckMode)
                    {
                        session.Commit();
                    }

                    session.Close();
				}

                connection.Close();
			}
		}

		private void MessageListenerA(IMessage message)
		{
			receivedA++;
			if(AcknowledgementMode.ClientAcknowledge == currentAckMode)
			{
				message.Acknowledge();
			}
		}

		private void MessageListenerB(IMessage message)
		{
			receivedB++;
			if(AcknowledgementMode.ClientAcknowledge == currentAckMode)
			{
				message.Acknowledge();
			}
		}
	}
}
