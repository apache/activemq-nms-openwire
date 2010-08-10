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
		protected static string DESTINATION_NAME = "queue://TestDestination";
		protected static string TEST_CLIENT_ID = "MessageConsumerTestClientId";

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
							producer.RequestTimeout = receiveTimeout;
							producer.DeliveryMode = deliveryMode;

							string msgText = "ExpiredMessage:" + Guid.NewGuid().ToString();

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
						}
					}
					finally
					{
						try
						{
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
