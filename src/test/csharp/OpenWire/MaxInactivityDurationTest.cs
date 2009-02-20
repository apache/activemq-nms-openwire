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
using Apache.NMS.Util;
using NUnit.Framework;
using NUnit.Framework.Extensions;
using System.Threading;

namespace Apache.NMS.Test
{
	[TestFixture]
	public class MaxInactivityDurationTest : NMSTestSupport
	{
		protected static string DESTINATION_NAME = "TestMaxInactivityDuration";
		protected static string CORRELATION_ID = "MaxInactivityCorrelationID";

		/// <summary>
		/// The name of the connection configuration that CreateNMSFactory() will load.
		/// </summary>
		/// <returns></returns>
		protected override string GetNameTestURI() { return "maxInactivityDurationURI"; }

		[Test, Explicit]
		public void TestMaxInactivityDuration()
		{
			using(IConnection connection = CreateConnection())
			{
				connection.Start();
				using(ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
				{
					IDestination destination = SessionUtil.GetDestination(session, DESTINATION_NAME);
					using(IMessageConsumer consumer = session.CreateConsumer(destination))
					using(IMessageProducer producer = session.CreateProducer(destination))
					{
						SendMessage(producer);

						IMessage receivedMsg = consumer.Receive(TimeSpan.FromSeconds(5));
						Assert.AreEqual(CORRELATION_ID, receivedMsg.NMSCorrelationID, "Invalid correlation ID.");

						// Go inactive...
						Thread.Sleep(60 * 1000);

						// Send another message.
						SendMessage(producer);
						receivedMsg = consumer.Receive(TimeSpan.FromSeconds(5));
						Assert.AreEqual(CORRELATION_ID, receivedMsg.NMSCorrelationID, "Invalid correlation ID.");
					}
				}
			}
		}

		protected void SendMessage(IMessageProducer producer)
		{
			IMessage request = producer.CreateMessage();
			request.NMSCorrelationID = CORRELATION_ID;
			request.NMSType = "Test";
			producer.Send(request);
		}
	}
}
