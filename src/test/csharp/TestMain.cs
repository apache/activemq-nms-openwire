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

// START SNIPPET: demo
using System;
using Apache.NMS;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.Test
{
	public class TestMain
	{
		public static void Main(string[] args)
		{
			try
			{
				Uri connecturi = new Uri("activemq:tcp://activemqhost:61616");
				
				Console.WriteLine("About to connect to " + connecturi);

				// NOTE: ensure the nmsprovider-activemq.config file exists in the executable folder.
				IConnectionFactory factory = new NMSConnectionFactory(connecturi);

				using(IConnection connection = factory.CreateConnection())
				using(ISession session = connection.CreateSession())
				{
					/*
					 * Examples for getting a destination:
					 *   IDestination destination = session.GetQueue("FOO.BAR");  // Hard coded to queue destination
					 *   IDestination destination = session.GetTopic("FOO.BAR");  // Hard coded to topic destination
					 *   IDestination destination = SessionUtil.GetDestination(session, "queue://FOO.BAR");  // Allows destination type to be embedded in name
					 *   IDestination destination = SessionUtil.GetDestination(session, "topic://FOO.BAR");  // Allows destination type to be embedded in name
					 *   IDestination destination = SessionUtil.GetDestination(session, "FOO.BAR");          // Defaults to queue if type not specified.
					 */
					IDestination destination = SessionUtil.GetDestination(session, "queue://FOO.BAR");
					Console.WriteLine("Using destination: " + destination);

					// Create a consumer and producer
					using(IMessageConsumer consumer = session.CreateConsumer(destination))
					using(IMessageProducer producer = session.CreateProducer(destination))
					{
						producer.Persistent = true;

						// Send a message
						ITextMessage request = session.CreateTextMessage("Hello World!");
						request.NMSCorrelationID = "abc";
						request.Properties["NMSXGroupID"] = "cheese";
						request.Properties["myHeader"] = "Cheddar";

						producer.Send(request);

						// Consume a message
						ITextMessage message = consumer.Receive() as ITextMessage;
						if(message == null)
						{
							Console.WriteLine("No message received!");
						}
						else
						{
							Console.WriteLine("Received message with ID:   " + message.NMSMessageId);
							Console.WriteLine("Received message with text: " + message.Text);
						}
					}
				}
			}
			catch(Exception e)
			{
				Console.WriteLine("Caught: " + e);
				Console.WriteLine("Stack: " + e.StackTrace);
			}
		}
	}
}
// END SNIPPET: demo
