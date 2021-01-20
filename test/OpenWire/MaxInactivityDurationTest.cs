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
using System.Diagnostics;
using System.Threading;
using Apache.NMS.Test;
using Apache.NMS.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class MaxInactivityDurationTest : NMSTestSupport
	{
		protected static string DESTINATION_NAME = "TEST.MaxInactivityDuration";
		protected static string CORRELATION_ID = "MaxInactivityCorrelationID";

		[Test]
		public void TestMaxInactivityDuration()
		{
			string testuri = "activemq:tcp://${activemqhost}:61616" +
										"?wireFormat.maxInactivityDurationInitialDelay=5000" +
										"&wireFormat.maxInactivityDuration=10000" +
										"&connection.asyncClose=false";

			NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(testuri));
			using(IConnection connection = factory.CreateConnection("", ""))
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
						Thread.Sleep(TimeSpan.FromSeconds(30));

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

		[Test, Sequential]
		public void TestInactivityMonitorThreadLeak(
			[Values(0, 1000)]
			int inactivityDuration)
		{
			Process currentProcess = Process.GetCurrentProcess();
			Tracer.InfoFormat("Beginning thread count: {0}, handle count: {1}", currentProcess.Threads.Count, currentProcess.HandleCount);

			string testuri = string.Format("activemq:tcp://${{activemqhost}}:61616?wireFormat.maxInactivityDuration={0}", inactivityDuration);
	
			NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(testuri));

			// We measure the initial resource counts, and then allow a certain fudge factor for the resources
			// to fluctuate at run-time.  We allow for a certain amount of fluctuation, but if the counts
			// grow outside the safe boundaries of delayed garbage collection, then we fail the test.
			currentProcess = Process.GetCurrentProcess();
			int beginThreadCount = currentProcess.Threads.Count;
			int beginHandleCount = currentProcess.HandleCount;
			int maxThreadGrowth = 10;
			int maxHandleGrowth = 500;

			for(int i = 0; i < 200; i++)
			{
				using(IConnection connection = factory.CreateConnection("ResourceLeakTest", "Password"))
				{
					using(ISession session = connection.CreateSession())
					{
						IDestination destination = SessionUtil.GetDestination(session, "topic://TEST.NMSResourceLeak");
						using(IMessageConsumer consumer = session.CreateConsumer(destination))
						{
							connection.Start();
						}
					}
				}

				currentProcess = Process.GetCurrentProcess();
				int endThreadCount = currentProcess.Threads.Count;
				int endHandleCount = currentProcess.HandleCount;

				Assert.Less(endThreadCount, beginThreadCount + maxThreadGrowth, string.Format("Thread count grew beyond maximum of {0} on iteration #{1}.", maxThreadGrowth, i));
				Assert.Less(endHandleCount, beginHandleCount + maxHandleGrowth, string.Format("Handle count grew beyond maximum of {0} on iteration #{1}.", maxHandleGrowth, i));
			}
		}
	}
}
