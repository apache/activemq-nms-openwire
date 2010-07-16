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

using Apache.NMS.Test;
using Apache.NMS.ActiveMQ;

using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class ConnectionFactoryTest : NMSTestSupport
	{
		[Test]
		public void TestConnectionFactorySetParams()
		{
			string connectionURI = "tcp://${activemqhost}:61616";
			ConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));
			
			factory.AcknowledgementMode = AcknowledgementMode.ClientAcknowledge;
			factory.AsyncSend = true;
			factory.AlwaysSyncSend = true;
			factory.AsyncClose = false;
			factory.CopyMessageOnSend = false;
			factory.RequestTimeout = 3000;
			factory.SendAcksAsync = true;
			factory.DispatchAsync = false;
			
			using(Connection connection = factory.CreateConnection() as Connection)
			{
				Assert.AreEqual(AcknowledgementMode.ClientAcknowledge, connection.AcknowledgementMode);
				Assert.IsTrue(connection.AsyncSend);
				Assert.IsTrue(connection.AlwaysSyncSend);
				Assert.IsFalse(connection.AsyncClose);
				Assert.IsFalse(connection.CopyMessageOnSend);
				Assert.AreEqual(3000, connection.RequestTimeout.TotalMilliseconds);
				Assert.IsTrue(connection.SendAcksAsync);
				Assert.IsFalse(connection.DispatchAsync);
			}
			
			factory.SendAcksAsync = false;
			
			using(Connection connection = factory.CreateConnection() as Connection)
			{
				Assert.AreEqual(AcknowledgementMode.ClientAcknowledge, connection.AcknowledgementMode);
				Assert.IsTrue(connection.AsyncSend);
				Assert.IsTrue(connection.AlwaysSyncSend);
				Assert.IsFalse(connection.AsyncClose);
				Assert.IsFalse(connection.CopyMessageOnSend);
				Assert.AreEqual(3000, connection.RequestTimeout.TotalMilliseconds);
				Assert.IsFalse(connection.SendAcksAsync);
				Assert.IsFalse(connection.DispatchAsync);
			}			
		}
	
		[Test]
		public void TestConnectionFactoryParseParams()
		{
			string connectionURI = "tcp://${activemqhost}:61616?" +
								   "connection.AckMode=ClientAcknowledge&" +
								   "connection.AsyncSend=true&" +
								   "connection.AlwaysSyncSend=true&" +
								   "connection.AsyncClose=false&" +
								   "connection.CopyMessageOnSend=false&" +
								   "connection.RequestTimeout=3000&" +
								   "connection.SendAcksAsync=true&" +
								   "connection.DispatchAsync=true";
			
			ConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));
			
			using(Connection connection = factory.CreateConnection() as Connection)
			{
				Assert.AreEqual(AcknowledgementMode.ClientAcknowledge, connection.AcknowledgementMode);
				Assert.IsTrue(connection.AsyncSend);
				Assert.IsTrue(connection.AlwaysSyncSend);
				Assert.IsFalse(connection.AsyncClose);
				Assert.IsFalse(connection.CopyMessageOnSend);
				Assert.AreEqual(3000, connection.RequestTimeout.TotalMilliseconds);
				Assert.IsTrue(connection.SendAcksAsync);
				Assert.IsTrue(connection.DispatchAsync);
			}
		}
		
	}	
}

