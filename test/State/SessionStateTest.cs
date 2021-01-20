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
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.State;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
	public class SessionStateTest
	{
		[Test]
		public void TestSessionState()
		{
		    // Create a Consumer
		    ConsumerId cid = new ConsumerId();
		    cid.ConnectionId = "CONNECTION";
		    cid.SessionId = 4096;
		    cid.Value = 42;
		    ConsumerInfo cinfo = new ConsumerInfo();
		    cinfo.ConsumerId = cid;

		    // Create a Producer
		    ProducerId pid = new ProducerId();
		    pid.ConnectionId = "CONNECTION";
		    pid.SessionId = 42;
		    pid.Value = 4096;
		    ProducerInfo pinfo = new ProducerInfo();
		    pinfo.ProducerId = pid;

		    // Create a Session
		    SessionId id = new SessionId();
		    id.ConnectionId = "CONNECTION";
		    id.Value = 42;
		    SessionInfo info = new SessionInfo();
		    info.SessionId = id;

		    SessionState state = new SessionState(info);
		    Assert.AreEqual(info, state.Info);

		    state.AddProducer(pinfo);
		    state.AddConsumer(cinfo);

		    Assert.AreEqual(1, state.ConsumerStates.Count);
		    Assert.AreEqual(1, state.ProducerStates.Count);

		    state.RemoveProducer(pinfo.ProducerId);
		    state.RemoveConsumer(cinfo.ConsumerId);

		    Assert.AreEqual(0, state.ConsumerStates.Count);
		    Assert.AreEqual(0, state.ProducerStates.Count);

		    state.AddConsumer(cinfo);
		    state.AddProducer(pinfo);
		    state.AddProducer(pinfo);
		    Assert.AreEqual(1, state.ProducerStates.Count);

			state.Shutdown();
		    Assert.AreEqual(0, state.ConsumerStates.Count);
		    Assert.AreEqual(0, state.ProducerStates.Count);

			try
			{
		  		state.AddConsumer(cinfo);
				Assert.Fail("Should have thrown an exception");
			}
			catch
			{
			}

			try
			{
		    	state.AddProducer(pinfo);
				Assert.Fail("Should have thrown an exception");
			}
			catch
			{
			}
		}
	}
}

