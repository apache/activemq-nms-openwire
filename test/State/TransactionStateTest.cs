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
	public class TransactionStateTest
	{
		[Test]
		public void TestTransactionState()
		{
		    LocalTransactionId id = new LocalTransactionId();
		    id.Value = 42;
		    TransactionState state = new TransactionState(id);

		    Assert.IsNotNull(state.Id);

			LocalTransactionId temp = state.Id as LocalTransactionId;
		    Assert.IsNotNull(temp);
			Assert.AreEqual(id.Value, temp.Value);
		}

		[Test]
		public void TestShutdown()
		{
		    LocalTransactionId id = new LocalTransactionId();
		    id.Value = 42;
		    TransactionState state = new TransactionState(id);

			state.AddCommand(new Message());
			state.AddCommand(new Message());
			state.AddCommand(new Message());
			state.AddCommand(new Message());

			state.AddProducer(new ProducerState(CreateProducerInfo(1)));
			state.AddProducer(new ProducerState(CreateProducerInfo(2)));

			Assert.AreEqual(4, state.Commands.Count);
			Assert.AreEqual(2, state.ProducerStates.Count);

			state.Shutdown();

			Assert.AreEqual(0, state.Commands.Count);
			Assert.AreEqual(0, state.ProducerStates.Count);

			try
			{
				state.AddCommand(new Message());
				Assert.Fail("Should have thrown an exception");
			}
			catch
			{
			}

			try
			{
				state.AddProducer(new ProducerState(CreateProducerInfo(2)));
				Assert.Fail("Should have thrown an exception");
			}
			catch
			{
			}
		}

		private ProducerInfo CreateProducerInfo(int value)
		{
			ProducerId pid = new ProducerId();
			pid.Value = value;
			pid.ConnectionId = "CONNECTION";
			pid.SessionId = 1;
			ProducerInfo info = new ProducerInfo();
			info.ProducerId = pid;

			return info;
		}
	}
}

