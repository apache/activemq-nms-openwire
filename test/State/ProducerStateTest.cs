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
	public class ProducerStateTest
	{
		[Test]
		public void TestProducerState()
		{
			ProducerState state = new ProducerState(CreateProducerInfo(1));

			Assert.IsNotNull(state.Info);
			Assert.AreEqual(1, state.Info.ProducerId.Value);

			state.TransactionState = CreateTXState(1);

			Assert.IsNotNull(state.TransactionState);
		}

		private TransactionState CreateTXState(int value)
		{
		    LocalTransactionId id = new LocalTransactionId();
		    id.Value = value;
		    return new TransactionState(id);
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

