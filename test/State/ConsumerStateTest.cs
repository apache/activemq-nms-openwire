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
	public class ConsumerStateTest
	{
		[Test]
		public void TestConsumerState()
		{
			ConsumerState state = new ConsumerState(CreateConsumerInfo(1));

			Assert.IsNotNull(state.Info);
			Assert.AreEqual(1, state.Info.ConsumerId.Value);
		}

		private ConsumerInfo CreateConsumerInfo(int value)
		{
			ConsumerId cid = new ConsumerId();
			cid.Value = value;
			cid.ConnectionId = "CONNECTION";
			cid.SessionId = 1;
			ConsumerInfo info = new ConsumerInfo();
			info.ConsumerId = cid;

			return info;
		}	
	}
}

