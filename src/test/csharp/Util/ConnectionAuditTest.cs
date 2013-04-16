/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class ConnectionAuditTest
	{
        internal class MyDispatcher : IDispatcher
        {
			public void Dispatch(MessageDispatch messageDispatch)
			{
			}
        }

		[Test]
		public void TestConstructor1()
		{
		    ConnectionAudit audit = new ConnectionAudit();
		    Assert.IsTrue(audit.CheckForDuplicates);
		    Assert.IsTrue(audit.AuditDepth == ActiveMQMessageAudit.DEFAULT_WINDOW_SIZE);
		    Assert.IsTrue(audit.AuditMaximumProducerNumber == ActiveMQMessageAudit.MAXIMUM_PRODUCER_COUNT);
		}

		[Test]
		public void TestConstructor2()
		{
		    ConnectionAudit audit = new ConnectionAudit(100, 200);
		    Assert.IsTrue(audit.CheckForDuplicates);
		    Assert.IsTrue(audit.AuditDepth == 100);
		    Assert.IsTrue(audit.AuditMaximumProducerNumber == 200);
		}

		[Test]
		public void testIsDuplicate()
		{
		    int count = 10000;
		    ConnectionAudit audit = new ConnectionAudit();
		    List<MessageId> list = new List<MessageId>();
		    MyDispatcher dispatcher = new MyDispatcher();

		    ProducerId pid = new ProducerId();
		    pid.ConnectionId = "test";
		    pid.SessionId = 0;
		    pid.Value = 1;

		    ActiveMQDestination destination = new ActiveMQQueue("TEST.QUEUE");
		    Message message = new Message();
		    message.Destination = destination;

		    for (int i = 0; i < count; i++) 
			{
		        MessageId id = new MessageId();
		        id.ProducerId = pid;
		        id.ProducerSequenceId = i;
		        list.Add(id);

		        message.MessageId = id;
		        Assert.IsFalse(audit.IsDuplicate(dispatcher, message));
		    }

		    int index = list.Count -1 -audit.AuditDepth;
		    for (; index < list.Count; index++)
			{
		        MessageId id = list[index];
		        message.MessageId = id;
		        Assert.IsTrue(audit.IsDuplicate(dispatcher, message), "duplicate msg:" + id);
		    }
		}

		[Test]
		public void testRollbackDuplicate()
		{
		    int count = 10000;
		    ConnectionAudit audit = new ConnectionAudit();
		    List<MessageId> list = new List<MessageId>();
		    MyDispatcher dispatcher = new MyDispatcher();

		    ProducerId pid = new ProducerId();
		    pid.ConnectionId = "test";
		    pid.SessionId = 0;
		    pid.Value = 1;

		    ActiveMQDestination destination = new ActiveMQQueue("TEST.QUEUE");
		    Message message = new Message();
		    message.Destination = destination;

		    for (int i = 0; i < count; i++) 
			{
		        MessageId id = new MessageId();
		        id.ProducerId = pid;
		        id.ProducerSequenceId = i;
		        list.Add(id);

		        message.MessageId = id;
		        Assert.IsFalse(audit.IsDuplicate(dispatcher, message));
		    }

		    int index = list.Count -1 -audit.AuditDepth;
		    for (; index < list.Count; index++) 
			{
		        MessageId id = list[index];
		        message.MessageId = id;
		        Assert.IsTrue(audit.IsDuplicate(dispatcher, message), "duplicate msg:" + id);
		        audit.RollbackDuplicate(dispatcher, message);
		        Assert.IsFalse(audit.IsDuplicate(dispatcher, message), "error: duplicate msg:" + id);
		    }
		}
	}
}

