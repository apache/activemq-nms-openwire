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
	public class ActiveMQMessageAuditTest
	{
		[Test]
		public void TestIsDuplicateMessageId()
		{
		    int count = 10000;
		    ActiveMQMessageAudit audit = new ActiveMQMessageAudit();
		    List<MessageId> list = new List<MessageId>();

		    ProducerId pid = new ProducerId();
		    pid.ConnectionId = "test";
		    pid.SessionId = 0;
		    pid.Value = 1;

		    for (int i = 0; i < count; i++) 
			{
		        MessageId id = new MessageId();
		        id.ProducerId = pid;
		        id.ProducerSequenceId = i;
		        list.Add(id);
		        Assert.IsFalse(audit.IsDuplicate(id));
		    }

		    int index = list.Count -1 -audit.AuditDepth;
		    for (; index < list.Count; index++) 
			{
		        MessageId id = list[index];
		        Assert.IsTrue(audit.IsDuplicate(id), "duplicate msg:" + id.ToString());
		    }
		}

		[Test]
		public void TestIsInOrderMessageId()
		{
		    int count = 10000;
		    ActiveMQMessageAudit audit = new ActiveMQMessageAudit();
		    List<MessageId> list = new List<MessageId>();

		    ProducerId pid = new ProducerId();
		    pid.ConnectionId = "test";
		    pid.SessionId = 0;
		    pid.Value = 1;

		    for (int i = 0; i < count; i++)
			{
		        MessageId id = new MessageId();
		        id.ProducerId = pid;
		        id.ProducerSequenceId = i;

		        if (i == 0) 
				{
		            Assert.IsFalse(audit.IsDuplicate(id));
		            Assert.IsTrue(audit.IsInOrder(id));
		        }
		        if (i > 1 && i % 2 != 0) 
				{
		            list.Add(id);
		        }
		    }

		    for (int i = 0; i < list.Count; i++)
			{
		        MessageId mid = list[i];
		        Assert.IsFalse(audit.IsInOrder(mid), "Out of order msg: " + mid.ToString());
		        Assert.IsFalse(audit.IsDuplicate(mid));
		    }
		}

		[Test]
		public void TestRollbackMessageId()
		{
		    int count = 10000;
		    ActiveMQMessageAudit audit = new ActiveMQMessageAudit();
		    List<MessageId> list = new List<MessageId>();

		    ProducerId pid = new ProducerId();
		    pid.ConnectionId = "test";
		    pid.SessionId = 0;
		    pid.Value = 1;

		    for (int i = 0; i < count; i++) 
			{
		        MessageId id = new MessageId();
		        id.ProducerId = pid;
		        id.ProducerSequenceId = i;
		        list.Add(id);
		        Assert.IsFalse(audit.IsDuplicate(id));
		    }

		    int index = list.Count -1 -audit.AuditDepth;
		    for (; index < list.Count; index++) 
			{
		        MessageId id = list[index];
		        Assert.IsTrue(audit.IsDuplicate(id), "duplicate msg:" + id.ToString());
		        audit.Rollback(id);
		        Assert.IsFalse(audit.IsDuplicate(id), "erronious duplicate msg:" + id.ToString());
		    }
		}

		[Test]
		public void TestGetLastSeqId()
		{
		    int count = 10000;
		    ActiveMQMessageAudit audit = new ActiveMQMessageAudit();
		    List<MessageId> list = new List<MessageId>();

		    ProducerId pid = new ProducerId();
		    pid.ConnectionId = "test";
		    pid.SessionId = 0;
		    pid.Value = 1;

			MessageId id = new MessageId();
		    id.ProducerId = pid;

		    for (int i = 0; i < count; i++)
			{
		        id.ProducerSequenceId = i;
		        list.Add(id);
		        Assert.IsFalse(audit.IsDuplicate(id));
		        Assert.AreEqual(i, audit.GetLastSeqId(pid));
		    }
		}
	}
}

