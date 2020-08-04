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
using System.Collections.Generic;
using System.Threading;
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class NonBlockingConsumerRedeliveryTest  : NMSTestSupport
	{
        private Connection connection;
		private ISession session;
    	private readonly int MSG_COUNT = 5;
		private int count;

        private List<IMessage> received = new List<IMessage>();
        private List<IMessage> dlqed = new List<IMessage>();
        private List<IMessage> beforeRollback = new List<IMessage>();
        private List<IMessage> afterRollback = new List<IMessage>();

        [SetUp]
        public override void SetUp()
        {
			base.SetUp();

			session = null;
        	connection = (Connection) CreateConnection();
        	connection.NonBlockingRedelivery = true;

			DeleteDLQ();

			this.received.Clear();
			this.beforeRollback.Clear();
			this.afterRollback.Clear();
			this.dlqed.Clear();
			this.count = 0;
        }

        [TearDown]
        public override void TearDown()
        {
            if(this.connection != null)
            {
                this.connection.Close();
                this.connection = null;
            }
            
            base.TearDown();
        }
	
		public void OnMessage(IMessage message)
		{
			this.received.Add(message);
		}

		private void AssertReceived(int count, String message)
		{
			AssertReceived(this.received, count, message);
		}

		private void AssertReceived(List<IMessage> target, int count, String message)
		{
			for (int i = 0; i < 30; ++i)
			{
				if (target.Count == count)
				{
					break;
				}
				Thread.Sleep(1000);				
			}
			Assert.AreEqual(count, target.Count, message);
		}

		[Test]
		public void testMessageDeleiveredWhenNonBlockingEnabled()
		{
	        session = connection.CreateSession(AcknowledgementMode.Transactional);
			IDestination destination = session.CreateTemporaryQueue();
	        IMessageConsumer consumer = session.CreateConsumer(destination);

			consumer.Listener += OnMessage;

	        SendMessages(destination);
	        session.Commit();
	        
			connection.Start();

			AssertReceived(MSG_COUNT, "Pre-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        this.beforeRollback.AddRange(received);
	        this.received.Clear();
	        session.Rollback();

			AssertReceived(MSG_COUNT, "Post-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        this.afterRollback.AddRange(received);
	        this.received.Clear();

	        Assert.AreEqual(this.beforeRollback.Count, this.afterRollback.Count);
	        Assert.AreEqual(this.beforeRollback, this.afterRollback);
	        session.Commit();
		}

		[Test]
		public void testMessageDeleiveredInCorrectOrder()
		{
	        session = connection.CreateSession(AcknowledgementMode.Transactional);
	        IDestination destination = session.CreateTemporaryQueue();
	        IMessageConsumer consumer = session.CreateConsumer(destination);

			consumer.Listener += OnMessage;

	        SendMessages(destination);

	        session.Commit();
	        connection.Start();

			AssertReceived(MSG_COUNT, "Pre-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        beforeRollback.AddRange(received);
	        received.Clear();
	        session.Rollback();

			AssertReceived(MSG_COUNT, "Post-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        afterRollback.AddRange(received);
	        received.Clear();

	        Assert.AreEqual(beforeRollback.Count, afterRollback.Count);
	        Assert.AreEqual(beforeRollback, afterRollback);

			IEnumerator<IMessage> after = afterRollback.GetEnumerator();
			IEnumerator<IMessage> before = beforeRollback.GetEnumerator();

			while (after.MoveNext() && before.MoveNext())
			{
				ITextMessage original = before.Current as ITextMessage;
				ITextMessage rolledBack = after.Current as ITextMessage;

				int originalId = Int32.Parse(original.Text);
				int rolledBackId = Int32.Parse (rolledBack.Text);

				Assert.AreEqual(originalId, rolledBackId);
			}

	        session.Commit();
		}

		[Test]
		public void testMessageDeleiveryDoesntStop()
		{
	        session = connection.CreateSession(AcknowledgementMode.Transactional);
			IDestination destination = session.CreateTemporaryQueue();
	        IMessageConsumer consumer = session.CreateConsumer(destination);

			consumer.Listener += OnMessage;

	        SendMessages(destination);
	        connection.Start();

			AssertReceived(MSG_COUNT, "Pre-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        beforeRollback.AddRange(received);
	        received.Clear();
	        session.Rollback();

	        SendMessages(destination);

			AssertReceived(MSG_COUNT * 2, "Post-Rollback expects to receive: " + MSG_COUNT * 2 + " messages.");

	        afterRollback.AddRange(received);
	        received.Clear();

	        Assert.AreEqual(beforeRollback.Count * 2, afterRollback.Count);

	        session.Commit();
		}

		[Test]
		public void testNonBlockingMessageDeleiveryIsDelayed()		
		{
	        connection.RedeliveryPolicy.InitialRedeliveryDelay = 7000;
	        session = connection.CreateSession(AcknowledgementMode.Transactional);
	        IDestination destination = session.CreateTemporaryQueue();
	        IMessageConsumer consumer = session.CreateConsumer(destination);

			consumer.Listener += OnMessage;

	        SendMessages(destination);
	        connection.Start();

			AssertReceived(MSG_COUNT, "Pre-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        received.Clear();
	        session.Rollback();

			Thread.Sleep(4000);
			Assert.IsFalse(this.received.Count > 0, "Delayed redelivery test not expecting any messages yet.");

			AssertReceived(MSG_COUNT, "Post-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        session.Commit();
	        session.Close();
		}

        public void OnMessageWithSomeRollbacks(IMessage message) 
		{
            if (++count > 10) 
			{
                try 
				{
                    session.Rollback();
                    Tracer.Info("Rolling back session.");
                    count = 0;
                }
				catch (Exception e) 
				{
					Tracer.WarnFormat("Caught an unexcepted exception: {0}", e.Message);
                }
            } 
			else 
			{
                received.Add(message);
                try 
				{
                    session.Commit();
				}
				catch (Exception e) 
				{
					Tracer.WarnFormat("Caught an unexcepted exception: {0}", e.Message);
                }
            }
        }

		[Test]
		public void testNonBlockingMessageDeleiveryWithRollbacks()
		{
	        session = connection.CreateSession(AcknowledgementMode.Transactional);
	        IDestination destination = session.CreateTemporaryQueue();
	        IMessageConsumer consumer = session.CreateConsumer(destination);

			consumer.Listener += OnMessage;

	        SendMessages(destination);
	        connection.Start();

			AssertReceived(MSG_COUNT, "Pre-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        received.Clear();

			consumer.Listener -= OnMessage;
			consumer.Listener += OnMessageWithSomeRollbacks;

	        session.Rollback();

			AssertReceived(MSG_COUNT, "Post-Rollback expects to receive: " + MSG_COUNT + " messages.");

	        Assert.AreEqual(MSG_COUNT, received.Count);
	        session.Commit();
		}

		private void OnDLQMessage(IMessage message)
		{
			Tracer.DebugFormat("DLQ Message {0}", message);
            dlqed.Add(message);
		}

		private void OnMessageAlwaysRollsBack(IMessage message)
		{
			session.Rollback();
		}

		[Test]
		public void testNonBlockingMessageDeleiveryWithAllRolledBack()
		{
	        connection.RedeliveryPolicy.MaximumRedeliveries = 3;
	        session = connection.CreateSession(AcknowledgementMode.Transactional);
	        IDestination destination = session.CreateTemporaryQueue();
	        IDestination dlq = session.GetQueue("ActiveMQ.DLQ");
	        IMessageConsumer consumer = session.CreateConsumer(destination);
	        IMessageConsumer dlqConsumer = session.CreateConsumer(dlq);

			dlqConsumer.Listener += OnDLQMessage;
			consumer.Listener += OnMessage;

	        SendMessages(destination);
	        connection.Start();

			AssertReceived(MSG_COUNT, "Pre-Rollback expects to receive: " + MSG_COUNT + " messages.");

			consumer.Listener -= OnMessage;
			consumer.Listener += OnMessageAlwaysRollsBack;

			session.Rollback();

			AssertReceived(dlqed, MSG_COUNT, "Post-Rollback expects to DLQ: " + MSG_COUNT + " messages.");

	        session.Commit();
		}

		private void DeleteDLQ()
		{
	        session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
	        IDestination dlq = session.GetQueue("ActiveMQ.DLQ");
			try
			{
				connection.DeleteDestination(dlq);
			}
			catch
			{
			}
		}

	    private void SendMessages(IDestination destination) 
		{
	        IConnection connection = CreateConnection();
	        ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
	        IMessageProducer producer = session.CreateProducer(destination);
	        for(int i = 0; i < MSG_COUNT; ++i) 
			{
	            producer.Send(session.CreateTextMessage("" + i));
	        }
			connection.Close();
	    }
	}
}

