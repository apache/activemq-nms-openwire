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
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Threading;
using Apache.NMS;
using Apache.NMS.Test;
using Apache.NMS.Util;
using Apache.NMS.Policies;
using Apache.NMS.ActiveMQ.Commands;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class TempDestinationTest : NMSTestSupport
	{
		private readonly IList connections = ArrayList.Synchronized(new ArrayList());

		[SetUp]
		public override void SetUp()
		{
			base.SetUp();

			lock(this.tempDestsAdded.SyncRoot)
			{
				this.tempDestsAdded.Clear();
			}
			
			lock(this.tempDestsRemoved.SyncRoot)
			{
				this.tempDestsRemoved.Clear();
			}
		}

		[TearDown]
		public override void TearDown()
		{
			foreach(Connection connection in connections)
			{
				try
				{
					connection.Close();
				}
				catch
				{
				}
			}

			connections.Clear();
			base.TearDown();
		}

		private Connection GetNewConnection()
		{
			Connection newConnection = CreateConnection() as Connection;
			connections.Add(newConnection);
			return newConnection;
		}

		/// <summary>
		/// Make sure Temp destination can only be consumed by local connection
		/// </summary>
		[Test]
		public void TestTempDestOnlyConsumedByLocalConn()
		{
			Connection connection = GetNewConnection();
			connection.Start();

			ISession tempSession = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			ITemporaryQueue queue = tempSession.CreateTemporaryQueue();
			IMessageProducer producer = tempSession.CreateProducer(queue);
			producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
			ITextMessage message = tempSession.CreateTextMessage("First");
			producer.Send(message);

			// temp destination should not be consume when using another connection
			Connection otherConnection = GetNewConnection();
			ISession otherSession = otherConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			ITemporaryQueue otherQueue = otherSession.CreateTemporaryQueue();
			IMessageConsumer consumer = otherSession.CreateConsumer(otherQueue);
			IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(3000));
			Assert.IsNull(msg);

			// should throw InvalidDestinationException when consuming a temp
			// destination from another connection
			try
			{
				consumer = otherSession.CreateConsumer(queue);
				Assert.Fail("Send should fail since temp destination should be used from another connection");
			}
			catch(InvalidDestinationException)
			{
			}

			// should be able to consume temp destination from the same connection
			consumer = tempSession.CreateConsumer(queue);
			msg = consumer.Receive(TimeSpan.FromMilliseconds(3000));
			Assert.NotNull(msg);
		}

		/// <summary>
		/// Make sure that a temp queue does not drop message if there is an active consumers.
		/// </summary>
		[Test]
		public void TestTempQueueHoldsMessagesWithConsumers()
		{
			Connection connection = GetNewConnection();
			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			IQueue queue = session.CreateTemporaryQueue();
			IMessageConsumer consumer = session.CreateConsumer(queue);
			connection.Start();

			IMessageProducer producer = session.CreateProducer(queue);
			producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
			ITextMessage message = session.CreateTextMessage("Hello");
			producer.Send(message);

			IMessage message2 = consumer.Receive(TimeSpan.FromMilliseconds(1000));
			Assert.IsNotNull(message2);
			Assert.IsTrue(message2 is ITextMessage, "Expected message to be a TextMessage");
			Assert.AreEqual(message.Text, ((ITextMessage)message2).Text);
		}

		/// <summary>
		/// Make sure that a temp queue does not drop message if there are no active consumers.
		/// </summary>
		[Test]
		public void TestTempQueueHoldsMessagesWithoutConsumers()
		{
			Connection connection = GetNewConnection();
			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			IQueue queue = session.CreateTemporaryQueue();
			IMessageProducer producer = session.CreateProducer(queue);
			producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
			ITextMessage message = session.CreateTextMessage("Hello");
			producer.Send(message);

			connection.Start();
			IMessageConsumer consumer = session.CreateConsumer(queue);
			IMessage message2 = consumer.Receive(TimeSpan.FromMilliseconds(3000));
			Assert.IsNotNull(message2);
			Assert.IsTrue(message2 is ITextMessage, "Expected message to be a TextMessage");
			Assert.AreEqual(message.Text, ((ITextMessage)message2).Text);
		}

		/// <summary>
		/// Test temp queue works under load
		/// </summary>
		[Test]
		public void TestTmpQueueWorksUnderLoad()
		{
			int count = 500;
			int dataSize = 1024;

			ArrayList list = new ArrayList(count);
			Connection connection = GetNewConnection();
			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			IQueue queue = session.CreateTemporaryQueue();
			IMessageProducer producer = session.CreateProducer(queue);
			producer.DeliveryMode = MsgDeliveryMode.NonPersistent;

			byte[] data = new byte[dataSize];
			for(int i = 0; i < count; i++)
			{
				IBytesMessage message = session.CreateBytesMessage();
				message.WriteBytes(data);
				message.Properties.SetInt("c", i);
				producer.Send(message);
				list.Add(message);
			}

			connection.Start();
			IMessageConsumer consumer = session.CreateConsumer(queue);
			for(int i = 0; i < count; i++)
			{
				IMessage message2 = consumer.Receive(TimeSpan.FromMilliseconds(2000));
				Assert.IsNotNull(message2);
				Assert.AreEqual(i, message2.Properties.GetInt("c"));
				Assert.AreEqual(list[i], message2);
			}
		}

		/// <summary>
		/// Make sure you cannot publish to a temp destination that does not exist anymore.
		/// </summary>
		[Test]
		public void TestPublishFailsForClosedConnection()
		{
			Connection connection = GetNewConnection();
			Connection tempConnection = GetNewConnection();
			ISession tempSession = tempConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			ITemporaryQueue queue = tempSession.CreateTemporaryQueue();

			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			connection.Start();

			IMessageConsumer advisoryConsumer = session.CreateConsumer(AdvisorySupport.TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC);
			advisoryConsumer.Listener += OnAdvisoryMessage;

			// This message delivery should work since the temp connection is still
			// open.
			IMessageProducer producer = session.CreateProducer(queue);
			producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
			ITextMessage message = session.CreateTextMessage("First");
			producer.Send(message);
			Thread.Sleep(1000);

			// Closing the connection should destroy the temp queue that was
			// created.
			tempConnection.Close();
			WaitForTempDestinationDelete(queue);

			// This message delivery should NOT work since the temp connection is now closed.
			try
			{
				message = session.CreateTextMessage("Hello");
				producer.Send(message);
				Assert.Fail("Send should fail since temp destination should not exist anymore.");
			}
			catch(NMSException e)
			{
				Tracer.Debug("Test threw expected exception: " + e.Message);
			}
		}

		/// <summary>
		/// Make sure you cannot publish to a temp destination that does not exist anymore.
		/// </summary>
		[Test]
		public void TestPublishFailsForDestroyedTempDestination()
		{
			Connection connection = GetNewConnection();
			Connection tempConnection = GetNewConnection();
			ISession tempSession = tempConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			ITemporaryQueue queue = tempSession.CreateTemporaryQueue();

			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			connection.Start();

			IMessageConsumer advisoryConsumer = session.CreateConsumer(AdvisorySupport.TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC);
			advisoryConsumer.Listener += OnAdvisoryMessage;

			// This message delivery should work since the temp connection is still open.
			IMessageProducer producer = session.CreateProducer(queue);
			producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
			ITextMessage message = session.CreateTextMessage("First");
			producer.Send(message);
			Thread.Sleep(1000);

			// deleting the Queue will cause sends to fail
			queue.Delete();
			WaitForTempDestinationDelete(queue);

			// This message delivery should NOT work since the temp connection is now closed.
			try
			{
				message = session.CreateTextMessage("Hello");
				producer.Send(message);
				Assert.Fail("Send should fail since temp destination should not exist anymore.");
			}
			catch(NMSException e)
			{
				Tracer.Debug("Test threw expected exception: " + e.Message);
			}
		}

		/// <summary>
		/// Make sure consumers work after a publisher fails to publish to deleted temp destination.
		/// </summary>
		[Test]
		[TestCase(MsgDeliveryMode.Persistent)]
		[TestCase(MsgDeliveryMode.NonPersistent)]
		public void TestConsumeAfterPublishFailsForDestroyedTempDestination(MsgDeliveryMode replyDeliveryMode)
		{
			const string msgQueueName = "Test.RequestReply.MsgQueue";
			Connection consumerConnection = GetNewConnection();
			ISession consumerSession = consumerConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			IDestination consumerDestination = consumerSession.GetQueue(msgQueueName);
			// Make sure we have a fresh test queue.
			consumerConnection.DeleteDestination(consumerDestination);
			IMessageConsumer consumer = consumerSession.CreateConsumer(consumerDestination);

			consumerConnection.Start();

			IMessageConsumer advisoryConsumer = consumerSession.CreateConsumer(AdvisorySupport.TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC);
			advisoryConsumer.Listener += OnAdvisoryMessage;

			// The real test is whether sending a message to a deleted temp queue messes up
			// the consumers on the same connection.
			for(int index = 0; index < 25; index++)
			{
				Tracer.InfoFormat("LOOP #{0} ---------------------------------------------------", index + 1);
				Connection producerConnection = GetNewConnection();
				ISession producerSession = producerConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
				IDestination producerDestination = producerSession.GetQueue(msgQueueName);
				IMessageProducer producer = producerSession.CreateProducer(producerDestination);
				IDestination replyDestination = producerSession.CreateTemporaryQueue();

				producerConnection.Start();

				IMessage sendMsg = producer.CreateTextMessage("Consumer check.");
				sendMsg.NMSReplyTo = replyDestination;

				producer.Send(sendMsg);

				// Will the following Receive() call fail on the second or subsequent calls?
				IMessage receiveMsg = consumer.Receive();
				IMessageProducer replyProducer = consumerSession.CreateProducer(receiveMsg.NMSReplyTo);
				replyProducer.DeliveryMode = replyDeliveryMode;

				connections.Remove(producerConnection);
				producerConnection.Close();

				WaitForTempDestinationDelete(replyDestination);

				// This message delivery NOT should work since the temp destination was removed by closing the connection.
				try
				{
					IMessage replyMsg = replyProducer.CreateTextMessage("Reply check.");
					replyProducer.Send(replyMsg);
					Assert.Fail("Send should fail since temp destination should not exist anymore.");
				}
				catch(NMSException e)
				{
					Tracer.Debug("Test threw expected exception: " + e.Message);
				}
			}
		}

		private void WaitForTempDestinationAdd(IDestination tempDestination)
		{
			const int MaxLoopCount = 200;
			int loopCount = 0;
			bool destinationAdded = false;
			ActiveMQTempDestination amqTempDestination = tempDestination as ActiveMQTempDestination;

			while(!destinationAdded)
			{
				loopCount++;
				if(loopCount > MaxLoopCount)
				{
					Assert.Fail(string.Format("Timeout waiting for Add of {0}", amqTempDestination.PhysicalName));
				}

				Thread.Sleep(10);
				lock(this.tempDestsAdded.SyncRoot)
				{
					foreach(ActiveMQTempDestination tempDest in this.tempDestsAdded)
					{
						if(0 == string.Compare(tempDest.PhysicalName, amqTempDestination.PhysicalName, true))
						{
							destinationAdded = true;
							break;
						}
					}
				}
			}
		}

		private void WaitForTempDestinationDelete(IDestination tempDestination)
		{
			const int MaxLoopCount = 200;
			int loopCount = 0;
			bool destinationDeleted = false;
			ActiveMQTempDestination amqTempDestination = tempDestination as ActiveMQTempDestination;

			while(!destinationDeleted)
			{
				loopCount++;
				if(loopCount > MaxLoopCount)
				{
					Assert.Fail(string.Format("Timeout waiting for Delete of {0}", amqTempDestination.PhysicalName));
				}

				Thread.Sleep(10);
				lock(this.tempDestsRemoved.SyncRoot)
				{
					foreach(ActiveMQTempDestination tempDest in this.tempDestsRemoved)
					{
						if(0 == string.Compare(tempDest.PhysicalName, amqTempDestination.PhysicalName, true))
						{
							destinationDeleted = true;
							break;
						}
					}
				}
			}
		}

		/// <summary>
		/// Test you can't delete a Destination with Active Subscribers
		/// </summary>
		[Test]
		public void TestDeleteDestinationWithSubscribersFails()
		{
			Connection connection = GetNewConnection();
			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			ITemporaryQueue queue = session.CreateTemporaryQueue();

			connection.Start();

			session.CreateConsumer(queue);

			try
			{
				queue.Delete();
				Assert.Fail("Should fail as Subscribers are active");
			}
			catch(NMSException)
			{
			}
		}

		/// <summary>
		/// Test clean up of multiple temp destinations
		/// </summary>
		[Test]
		public void TestCloseConnectionWithTempQueues()
		{
			List<ITemporaryQueue> listTempQueues = new List<ITemporaryQueue>();
			// Don't call GetNewConnection(), because we want to close the connection within this test.
			IConnection connection = CreateConnection();
			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

			connection.Start();

			for(int index = 0; index < 25; index++)
			{
				listTempQueues.Add(session.CreateTemporaryQueue());
			}

			connection.Close();
		}

		[Test]
		public void TestConnectionCanPurgeTempDestinations()
		{
			Connection connection = GetNewConnection();
			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			IMessageConsumer advisoryConsumer = session.CreateConsumer(AdvisorySupport.TEMP_DESTINATION_COMPOSITE_ADVISORY_TOPIC);
			advisoryConsumer.Listener += OnAdvisoryMessage;

			connection.Start();

			List<ITemporaryTopic> tempTopics = new List<ITemporaryTopic>();

			for(int i = 0; i < 10; ++i)
			{
				ITemporaryTopic tempTopic = session.CreateTemporaryTopic();
				tempTopics.Add(tempTopic);
				WaitForTempDestinationAdd(tempTopic);
				Tracer.Debug("Created TempDestination: " + tempTopic);
			}

			// Create one from an alternate connection, it shouldn't get purged
			// so we should have one less removed than added entries.
			Connection connection2 = GetNewConnection();
			ISession session2 = connection2.CreateSession(AcknowledgementMode.AutoAcknowledge);
			ITemporaryTopic tempTopic2 = session2.CreateTemporaryTopic();

			WaitForTempDestinationAdd(tempTopic2);
			Assert.AreEqual(11, tempDestsAdded.Count);

			connection.PurgeTempDestinations();

			foreach(ITemporaryTopic tempTopic in tempTopics)
			{
				WaitForTempDestinationDelete(tempTopic);
			}

			Assert.AreEqual(10, tempDestsRemoved.Count);
		}

		private readonly IList tempDestsAdded = ArrayList.Synchronized(new ArrayList());
		private readonly IList tempDestsRemoved = ArrayList.Synchronized(new ArrayList());

		private void OnAdvisoryMessage(IMessage msg)
		{
			Message message = msg as Message;
			DestinationInfo destInfo = message.DataStructure as DestinationInfo;

			if(destInfo != null)
			{
				ActiveMQDestination dest = destInfo.Destination;
				if(!dest.IsTemporary)
				{
					return;
				}

				ActiveMQTempDestination tempDest = dest as ActiveMQTempDestination;
				if(destInfo.OperationType == DestinationInfo.ADD_OPERATION_TYPE)
				{
					if(Tracer.IsDebugEnabled)
					{
						Tracer.Debug("Connection adding: " + tempDest);
					}
					
					lock(this.tempDestsAdded.SyncRoot)
					{
						this.tempDestsAdded.Add(tempDest);
					}
				}
				else if(destInfo.OperationType == DestinationInfo.REMOVE_OPERATION_TYPE)
				{
					if(Tracer.IsDebugEnabled)
					{
						Tracer.Debug("Connection removing: " + tempDest);
					}
					
					lock(this.tempDestsRemoved.SyncRoot)
					{
						this.tempDestsRemoved.Add(tempDest);
					}
				}
			}
		}
	}
}

