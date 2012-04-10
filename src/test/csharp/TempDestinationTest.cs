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
                Assert.IsTrue(true, "failed to throw an exception");
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
            Assert.IsTrue(((ITextMessage)message2).Text.Equals(message.Text),
                          "Expected message to be a '" + message.Text + "'");
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
            Assert.IsTrue(((ITextMessage)message2).Text.Equals(message.Text),
                          "Expected message to be a '" + message.Text + "'");
    
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
            for (int i = 0; i < count; i++)
            {
                IBytesMessage message = session.CreateBytesMessage();
                message.WriteBytes(data);
                message.Properties.SetInt("c", i);
                producer.Send(message);
                list.Add(message);
            }

            connection.Start();
            IMessageConsumer consumer = session.CreateConsumer(queue);
            for (int i = 0; i < count; i++)
            {
                IMessage message2 = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                Assert.IsTrue(message2 != null);
                Assert.AreEqual(i, message2.Properties.GetInt("c"));
                Assert.IsTrue(message2.Equals(list[i]));
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
            Thread.Sleep(5000); // Wait a little bit to let the delete take effect.
    
            // This message delivery NOT should work since the temp connection is
            // now closed.
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

            // This message delivery should work since the temp connection is still
            // open.
            IMessageProducer producer = session.CreateProducer(queue);
            producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
            ITextMessage message = session.CreateTextMessage("First");
            producer.Send(message);
            Thread.Sleep(1000);

            // deleting the Queue will cause sends to fail
            queue.Delete();
            Thread.Sleep(5000); // Wait a little bit to let the delete take effect.
    
            // This message delivery NOT should work since the temp connection is
            // now closed.
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
		public void TestConsumeAfterPublishFailsForDestroyedTempDestination()
		{
			const string msgQueueName = "Test.RequestReply.MsgQueue";
			Connection consumerConnection = GetNewConnection();
			ISession consumerSession = consumerConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			IDestination consumerDestination = consumerSession.GetQueue(msgQueueName);
			IMessageConsumer consumer = consumerSession.CreateConsumer(consumerDestination);

			consumerConnection.Start();

			// The real test is whether sending a message to a deleted temp queue messes up
			// the consumers on the same connection.
			for(int index = 0; index < 25; index++)
			{
				Connection producerConnection = GetNewConnection();
				ISession producerSession = producerConnection.CreateSession(AcknowledgementMode.AutoAcknowledge);
				IDestination producerDestination = producerSession.GetQueue(msgQueueName);
				IMessageProducer producer = producerSession.CreateProducer(producerDestination);
				IDestination replyDestination = producerSession.CreateTemporaryQueue();
				IMessageConsumer replyConsumer = producerSession.CreateConsumer(replyDestination);

				producerConnection.Start();

				IMessage sendMsg = producer.CreateTextMessage("Consumer check.");
				sendMsg.NMSReplyTo = replyDestination;

				producer.Send(sendMsg);

				// Will the following Receive() call fail on the second or subsequent calls?
				IMessage receiveMsg = consumer.Receive();
				IMessageProducer replyProducer = consumerSession.CreateProducer(receiveMsg.NMSReplyTo);

				connections.Remove(producerConnection);
				producerConnection.Close();
				Thread.Sleep(1000); // Wait a little bit to let the delete take effect.

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
                Assert.IsTrue(true, "failed to throw an exception");
            }
        }

		/// <summary>
		/// Test clean up of multiple temp destinations
		/// </summary>
		[Test]
		public void TestCloseConnectionWithTempQueues()
		{
			List<ITemporaryQueue> listTempQueues = new List<ITemporaryQueue>();
			IConnection connection = CreateConnection();
			ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);

			connection.Start();

			for(int index = 0; index < 25; index++)
			{
				listTempQueues.Add(session.CreateTemporaryQueue());
			}

			connection.Close();
		}
	}
}

