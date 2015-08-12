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
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class IndividualAckTest : NMSTestSupport
    {
        private IConnection connection;

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            connection = CreateConnection();
            connection.Start();
        }

        [TearDown]
        public override void TearDown()
        {
            connection.Close();
            base.TearDown();
        }

        [Test]
        public void TestAckedMessageAreConsumed()
        {
            ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);
            ITemporaryQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = session.CreateProducer(queue);
            producer.Send(session.CreateTextMessage("Hello"));

            // Consume the message...
            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(msg);
            msg.Acknowledge();

            // Reset the session.
            session.Close();
            session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);

            // Attempt to Consume the message...
            consumer = session.CreateConsumer(queue);
            msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNull(msg);

            session.Close();
        }

        [Test]
        public void TestLastMessageAcked()
        {
            ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);
            ITemporaryQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = session.CreateProducer(queue);
            ITextMessage msg1 = session.CreateTextMessage("msg1" + Guid.NewGuid().ToString());
            ITextMessage msg2 = session.CreateTextMessage("msg2" + Guid.NewGuid().ToString());
            ITextMessage msg3 = session.CreateTextMessage("msg3" + Guid.NewGuid().ToString());
            producer.Send(msg1);
            producer.Send(msg2);
            producer.Send(msg3);

            // Consume the message...
            IMessageConsumer consumer = session.CreateConsumer(queue);
            ITextMessage ackmsg = consumer.Receive(TimeSpan.FromMilliseconds(1000)) as ITextMessage;
            Assert.IsNotNull(ackmsg);
            Assert.AreEqual(msg1.Text,ackmsg.Text);
            ackmsg = consumer.Receive(TimeSpan.FromMilliseconds(1000)) as ITextMessage;
            Assert.IsNotNull(ackmsg);
            Assert.AreEqual(msg2.Text,ackmsg.Text);
            ackmsg = consumer.Receive(TimeSpan.FromMilliseconds(1000)) as ITextMessage;
            Assert.IsNotNull(ackmsg);
            Assert.AreEqual(msg3.Text,ackmsg.Text);
            ackmsg.Acknowledge();

            // Reset the session.
            session.Close();
            session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);

            // Attempt to Consume the message...
            consumer = session.CreateConsumer(queue);
            ackmsg = consumer.Receive(TimeSpan.FromMilliseconds(1000)) as ITextMessage;
            Assert.IsNotNull(ackmsg);
            Assert.AreEqual(msg1.Text,ackmsg.Text);
            ackmsg = consumer.Receive(TimeSpan.FromMilliseconds(1000)) as ITextMessage;
            Assert.IsNotNull(ackmsg);
            Assert.AreEqual(msg2.Text,ackmsg.Text);
            IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNull(msg);
            session.Close();
        }

        [Test]
        public void TestUnAckedMessageAreNotConsumedOnSessionClose()
        {
            ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);
            ITemporaryQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = session.CreateProducer(queue);
            producer.Send(session.CreateTextMessage("Hello"));

            // Consume the message...
            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(msg);
            // Don't ack the message.

            // Reset the session.  This should cause the unacknowledged message to be re-delivered.
            session.Close();
            session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);

            // Attempt to Consume the message...
            consumer = session.CreateConsumer(queue);
            msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
            Assert.IsNotNull(msg);
            msg.Acknowledge();

            session.Close();
        }

        [Test]
	    public void TestIndividualAcknowledgeMultiMessages_AcknowledgeFirstTest()
		{
            ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);

            // Push 2 messages to queue
            ITemporaryQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = session.CreateProducer(queue);

            ITextMessage msg = session.CreateTextMessage("test 1");
            producer.Send(msg, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.MinValue);
			msg = session.CreateTextMessage("test 2");
            producer.Send(msg, MsgDeliveryMode.Persistent, MsgPriority.Normal, TimeSpan.MinValue);
            producer.Close();

            IMessageConsumer consumer = session.CreateConsumer(queue);

            // Read the first message
            ITextMessage fetchedMessage1 = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(2000));
            Assert.IsNotNull(fetchedMessage1);
            Assert.AreEqual("test 1", fetchedMessage1.Text);

			// Read the second message
			ITextMessage fetchedMessage2 = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(2000));
            Assert.IsNotNull(fetchedMessage2);
            Assert.AreEqual("test 2", fetchedMessage2.Text);

            // Acknowledge first message
            fetchedMessage1.Acknowledge();

            consumer.Close();

            // Read first message a second time
            consumer = session.CreateConsumer(queue);
            fetchedMessage1 = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(2000));
            Assert.IsNotNull(fetchedMessage1);
            Assert.AreEqual("test 2", fetchedMessage1.Text);

            // Try to read second message a second time
            fetchedMessage2 = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(2000));
            Assert.IsNull(fetchedMessage2);
            consumer.Close();
	    }

        [Test]
        public void TestManyMessageAckedAfterMessageConsumption()
        {
            int messageCount = 20;
            IMessage msg;

            ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);
            ITemporaryQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = session.CreateProducer(queue);
            for(int i = 0; i < messageCount; i++)
            {
                msg = session.CreateTextMessage("msg" + i);
                producer.Send(msg);
            }

            // Consume the message...
            IMessageConsumer consumer = session.CreateConsumer(queue);
            for(int i = 0; i < messageCount; i++)
            {
                msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(msg);
                msg.Acknowledge();
            }
            msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNull(msg);

            // Reset the session.
            session.Close();
            session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);

            // Attempt to Consume the message...
            consumer = session.CreateConsumer(queue);
            msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNull(msg);
            session.Close();
        }

        [Test]
        public void TestManyMessageAckedAfterAllConsumption()
        {
            int messageCount = 20;
            IMessage msg;

            ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);
            ITemporaryQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = session.CreateProducer(queue);
            for(int i = 0; i < messageCount; i++)
            {
                msg = session.CreateTextMessage("msg" + i);
                producer.Send(msg);
            }

            // Consume the message...
            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage[] consumedMessages = new IMessage[messageCount];
            for(int i = 0; i < messageCount; i++)
            {
                msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(msg);
                consumedMessages[i] = msg;
            }
            for(int i = 0; i < messageCount; i++)
            {
                consumedMessages[i].Acknowledge();
            }
            msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNull(msg);

            // Reset the session.
            session.Close();
            session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge);

            // Attempt to Consume the message...
            consumer = session.CreateConsumer(queue);
            msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNull(msg);
            session.Close();
        }

        [Test]
        public void TestIndividualAcksWithClosedConsumerAndAuditSync()
        {
            const int MSG_COUNT = 20;
            const string QUEUE_NAME = "TEST.TestIndividualAcksWithClosedConsumerAndAuditSync";

            ProduceSomeMessages(MSG_COUNT, QUEUE_NAME);

            string uri = "failover:(tcp://${activemqhost}:61616)";
            IConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri));

            using (IConnection connection = factory.CreateConnection() as Connection)
            using (ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge))
            using (IQueue queue = session.GetQueue(QUEUE_NAME))
            {
                connection.Start();

                // Consume all messages with no ACK
                using (IMessageConsumer consumer = session.CreateConsumer(queue))
                {
                    for (int i = 0; i < MSG_COUNT; ++i)
                    {
                        IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                        Assert.NotNull(message);
                        Tracer.DebugFormat("Received message: {0}", message.NMSMessageId);
                    }
                }

                // Consumer the same batch again.
                using (IMessageConsumer consumer = session.CreateConsumer(queue))
                {
                    for (int i = 0; i < MSG_COUNT; ++i)
                    {
                        IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                        Assert.NotNull(message);
                        Tracer.DebugFormat("Received message: {0}", message.NMSMessageId);
                    }
                }

                session.DeleteDestination(queue);
            }
        }

        private void ProduceSomeMessages(int count, string queueName)
        {
            using (IConnection connection = CreateConnection())
            using (ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge))
            using (IQueue queue = session.GetQueue(queueName))
            using (IMessageProducer producer = session.CreateProducer(queue))
            {
                for (int i = 0; i < count; ++i)
                {
                    producer.Send(session.CreateMessage());
                }
            }
        }
    }
}
