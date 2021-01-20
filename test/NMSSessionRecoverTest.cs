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
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Test;
using Apache.NMS.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class NMSSessionRecoverTest : NMSTestSupport
    {
        private IConnection connection;
        private IDestination destination;
        private CountDownLatch doneCountDownLatch;
        private ISession session;
        private int counter;
        private String errorMessage;

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            counter = 0;
            errorMessage = null;
            doneCountDownLatch = new CountDownLatch(1);
            connection = CreateConnection();
        }

        [TearDown]
        public override void TearDown()
        {
            base.TearDown();

            if (connection != null)
            {
                connection.Close();
            }
        }

        [Test]
        public void TestQueueSynchRecover()
        {
            destination = new ActiveMQQueue("TEST.Queue-" + DateTime.Now.Ticks);
            DoTestSynchRecover();
        }

        [Test]
        public void TestQueueAsynchRecover()
        {
			destination = new ActiveMQQueue("TEST.Queue-" + DateTime.Now.Ticks);
            DoTestAsynchRecover();
        }

        [Test]
        public void TestTopicSynchRecover()
        {
			destination = new ActiveMQTopic("TEST.Topic-" + DateTime.Now.Ticks);
            DoTestSynchRecover();
        }

        [Test]
        public void TestTopicAsynchRecover()
        {
			destination = new ActiveMQTopic("TEST.Topic-" + DateTime.Now.Ticks);
            DoTestAsynchRecover();
        }

        [Test]
        public void TestQueueAsynchRecoverWithAutoAck()
        {
			destination = new ActiveMQQueue("TEST.Queue-" + DateTime.Now.Ticks);
            DoTestAsynchRecoverWithAutoAck();
        }

        [Test]
        public void TestTopicAsynchRecoverWithAutoAck()
        {
			destination = new ActiveMQTopic("TEST.Topic-" + DateTime.Now.Ticks);
            DoTestAsynchRecoverWithAutoAck();
        }

        public void DoTestSynchRecover()
        {
            session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
            IMessageConsumer consumer = session.CreateConsumer(destination);
            connection.Start();
    
            IMessageProducer producer = session.CreateProducer(destination);
            producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
            producer.Send(session.CreateTextMessage("First"));
            producer.Send(session.CreateTextMessage("Second"));

            ITextMessage message = consumer.Receive(TimeSpan.FromMilliseconds(2000)) as ITextMessage;
            Assert.AreEqual("First", message.Text);
            Assert.IsFalse(message.NMSRedelivered);
            message.Acknowledge();
    
            message = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(2000));
            Assert.AreEqual("Second", message.Text);
            Assert.IsFalse(message.NMSRedelivered);
    
            session.Recover();
    
            message = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(3000));
            Assert.AreEqual("Second", message.Text);
            Assert.IsTrue(message.NMSRedelivered);
    
            message.Acknowledge();
        }

        private void OnTestAsynchRecoverMessage(IMessage msg)
        {
            counter++;
            try
            {
                ITextMessage message = msg as ITextMessage;
                switch (counter)
                {
                case 1:
                    Tracer.Debug("Got first Message: " + message.Text);
                    Assert.AreEqual("First", message.Text);
                    Assert.IsFalse(message.NMSRedelivered);
                    message.Acknowledge();
                    break;
                case 2:
                    Tracer.Debug("Got Second Message: " + message.Text);
                    Assert.AreEqual("Second", message.Text);
                    Assert.IsFalse(message.NMSRedelivered);
                    session.Recover();
                    break;
                case 3:
                    Tracer.Debug("Got Third Message: " + message.Text);
                    Assert.AreEqual("Second", message.Text);
                    Assert.IsTrue(message.NMSRedelivered);
                    message.Acknowledge();
                    doneCountDownLatch.countDown();
                    break;
                default:
                    errorMessage = "Got too many messages: " + counter;
                    Tracer.Debug(errorMessage);
                    doneCountDownLatch.countDown();
                    break;
                }
            }
            catch (Exception e)
            {
                errorMessage = "Got exception: " + e.Message;
                Tracer.Warn("Exception on Message Receive: " + e.Message);
                doneCountDownLatch.countDown();
            }
        }

        public void DoTestAsynchRecover()
        {
            session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);

            IMessageConsumer consumer = session.CreateConsumer(destination);
    
            IMessageProducer producer = session.CreateProducer(destination);
            producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
            producer.Send(session.CreateTextMessage("First"));
            producer.Send(session.CreateTextMessage("Second"));

            consumer.Listener += OnTestAsynchRecoverMessage;
            connection.Start();

            if (doneCountDownLatch.await(TimeSpan.FromSeconds(10)))
            {
                if (!String.IsNullOrEmpty(errorMessage))
                {
                    Assert.Fail(errorMessage);
                }
            }
            else
            {
                Assert.Fail("Timeout waiting for async message delivery to complete.");
            }
        }

        private void OnTestAsynchRecoverWithAutoAck(IMessage msg)
        {
            counter++;
            try
            {
                ITextMessage message = msg as ITextMessage;
                switch (counter)
                {
                case 1:
                    Tracer.Debug("Got first Message: " + message.Text);
                    Assert.AreEqual("First", message.Text);
                    Assert.IsFalse(message.NMSRedelivered);
                    break;
                case 2:
                    // This should rollback the delivery of this message..
                    // and re-deliver.
                    Tracer.Debug("Got Second Message: " + message.Text);
                    Assert.AreEqual("Second", message.Text);
                    Assert.IsFalse(message.NMSRedelivered);
                    session.Recover();
                    break;
                case 3:
                    Tracer.Debug("Got Third Message: " + message.Text);
                    Assert.AreEqual("Second", message.Text);
                    Assert.IsTrue(message.NMSRedelivered);
                    doneCountDownLatch.countDown();
                    break;
                default:
                    errorMessage = "Got too many messages: " + counter;
                    Tracer.Debug(errorMessage);
                    doneCountDownLatch.countDown();
                    break;
                }
            }
            catch (Exception e)
            {
                errorMessage = "Got exception: " + e.Message;
                Tracer.Warn("Exception on Message Receive: " + e.Message);
                doneCountDownLatch.countDown();
            }
        }

        public void DoTestAsynchRecoverWithAutoAck()
        {
            session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IMessageConsumer consumer = session.CreateConsumer(destination);

            IMessageProducer producer = session.CreateProducer(destination);
            producer.DeliveryMode = MsgDeliveryMode.NonPersistent;
            producer.Send(session.CreateTextMessage("First"));
            producer.Send(session.CreateTextMessage("Second"));

            consumer.Listener += OnTestAsynchRecoverWithAutoAck;
            connection.Start();

            if (doneCountDownLatch.await(TimeSpan.FromSeconds(10)))
            {
                Tracer.Info("Finished waiting for async message delivery to complete.");
                if (!String.IsNullOrEmpty(errorMessage))
                {
                    Assert.Fail(errorMessage);
                }
            }
            else
            {
                Tracer.Warn("Timeout waiting for async message delivery to complete.");
                Assert.Fail("Timeout waiting for async message delivery to complete.");
            }
        }
    }
}

