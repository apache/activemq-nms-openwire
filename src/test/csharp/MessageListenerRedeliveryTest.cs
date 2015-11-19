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
using System.Collections;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.Test;
using Apache.NMS.Policies;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class MessageListenerRedeliveryTest : NMSTestSupport
    {
        private Connection connection;
        private volatile int counter;
        private ISession session;
        private ArrayList received;
        private ArrayList dlqMessages;
        private int maxDeliveries;

        private CountDownLatch gotOneMessage;
        private CountDownLatch gotTwoMessages;
        private CountDownLatch gotOneDlqMessage;
        private CountDownLatch gotMaxRedeliveries;

        [SetUp]
        public override void SetUp()
        {
            this.connection = (Connection) CreateConnection();
            this.connection.RedeliveryPolicy = GetRedeliveryPolicy();
            this.gotOneMessage = new CountDownLatch(1);
            this.gotTwoMessages = new CountDownLatch(2);
            this.gotOneDlqMessage = new CountDownLatch(1);
            this.maxDeliveries = GetRedeliveryPolicy().MaximumRedeliveries;
            this.gotMaxRedeliveries = new CountDownLatch(maxDeliveries);
            this.received = new ArrayList();
            this.dlqMessages = new ArrayList();
            this.counter = 0;
        }

        [TearDown]
        public override void TearDown()
        {
            this.session = null;

            if(this.connection != null)
            {
                this.connection.Close();
                this.connection = null;
            }
            
            base.TearDown();
        }
    
        protected IRedeliveryPolicy GetRedeliveryPolicy() 
        {
            RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
            redeliveryPolicy.InitialRedeliveryDelay = 1000;
            redeliveryPolicy.MaximumRedeliveries = 3;
            redeliveryPolicy.BackOffMultiplier = (short)2;
            redeliveryPolicy.UseExponentialBackOff = true;
            return redeliveryPolicy;
        }
    
        private void OnMessageListener(IMessage message)
        {        
            counter++;
            if(this.counter <= 4) 
            {
                session.Rollback();
            }
            else
            {
                message.Acknowledge();
                session.Commit();
            }
        }

        private void OnTracedReceiveMessage(IMessage message) 
        {
            try 
            {
                received.Add(((ITextMessage) message).Text);
            } 
            catch (Exception e) 
            {
                Assert.Fail("Error: " + e.Message);
            }

            if (++counter < maxDeliveries) 
            {
                throw new Exception("force a redelivery");
            }

            // new blood
            counter = 0;
            gotTwoMessages.countDown();
        }

        private void OnDlqMessage(IMessage message) 
        {
            dlqMessages.Add(message);
            gotOneDlqMessage.countDown();
        }

        private void OnRedeliveredMessage(IMessage message) 
        {
            gotMaxRedeliveries.countDown();
            throw new Exception("Test Forcing a Rollback");
        }

        [Test]
        public void TestQueueRollbackConsumerListener() 
        {
            connection.Start();
    
            this.session = connection.CreateSession(AcknowledgementMode.Transactional);
            ITemporaryQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = session.CreateProducer(queue);
            IMessage message = session.CreateTextMessage("Test Message");
            producer.Send(message);
            session.Commit();
    
            IMessageConsumer consumer = session.CreateConsumer(queue);

            consumer.Listener += new MessageListener(OnMessageListener);

            Thread.Sleep(500);

            // first try.. should get 2 since there is no delay on the
            // first redeliver..
            Assert.AreEqual(2, counter);
    
            Thread.Sleep(1000);
    
            // 2nd redeliver (redelivery after 1 sec)
            Assert.AreEqual(3, counter);
    
            Thread.Sleep(2000);
    
            // 3rd redeliver (redelivery after 2 seconds) - it should give up after
            // that
            Assert.AreEqual(4, counter);
    
            // create new message
            producer.Send(session.CreateTextMessage("Test Message Again"));
            session.Commit();
    
            Thread.Sleep(500);
            
            // it should be committed, so no redelivery
            Assert.AreEqual(5, counter);
    
            Thread.Sleep(1500);

            // no redelivery, counter should still be 5
            Assert.AreEqual(5, counter);
    
            session.Close();
        }

        [Test]
        public void TestQueueRollbackSessionListener()
        {
            connection.Start();

            this.session = connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = CreateProducer(session, queue);
            IMessage message = CreateTextMessage(session);
            producer.Send(message);
            session.Commit();

            IMessageConsumer consumer = session.CreateConsumer(queue);
            consumer.Listener += new MessageListener(OnMessageListener);

            Thread.Sleep(1000);

            // first try
            Assert.AreEqual(2, counter);

            Thread.Sleep(1500);

            // second try (redelivery after 1 sec)
            Assert.AreEqual(3, counter);

            Thread.Sleep(3000);

            // third try (redelivery after 2 seconds) - it should give up after that
            Assert.AreEqual(4, counter);

            // create new message
            producer.Send(CreateTextMessage(session));
            session.Commit();
          
            Thread.Sleep(1000);

            // it should be committed, so no redelivery
            Assert.AreEqual(5, counter);

            Thread.Sleep(2000);

            // no redelivery, counter should still be 4
            Assert.AreEqual(5, counter);

            session.Close();
        }

        [Test]
        public void TestQueueSessionListenerExceptionRetry()
        {
            connection.Start();

            ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = CreateProducer(session, queue);
            IMessage message = CreateTextMessage(session, "1");
            producer.Send(message);
            message = CreateTextMessage(session, "2");
            producer.Send(message);

            IMessageConsumer consumer = session.CreateConsumer(queue);
            consumer.Listener += new MessageListener(OnTracedReceiveMessage);

            Assert.IsTrue(gotTwoMessages.await(TimeSpan.FromSeconds(20)), "got message before retry expiry");

            for (int i = 0; i < maxDeliveries; i++)
            {
                Assert.AreEqual("1", received[i], "got first redelivered: " + i);
            }
            for (int i = maxDeliveries; i < maxDeliveries * 2; i++)
            {
                Assert.AreEqual("2", received[i], "got first redelivered: " + i);
            }

            session.Close();
        }

        [Test]
        public void TestQueueSessionListenerExceptionDlq()
        {
            connection.Start();

            session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = CreateProducer(session, queue);
            IMessage message = CreateTextMessage(session);
            producer.Send(message);

            IDestination dlqDestination = session.GetQueue("ActiveMQ.DLQ");
            connection.DeleteDestination(dlqDestination);
            IMessageConsumer dlqConsumer = session.CreateConsumer(dlqDestination);
            dlqConsumer.Listener += new MessageListener(OnDlqMessage);           

            IMessageConsumer consumer = session.CreateConsumer(queue);
            consumer.Listener += new MessageListener(OnRedeliveredMessage);

            Assert.IsTrue(gotMaxRedeliveries.await(TimeSpan.FromSeconds(20)), "got message before retry expiry");

            // check DLQ
            Assert.IsTrue(gotOneDlqMessage.await(TimeSpan.FromSeconds(20)), "got dlq message");

            // check DLQ message cause is captured
            message = dlqMessages[0] as IMessage;
            Assert.IsNotNull(message, "dlq message captured");
            String cause = message.Properties.GetString("dlqDeliveryFailureCause");

            Assert.IsTrue(cause.Contains("JMSException"), "cause 'cause' exception is remembered");
            Assert.IsTrue(cause.Contains("Test"), "is correct exception");
            Assert.IsTrue(cause.Contains("RedeliveryPolicy"), "cause policy is remembered");

            session.Close();
        }

        private void OnMessageThenRollback(IMessage message)
        {
            gotOneMessage.countDown();
            try
            {
                session.Rollback();
            } 
            catch (Exception) 
            {
            }

            throw new Exception("Test force a redelivery");
        }

        [Test]
        public void TestTransactedQueueSessionListenerExceptionDlq()
        {
            connection.Start();

            session = connection.CreateSession(AcknowledgementMode.Transactional);
            IQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = CreateProducer(session, queue);
            IMessage message = CreateTextMessage(session);
            producer.Send(message);
            session.Commit();

            IDestination dlqDestination = session.GetQueue("ActiveMQ.DLQ");
            connection.DeleteDestination(dlqDestination);
            IMessageConsumer dlqConsumer = session.CreateConsumer(dlqDestination);
            dlqConsumer.Listener += new MessageListener(OnDlqMessage);           

            IMessageConsumer consumer = session.CreateConsumer(queue);
            consumer.Listener += new MessageListener(OnMessageThenRollback);

            Assert.IsTrue(gotOneMessage.await(TimeSpan.FromSeconds(20)), "got message before retry expiry");

            // check DLQ
            Assert.IsTrue(gotOneDlqMessage.await(TimeSpan.FromSeconds(20)), "got dlq message");

            // check DLQ message cause is captured
            message = dlqMessages[0] as IMessage;
            Assert.IsNotNull(message, "dlq message captured");
            String cause = message.Properties.GetString("dlqDeliveryFailureCause");

            Assert.IsTrue(cause.Contains("JMSException"), "cause 'cause' exception is remembered");
            Assert.IsTrue(cause.Contains("Test force"), "is correct exception");
            Assert.IsTrue(cause.Contains("RedeliveryPolicy"), "cause policy is remembered");

            session.Close();
        }

        private ITextMessage CreateTextMessage(ISession session, String text)
        {
            return session.CreateTextMessage(text);
        }

        private ITextMessage CreateTextMessage(ISession session)
        {
            return session.CreateTextMessage("Hello");
        }

        private IMessageProducer CreateProducer(ISession session, IDestination queue)
        {
            IMessageProducer producer = session.CreateProducer(queue);
            producer.DeliveryMode = GetDeliveryMode();
            return producer;
        }

        protected MsgDeliveryMode GetDeliveryMode() 
        {
            return MsgDeliveryMode.Persistent;
        }
    }
}
