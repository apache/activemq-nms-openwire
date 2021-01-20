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
using Apache.NMS.ActiveMQ.Commands;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{    
    [TestFixture]
    public class ZeroPrefetchConsumerTest : NMSTestSupport
    {
        protected IConnection connection;
        protected IQueue queue;

        public void OnMessageFailTest(IMessage message)
        {
        }

        [Test]
        public void TestCannotUseMessageListener()
        {
            ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IMessageConsumer consumer = session.CreateConsumer(queue);
    
            try 
            {
                consumer.Listener += new MessageListener(OnMessageFailTest);
                Assert.Fail("Should have thrown JMSException as we cannot use MessageListener with zero prefetch");
            } 
            catch(NMSException) 
            {
            }
        }

        [Test]
        public void TestPullConsumerWorks() 
        {
            ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
    
            IMessageProducer producer = session.CreateProducer(queue);
            producer.Send(session.CreateTextMessage("Hello World!"));
    
            // now lets Receive it
            IMessageConsumer consumer = session.CreateConsumer(queue);
            IMessage answer = consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.IsNotNull(answer, "Should have received a message!");
            // check if method will return at all and will return a null
            answer = consumer.Receive(TimeSpan.FromMilliseconds(1));
            Assert.IsNull(answer, "Should have not received a message!");
            answer = consumer.ReceiveNoWait();
            Assert.IsNull(answer, "Should have not received a message!");
        }

        [Test]
        public void TestIdleConsumer(
			[Values(AcknowledgementMode.AutoAcknowledge, AcknowledgementMode.Transactional)]
			AcknowledgementMode ackMode) 
        {
            ISession session = connection.CreateSession(ackMode);
    
            IMessageProducer producer = session.CreateProducer(queue);
            producer.Send(session.CreateTextMessage("Msg1"));
            producer.Send(session.CreateTextMessage("Msg2"));
            if(session.Transacted)
            {
                session.Commit();
            }

            // now lets Receive it
            IMessageConsumer consumer = session.CreateConsumer(queue);
            
            session.CreateConsumer(queue);
            ITextMessage answer = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.AreEqual(answer.Text, "Msg1", "Should have received a message!");
            
            if(session.Transacted)
            {
                session.Commit();
            }
            
            // this call would return null if prefetchSize > 0
            answer = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.AreEqual(answer.Text, "Msg2", "Should have received a message!");
            if(session.Transacted)
            {
                session.Commit();
            }
            answer = (ITextMessage)consumer.ReceiveNoWait();
            Assert.IsNull(answer, "Should have not received a message!");
        }
    
        [Test]
        public void TestRecvRecvCommit(
			[Values(AcknowledgementMode.AutoAcknowledge, AcknowledgementMode.Transactional)]
			AcknowledgementMode ackMode)
        {
            ISession session = connection.CreateSession(ackMode);
    
            IMessageProducer producer = session.CreateProducer(queue);
            producer.Send(session.CreateTextMessage("Msg1"));
            producer.Send(session.CreateTextMessage("Msg2"));
            if(session.Transacted)
            {
                session.Commit();
            }
            // now lets Receive it
            IMessageConsumer consumer = session.CreateConsumer(queue);
            ITextMessage answer = (ITextMessage)consumer.ReceiveNoWait();
            Assert.AreEqual(answer.Text, "Msg1", "Should have received a message!");
            answer = (ITextMessage)consumer.ReceiveNoWait();
            Assert.AreEqual(answer.Text, "Msg2", "Should have received a message!");
            if(session.Transacted)
            {
                session.Commit();
            }
            answer = (ITextMessage)consumer.ReceiveNoWait();
            Assert.IsNull(answer, "Should have not received a message!");
        }

        [Test]
        public void TestTwoConsumers() 
        {
            ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
    
            IMessageProducer producer = session.CreateProducer(queue);
            producer.Send(session.CreateTextMessage("Msg1"));
            producer.Send(session.CreateTextMessage("Msg2"));
    
            // now lets Receive it
            IMessageConsumer consumer1 = session.CreateConsumer(queue);
            IMessageConsumer consumer2 = session.CreateConsumer(queue);
            ITextMessage answer = (ITextMessage)consumer1.ReceiveNoWait();
            Assert.AreEqual(answer.Text, "Msg1", "Should have received a message!");
            answer = (ITextMessage)consumer2.ReceiveNoWait();
            Assert.AreEqual(answer.Text, "Msg2", "Should have received a message!");
    
            answer = (ITextMessage)consumer2.ReceiveNoWait();
            Assert.IsNull(answer, "Should have not received a message!");
        }

        [Test]
        public void TestConsumerReceivePrefetchZeroRedeliveryZero()
        {
            const string QUEUE_NAME = "TEST.TestConsumerReceivePrefetchZeroRedeliveryZero";

            using (Connection connection = CreateConnection() as Connection)
            using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
            using (IQueue queue = session.GetQueue(QUEUE_NAME))
            {              
                session.DeleteDestination(queue);

                using (IMessageProducer producer = session.CreateProducer(queue))
                {
                    ITextMessage textMessage = session.CreateTextMessage("test Message");
                    producer.Send(textMessage);
                }
            }

            // consume and rollback - increase redelivery counter on message
            using (Connection connection = CreateConnection() as Connection)
            using (ISession session = connection.CreateSession(AcknowledgementMode.Transactional))
            using (IQueue queue = session.GetQueue(QUEUE_NAME))
            using (IMessageConsumer consumer = session.CreateConsumer(queue))
            {              
                connection.Start();
                IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                Assert.IsNotNull(message);
                session.Rollback();
            }

            // try consume with timeout - expect it to timeout and return NULL message
            using (Connection connection = CreateConnection() as Connection)
            {
                connection.PrefetchPolicy.All = 0;
                connection.RedeliveryPolicy.MaximumRedeliveries = 0;
                connection.Start();

                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IQueue queue = session.GetQueue(QUEUE_NAME);

                using (IMessageConsumer consumer = session.CreateConsumer(queue))
                {
                    IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(3000));
                    Assert.IsNull(message);
                }
            }
        }

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();
    
            connection = CreateConnection();
            connection.Start();
            queue = CreateQueue();            
        }

        [TearDown]
        public override void TearDown()
        {
            connection.Close();
            base.TearDown();
        }
        
        protected IQueue CreateQueue() 
        {
            return new ActiveMQQueue( "ZeroPrefetchConsumerTest?consumer.prefetchSize=0");
        }
        
    }
}
