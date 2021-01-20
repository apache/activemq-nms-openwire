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
using Apache.NMS.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class AMQRedeliveryPolicyTest : NMSTestSupport
    {
        private const string DESTINATION_NAME = "TEST.RedeliveryPolicyTestDest";
        private const string DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = "dlqDeliveryFailureCause";

        [Test]
        public void TestExponentialRedeliveryPolicyDelaysDeliveryOnRollback()
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                IRedeliveryPolicy policy = connection.RedeliveryPolicy;
                policy.InitialRedeliveryDelay = 500;
                policy.BackOffMultiplier = 2;
                policy.UseExponentialBackOff = true;
                policy.UseCollisionAvoidance = false;

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IDestination destination = session.CreateTemporaryQueue();
                IMessageProducer producer = session.CreateProducer(destination);

                IMessageConsumer consumer = session.CreateConsumer(destination);

                // Send the messages
                producer.Send(session.CreateTextMessage("1st"));
                producer.Send(session.CreateTextMessage("2nd"));
                session.Commit();

                ITextMessage m;
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                // No delay on first Rollback..
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(100));
                Assert.IsNotNull(m);
                session.Rollback();

                // Show subsequent re-delivery delay is incrementing.
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(100));
                Assert.IsNull(m);

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(700));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                // Show re-delivery delay is incrementing exponentially
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(100));
                Assert.IsNull(m);
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(500));
                Assert.IsNull(m);
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(700));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
            }
        }

        [Test]
        public void TestNornalRedeliveryPolicyDelaysDeliveryOnRollback()
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                IRedeliveryPolicy policy = connection.RedeliveryPolicy;
                policy.InitialRedeliveryDelay = 500;
                policy.UseExponentialBackOff = false;

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IDestination destination = session.CreateTemporaryQueue();

                IMessageProducer producer = session.CreateProducer(destination);
                IMessageConsumer consumer = session.CreateConsumer(destination);

                // Send the messages
                producer.Send(session.CreateTextMessage("1st"));
                producer.Send(session.CreateTextMessage("2nd"));
                session.Commit();

                ITextMessage m;
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                // No delay on first Rollback..
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(100));
                Assert.IsNotNull(m);
                session.Rollback();

                // Show subsequent re-delivery delay is incrementing.
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(100));
                Assert.IsNull(m);
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(700));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                // The message gets redelivered after 500 ms every time since
                // we are not using exponential backoff.
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(100));
                Assert.IsNull(m);
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(700));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
            }
        }

        [Test]
        public void TestDLQHandling()
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                IRedeliveryPolicy policy = connection.RedeliveryPolicy;
                policy.InitialRedeliveryDelay = 100;
                policy.UseExponentialBackOff = false;
                policy.MaximumRedeliveries = 2;

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IDestination destination = session.CreateTemporaryQueue();
                IMessageProducer producer = session.CreateProducer(destination);

                session.DeleteDestination(new ActiveMQQueue("ActiveMQ.DLQ"));

                IMessageConsumer consumer = session.CreateConsumer(destination);
                IMessageConsumer dlqConsumer = session.CreateConsumer(new ActiveMQQueue("ActiveMQ.DLQ"));

                // Purge any messages already in the DLQ.
                while(dlqConsumer.ReceiveNoWait() != null)
                {
                    session.Commit();
                }

                // Send the messages
                producer.Send(session.CreateTextMessage("1st"));
                producer.Send(session.CreateTextMessage("2nd"));
                session.Commit();

                ITextMessage m;
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                // The last Rollback should cause the 1st message to get sent to the DLQ
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("2nd", m.Text);
                session.Commit();

                // We should be able to get the message off the DLQ now.
                m = (ITextMessage)dlqConsumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Commit();
            }
        }

        [Test]
        public void TestInfiniteMaximumNumberOfRedeliveries()
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                IRedeliveryPolicy policy = connection.RedeliveryPolicy;
                policy.InitialRedeliveryDelay = 100;
                policy.UseExponentialBackOff = false;
                // let's set the maximum redeliveries to no maximum (ie. infinite)
                policy.MaximumRedeliveries = -1;

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IDestination destination = session.CreateTemporaryQueue();
                IMessageProducer producer = session.CreateProducer(destination);

                IMessageConsumer consumer = session.CreateConsumer(destination);

                // Send the messages
                producer.Send(session.CreateTextMessage("1st"));
                producer.Send(session.CreateTextMessage("2nd"));
                session.Commit();

                ITextMessage m;

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                //we should be able to get the 1st message redelivered until a session.Commit is called
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Commit();

                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("2nd", m.Text);
                session.Commit();
            }
        }

        [Test]
        public void TestZeroMaximumNumberOfRedeliveries()
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                IRedeliveryPolicy policy = connection.RedeliveryPolicy;
                policy.InitialRedeliveryDelay = 100;
                policy.UseExponentialBackOff = false;
                //let's set the maximum redeliveries to 0
                policy.MaximumRedeliveries = 0;

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IDestination destination = session.CreateTemporaryQueue();
                IMessageProducer producer = session.CreateProducer(destination);

                IMessageConsumer consumer = session.CreateConsumer(destination);

                // Send the messages
                producer.Send(session.CreateTextMessage("1st"));
                producer.Send(session.CreateTextMessage("2nd"));
                session.Commit();

                ITextMessage m;
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                //the 1st  message should not be redelivered since maximumRedeliveries is set to 0
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("2nd", m.Text);
                session.Commit();
            }
        }

        [Test]
        public void TestURIForRedeliverPolicyHandling()
        {
            string uri1 = "activemq:tcp://${activemqhost}:61616" +
                          "?nms.RedeliveryPolicy.BackOffMultiplier=10" +
                          "&nms.RedeliveryPolicy.InitialRedeliveryDelay=2000" +
                          "&nms.RedeliveryPolicy.UseExponentialBackOff=true" +
                          "&nms.RedeliveryPolicy.UseCollisionAvoidance=true" +
                          "&nms.RedeliveryPolicy.CollisionAvoidancePercent=20";

            string uri2 = "activemq:tcp://${activemqhost}:61616" +
                          "?nms.RedeliveryPolicy.backOffMultiplier=50" +
                          "&nms.RedeliveryPolicy.initialRedeliveryDelay=4000" +
                          "&nms.RedeliveryPolicy.useExponentialBackOff=false" +
                          "&nms.RedeliveryPolicy.useCollisionAvoidance=false" +
                          "&nms.RedeliveryPolicy.collisionAvoidancePercent=10";

            NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri1));

            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using(IConnection connection = factory.CreateConnection("", ""))
            {
                Assert.IsNotNull(connection);

                Connection amqConnection = connection as Connection;

                Assert.AreEqual(10, amqConnection.RedeliveryPolicy.BackOffMultiplier);
                Assert.AreEqual(2000, amqConnection.RedeliveryPolicy.InitialRedeliveryDelay);
                Assert.AreEqual(true, amqConnection.RedeliveryPolicy.UseExponentialBackOff);
                Assert.AreEqual(true, amqConnection.RedeliveryPolicy.UseCollisionAvoidance);
                Assert.AreEqual(20, amqConnection.RedeliveryPolicy.CollisionAvoidancePercent);
            }

            factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri2));

            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using(IConnection connection = factory.CreateConnection("", ""))
            {
                Assert.IsNotNull(connection);

                Connection amqConnection = connection as Connection;
                Assert.AreEqual(50, amqConnection.RedeliveryPolicy.BackOffMultiplier);
                Assert.AreEqual(4000, amqConnection.RedeliveryPolicy.InitialRedeliveryDelay);
                Assert.AreEqual(false, amqConnection.RedeliveryPolicy.UseExponentialBackOff);
                Assert.AreEqual(false, amqConnection.RedeliveryPolicy.UseCollisionAvoidance);
                Assert.AreEqual(10, amqConnection.RedeliveryPolicy.CollisionAvoidancePercent);
            }
        }

        [Test]
        public void TestNornalRedeliveryPolicyOnRollbackUntilTimeToLive()
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                IRedeliveryPolicy policy = connection.RedeliveryPolicy;
                policy.MaximumRedeliveries = -1;
                policy.InitialRedeliveryDelay = 500;
                policy.UseExponentialBackOff = false;

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IDestination destination = session.CreateTemporaryQueue();

                IMessageProducer producer = session.CreateProducer(destination);
                IMessageConsumer consumer = session.CreateConsumer(destination);

                // Send the messages
                ITextMessage textMessage = session.CreateTextMessage("1st");
                textMessage.NMSTimeToLive = TimeSpan.FromMilliseconds(800.0);
                producer.Send(textMessage);
                session.Commit();

                ITextMessage m;
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                // No delay on first Rollback..
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(100));
                Assert.IsNotNull(m);
                session.Rollback();

                // Show subsequent re-delivery delay is incrementing.
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(100));
                Assert.IsNull(m);
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(700));
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();

                // The message gets redelivered after 500 ms every time since
                // we are not using exponential backoff.
                m = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(700));
                Assert.IsNull(m);
            }
        }

        [Test]
        public void TestNornalRedeliveryPolicyOnRollbackUntilTimeToLiveCallback()
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                IRedeliveryPolicy policy = connection.RedeliveryPolicy;
                policy.MaximumRedeliveries = -1;
                policy.InitialRedeliveryDelay = 500;
                policy.UseExponentialBackOff = false;

                connection.Start();
                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IDestination destination = session.CreateTemporaryQueue();

                IMessageProducer producer = session.CreateProducer(destination);
                IMessageConsumer consumer = session.CreateConsumer(destination);
                CallbackClass cc = new CallbackClass(session);
                consumer.Listener += new MessageListener(cc.consumer_Listener);

                // Send the messages
                ITextMessage textMessage = session.CreateTextMessage("1st");
                textMessage.NMSTimeToLive = TimeSpan.FromMilliseconds(800.0);
                producer.Send(textMessage, MsgDeliveryMode.Persistent,MsgPriority.Normal,TimeSpan.FromMilliseconds(800.0));
                session.Commit();

                // sends normal message, then immediate retry, then retry after 500 ms, then expire.
                Thread.Sleep(2000);
                Assert.AreEqual(3, cc.numReceived);
            }
        }

        class CallbackClass
        {
            private ISession session;
            public int numReceived = 0;

            public CallbackClass(ISession session)
            {
                this.session = session;
            }

            public void consumer_Listener(IMessage message)
            {
                numReceived++;
                ITextMessage m = message as ITextMessage;
                Assert.IsNotNull(m);
                Assert.AreEqual("1st", m.Text);
                session.Rollback();
            }
        }

        [Test]
        public void TestRepeatedRedeliveryReceiveNoCommit() 
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                connection.Start();

                ISession dlqSession = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IDestination destination = dlqSession.GetQueue("TestRepeatedRedeliveryReceiveNoCommit");
                IDestination dlq = dlqSession.GetQueue("ActiveMQ.DLQ");
                connection.DeleteDestination(destination);
                connection.DeleteDestination(dlq);
                IMessageProducer producer = dlqSession.CreateProducer(destination);
                producer.Send(dlqSession.CreateTextMessage("1st"));
                IMessageConsumer dlqConsumer = dlqSession.CreateConsumer(dlq);

                const int maxRedeliveries = 4;
                for (int i = 0; i <= maxRedeliveries + 1; i++) 
                {
                    using(Connection loopConnection = (Connection) CreateConnection())
                    {
                        // Receive a message with the JMS API
                        IRedeliveryPolicy policy = loopConnection.RedeliveryPolicy;
                        policy.InitialRedeliveryDelay = 0;
                        policy.UseExponentialBackOff = false;
                        policy.MaximumRedeliveries = maxRedeliveries;

                        loopConnection.Start();
                        ISession session = loopConnection.CreateSession(AcknowledgementMode.Transactional);
                        IMessageConsumer consumer = session.CreateConsumer(destination);

                        ActiveMQTextMessage m = consumer.Receive(TimeSpan.FromMilliseconds(4000)) as ActiveMQTextMessage;
                        if (m != null) 
                        {
                            Tracer.DebugFormat("Received Message: {0} delivery count = {1}", m.Text, m.RedeliveryCounter);
                        }

                        if (i <= maxRedeliveries)
                        {
                            Assert.IsNotNull(m);
                            Assert.AreEqual("1st", m.Text);
                            Assert.AreEqual(i, m.RedeliveryCounter);
                        } 
                        else
                        {
                            Assert.IsNull(m, "null on exceeding redelivery count");
                        }
                    }
                }

                // We should be able to get the message off the DLQ now.
                ITextMessage msg = dlqConsumer.Receive(TimeSpan.FromMilliseconds(2000)) as ITextMessage;
                Assert.IsNotNull(msg, "Got message from DLQ");
                Assert.AreEqual("1st", msg.Text);
                String cause = msg.Properties.GetString(DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY);
                if (cause != null) 
                {
                    Tracer.DebugFormat("Rollback Cause = {0}", cause);
                    Assert.IsTrue(cause.Contains("RedeliveryPolicy"), "cause exception has no policy ref");
                }
                else
                {
                    Tracer.Debug("DLQ'd message has no cause tag.");
                }
            }
        }

        [Test]
        public void TestRepeatedRedeliveryOnMessageNoCommit() 
        {
            using(Connection connection = (Connection) CreateConnection())
            {
                connection.Start();
                ISession dlqSession = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
                IDestination destination = dlqSession.GetQueue("TestRepeatedRedeliveryOnMessageNoCommit");
                IDestination dlq = dlqSession.GetQueue("ActiveMQ.DLQ");
                connection.DeleteDestination(destination);
                connection.DeleteDestination(dlq);
                IMessageProducer producer = dlqSession.CreateProducer(destination);
                IMessageConsumer dlqConsumer = dlqSession.CreateConsumer(dlq);

                producer.Send(dlqSession.CreateTextMessage("1st"));

                const int maxRedeliveries = 4;
                Atomic<int> receivedCount = new Atomic<int>(0);

                for (int i = 0; i <= maxRedeliveries + 1; i++) 
                {
                    using(Connection loopConnection = (Connection) CreateConnection())
                    {
                        IRedeliveryPolicy policy = loopConnection.RedeliveryPolicy;
                        policy.InitialRedeliveryDelay = 0;
                        policy.UseExponentialBackOff = false;
                        policy.MaximumRedeliveries = maxRedeliveries;

                        loopConnection.Start();
                        ISession session = loopConnection.CreateSession(AcknowledgementMode.Transactional);
                        IMessageConsumer consumer = session.CreateConsumer(destination);
                        OnMessageNoCommitCallback callback = new OnMessageNoCommitCallback(receivedCount);
                        consumer.Listener += new MessageListener(callback.consumer_Listener);

                        if (i <= maxRedeliveries) 
                        {
                            Assert.IsTrue(callback.Await(), "listener should have dispatched a message");
                        } 
                        else 
                        {
                            // final redlivery gets poisoned before dispatch
                            Assert.IsFalse(callback.Await(), "listener should not have dispatched after max redliveries");
                        }
                    }
                }

                // We should be able to get the message off the DLQ now.
                ITextMessage msg = dlqConsumer.Receive(TimeSpan.FromMilliseconds(2000)) as ITextMessage;
                Assert.IsNotNull(msg, "Got message from DLQ");
                Assert.AreEqual("1st", msg.Text);
                String cause = msg.Properties.GetString(DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY);
                if (cause != null) 
                {
                    Tracer.DebugFormat("Rollback Cause = {0}", cause);
                    Assert.IsTrue(cause.Contains("RedeliveryPolicy"), "cause exception has no policy ref");
                }
                else
                {
                    Tracer.Debug("DLQ'd message has no cause tag.");
                }
            }
        }

        class OnMessageNoCommitCallback
        {
            private Atomic<int> receivedCount;
            private CountDownLatch done = new CountDownLatch(1);

            public OnMessageNoCommitCallback(Atomic<int> receivedCount)
            {
                this.receivedCount = receivedCount;
            }

            public bool Await() 
            {
                return done.await(TimeSpan.FromMilliseconds(5000));
            }

            public void consumer_Listener(IMessage message)
            {
                ActiveMQTextMessage m = message as ActiveMQTextMessage;
                Tracer.DebugFormat("Received Message: {0} delivery count = {1}", m.Text, m.RedeliveryCounter);
                Assert.AreEqual("1st", m.Text);
                Assert.AreEqual(receivedCount.Value, m.RedeliveryCounter);
                receivedCount.GetAndSet(receivedCount.Value + 1);
                done.countDown();
            }
        }
    }
}
