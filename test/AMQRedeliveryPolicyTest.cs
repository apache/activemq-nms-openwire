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
using Apache.NMS.Policies;
using Apache.NMS.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class AMQRedeliveryPolicyTest : NMSTestSupport
    {
        private const string DLQ_QUEUE_NAME = "ActiveMQ.DLQ";
        private const int MAX_REDELIVERIES = 1;
        private const int RECEIVE_TIMEOUT_SECONDS = 5;
        private const string DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = "dlqDeliveryFailureCause";

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
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

        [Test, Timeout(20_000)]
        public void TestConsumedAckOutcomeDiscardsMessageAfterMaxRedeliveriesInTransactionalSession()
        {
            DrainQueue(DLQ_QUEUE_NAME);

            var connectionFactory = new ConnectionFactory(Factory.BrokerUri)
            {
                RedeliveryPolicy = new CustomRedeliveryPolicy
                {
                    Outcome = (int) AckType.ConsumedAck,
                    MaximumRedeliveries = MAX_REDELIVERIES
                }
            };

            var queueName = Guid.NewGuid().ToString();

            using var connection = connectionFactory.CreateConnection(userName, passWord);
            connection.Start();

            using var session = connection.CreateSession(AcknowledgementMode.Transactional);
            var queue = session.GetQueue(queueName);
            using var producer = session.CreateProducer(queue);
            using var consumer = session.CreateConsumer(queue);

            var message = session.CreateTextMessage("Test Message");
            producer.Send(message);
            session.Commit();

            // First delivery - consume and rollback
            var receivedMessage = consumer.Receive(TimeSpan.FromSeconds(RECEIVE_TIMEOUT_SECONDS));
            Assert.IsNotNull(receivedMessage, "Should have received the message on first delivery");
            session.Rollback();

            // Second delivery (redelivery #1) - consume and rollback, exceeding max redeliveries
            receivedMessage = consumer.Receive(TimeSpan.FromSeconds(RECEIVE_TIMEOUT_SECONDS));
            Assert.IsNotNull(receivedMessage, "Should have received the redelivered message");
            session.Rollback();

            // Verify the message is no longer in the queue (consumed due to ConsumedAck outcome)
            Assert.IsTrue(IsQueueEmpty(queueName), "Queue should be empty after exceeding max redeliveries with ConsumedAck outcome");

            // Verify the message was not sent to DLQ (ConsumedAck discards the message)
            Assert.IsTrue(IsQueueEmpty(DLQ_QUEUE_NAME), "DLQ should be empty when using ConsumedAck outcome");
        }

        [Test, Timeout(20_000)]
        public void TestConsumedAckOutcomeRejectsMessageOnReceiveAfterRedeliveryLimitExceededInPreviousSessions()
        {
            DrainQueue(DLQ_QUEUE_NAME);

            var connectionFactory = new ConnectionFactory(Factory.BrokerUri)
            {
                RedeliveryPolicy = new CustomRedeliveryPolicy
                {
                    Outcome = (int) AckType.ConsumedAck,
                    MaximumRedeliveries = MAX_REDELIVERIES
                }
            };
            var queueName = Guid.NewGuid().ToString();

            // Send initial message
            SendMessage(connectionFactory, queueName, "Test Message");
            
            ExceedRedeliveryLimitInNonTransactionalSessions(connectionFactory, queueName, MAX_REDELIVERIES + 1);

            // Verify the message is still in the queue
            Assert.IsFalse(IsQueueEmpty(queueName), "Message should still be in queue after non-transactional redeliveries");

            // Attempt to consume in a transactional session - message should be rejected on arrival
            using (var connection = connectionFactory.CreateConnection(userName, passWord))
            {
                connection.Start();
                using var session = connection.CreateSession(AcknowledgementMode.Transactional);
                var destination = session.GetQueue(queueName);
                using var consumer = session.CreateConsumer(destination);

                var receivedMessage = consumer.Receive(TimeSpan.FromMilliseconds(50));
                Assert.IsNull(receivedMessage, "Message should be rejected on arrival when redelivery limit already exceeded");
            }

            // Verify the message was discarded (not in original queue or DLQ)
            Assert.IsTrue(IsQueueEmpty(queueName), "Queue should be empty after message rejection");
            Assert.IsTrue(IsQueueEmpty(DLQ_QUEUE_NAME), "DLQ should be empty when using ConsumedAck outcome");
        }

        [Test, Timeout(20_000)]
        public void TestConsumedAckOutcomeRejectsMessageOnListenerDeliveryAfterRedeliveryLimitExceededInPreviousSessions()
        {
            DrainQueue(DLQ_QUEUE_NAME);

            var connectionFactory = new ConnectionFactory(Factory.BrokerUri)
            {
                RedeliveryPolicy = new CustomRedeliveryPolicy
                {
                    Outcome = (int) AckType.ConsumedAck,
                    MaximumRedeliveries = MAX_REDELIVERIES
                }
            };
            var queueName = Guid.NewGuid().ToString();

            // Send initial message
            SendMessage(connectionFactory, queueName, "Test Message");
            
            ExceedRedeliveryLimitInNonTransactionalSessions(connectionFactory, queueName, MAX_REDELIVERIES + 1);

            // Verify the message is still in the queue
            Assert.IsFalse(IsQueueEmpty(queueName), "Message should still be in queue after non-transactional redeliveries");

            // Attempt to consume with a listener in a transactional session - message should be rejected on arrival
            using (var connection = connectionFactory.CreateConnection(userName, passWord))
            {
                var messageReceived = false;

                using var session = connection.CreateSession(AcknowledgementMode.Transactional);
                var destination = session.GetQueue(queueName);
                using var consumer = session.CreateConsumer(destination);

                consumer.Listener += _ => { messageReceived = true; };

                connection.Start();

                // Wait for the queue to be empty (message rejected) or timeout
                SpinWait.SpinUntil(() => IsQueueEmpty(queueName), TimeSpan.FromSeconds(RECEIVE_TIMEOUT_SECONDS));

                Assert.IsFalse(messageReceived, "Message listener should not have been invoked when message is rejected on arrival");
            }

            // Verify the message was discarded (not in DLQ)
            Assert.IsTrue(IsQueueEmpty(DLQ_QUEUE_NAME), "DLQ should be empty when using ConsumedAck outcome");
        }

        private void SendMessage(ConnectionFactory connectionFactory, string queueName, string messageText)
        {
            using var connection = connectionFactory.CreateConnection(userName, passWord);
            using var session = connection.CreateSession();
            var destination = session.GetQueue(queueName);
            using var producer = session.CreateProducer(destination);
            var message = session.CreateTextMessage(messageText);
            producer.Send(message);
        }

        /// <summary>
        /// Consume the message multiple times without acknowledgment using ClientAcknowledge mode
        /// This increments the redelivery counter but won't trigger RedeliveryExceeded handling
        /// since that only happens in transactional sessions
        /// </summary>
        private void ExceedRedeliveryLimitInNonTransactionalSessions(ConnectionFactory connectionFactory, string queueName, int attemptCount)
        {
            for (int i = 0; i < attemptCount; i++)
            {
                using var connection = connectionFactory.CreateConnection(userName, passWord);
                connection.Start();
                using var session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
                var destination = session.GetQueue(queueName);
                using var consumer = session.CreateConsumer(destination);

                var receivedMessage = consumer.Receive(TimeSpan.FromSeconds(RECEIVE_TIMEOUT_SECONDS));
                Assert.IsNotNull(receivedMessage, $"Should have received the message on attempt {i + 1}");
                // Deliberately not acknowledging to trigger redelivery
            }
        }

        private bool IsQueueEmpty(string queueName)
        {
            var connectionFactory = (ConnectionFactory) Factory;
            using var connection = connectionFactory.CreateConnection(userName, passWord);
            connection.Start();
            using var session = connection.CreateSession(AcknowledgementMode.ClientAcknowledge);
            var queue = session.GetQueue(queueName);
            using var browser = session.CreateBrowser(queue);

            var enumerator = browser.GetEnumerator();
            using var _ = enumerator as IDisposable;
            return !enumerator.MoveNext();
        }

        private void DrainQueue(string queueName)
        {
            if (IsQueueEmpty(queueName))
            {
                return;
            }

            var connectionFactory = (ConnectionFactory) Factory;
            using var connection = connectionFactory.CreateConnection(userName, passWord);
            connection.Start();
            using var session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            var queue = session.GetQueue(queueName);
            using var consumer = session.CreateConsumer(queue);

            while (true)
            {
                var message = consumer.ReceiveNoWait();
                if (message == null)
                {
                    break;
                }
            }
        }

        private class CustomRedeliveryPolicy : RedeliveryPolicy
        {
            public int Outcome { get; set; }

            public override int GetOutcome(IDestination destination)
            {
                return Outcome;
            }
        }
    }
}
