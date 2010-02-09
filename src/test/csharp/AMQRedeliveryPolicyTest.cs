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
using NUnit.Framework;

namespace Apache.NMS.Test
{
    [TestFixture]
    public class AMQRedeliveryPolicyTest : NMSTestSupport
    {
        private const string DESTINATION_NAME = "RedeliveryPolicyTestDest";

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

    }
}
