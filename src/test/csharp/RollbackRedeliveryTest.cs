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
using System.Threading;
using Apache.NMS.Test;
using Apache.NMS.Policies;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class RollbackRedeliveryTest : NMSTestSupport
    {
        protected static string DESTINATION_NAME = "TEST.RollbackDelivery";
        protected static string TEST_CLIENT_ID = "RollbackRedeliveryTestId";

        private const int nbMessages = 10;

        private IConnection connection;

        public RollbackRedeliveryTest()
        {
        }

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            connection = CreateConnection();
            connection.Start();

            Session session = connection.CreateSession() as Session;
            IQueue queue = session.GetQueue(DESTINATION_NAME);
            session.DeleteDestination(queue);
        }

        [TearDown]
        public override void TearDown()
        {
            connection.Close();
            base.TearDown();
        }

        [Test]
        public void TestRedelivery()
        {
            // Use non-interleaved producer and default prefetch.
            DoTestRedelivery((connection as Connection).PrefetchPolicy.QueuePrefetch, false);
        }

//        [Test]
//        public void TestRedeliveryWithInterleavedProducer()
//        {
//            DoTestRedelivery((connection as Connection).PrefetchPolicy.QueuePrefetch, true);
//        }
//
//        [Test]
//        public void TestRedeliveryWithPrefetch0()
//        {
//            DoTestRedelivery(0, true);
//        }
//
//        [Test]
//        public void TestRedeliveryWithPrefetch1()
//        {
//            DoTestRedelivery(1, true);
//        }

        public void DoTestRedelivery(int queuePrefetch, bool interleaveProducer)
        {
            (connection as Connection).PrefetchPolicy.QueuePrefetch = queuePrefetch;

            connection.Start();

            if(interleaveProducer)
            {
                PopulateDestinationWithInterleavedProducer(nbMessages, DESTINATION_NAME);
            }
            else
            {
                PopulateDestination(nbMessages, DESTINATION_NAME);
            }

            // Consume messages and Rollback transactions
            {
                int received = 0;
                IDictionary rolledback = Hashtable.Synchronized(new Hashtable());

                while(received < nbMessages)
                {
                    ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                    IDestination destination = session.GetQueue(DESTINATION_NAME);
                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    ITextMessage msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(6000000));

                    if(msg != null)
                    {
                        if(msg != null && rolledback.Contains(msg.Text))
                        {
                            Interlocked.Increment(ref received);
                            Assert.IsTrue(msg.NMSRedelivered);
                            Assert.AreEqual(2, msg.Properties.GetLong("NMSXDeliveryCount"));
                            session.Commit();
                        }
                        else
                        {
                            Assert.IsFalse(msg.NMSRedelivered);
                            rolledback.Add(msg.Text, (Boolean) true);
                            session.Rollback();
                        }
                    }
                    consumer.Close();
                    session.Close();
                }
            }
        }

        [Test]
	    public void TestRedeliveryOnSingleConsumer()
        {
	        connection.Start();

	        PopulateDestinationWithInterleavedProducer(nbMessages, DESTINATION_NAME);

	        // Consume messages and Rollback transactions
	        {
	            int received = 0;
                IDictionary rolledback = Hashtable.Synchronized(new Hashtable());
	            ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
	            IDestination destination = session.GetQueue(DESTINATION_NAME);
	            IMessageConsumer consumer = session.CreateConsumer(destination);

                while(received < nbMessages)
                {
	                ITextMessage msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(6000000));
	                if(msg != null)
                    {
                        if(msg != null && rolledback.Contains(msg.Text))
                        {
                            Interlocked.Increment(ref received);
	                        Assert.IsTrue(msg.NMSRedelivered);
	                        session.Commit();
	                    }
                        else
                        {
                            rolledback.Add(msg.Text, (Boolean) true);
	                        session.Rollback();
	                    }
	                }
	            }
	            consumer.Close();
	            session.Close();
	        }
	    }

        [Test]
        public void TestRedeliveryOnSingleSession()
        {
	        connection.Start();

	        PopulateDestination(nbMessages, DESTINATION_NAME);

	        // Consume messages and Rollback transactions
	        {
                int received = 0;
                IDictionary rolledback = Hashtable.Synchronized(new Hashtable());
	            ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
	            IDestination destination = session.GetQueue(DESTINATION_NAME);

                while(received < nbMessages)
                {
	                IMessageConsumer consumer = session.CreateConsumer(destination);
	                ITextMessage msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(6000000));
	                if(msg != null)
                    {
                        if(msg != null && rolledback.Contains(msg.Text))
                        {
                            Interlocked.Increment(ref received);
	                        Assert.IsTrue(msg.NMSRedelivered);
	                        session.Commit();
	                    }
                        else
                        {
                            rolledback.Add(msg.Text, (Boolean) true);
	                        session.Rollback();
	                    }
	                }
	                consumer.Close();
	            }
	            session.Close();
	        }
	    }

        [Test]
        public void TestMessageRedelivedMaxRedeliveriesTimesSingleSession()
        {
            connection.RedeliveryPolicy.MaximumRedeliveries = 15;
            connection.RedeliveryPolicy.UseCollisionAvoidance = false;
            connection.RedeliveryPolicy.UseExponentialBackOff = false;

            connection.Start();

            PopulateDestination(1, DESTINATION_NAME);

            // Consume messages and Rollback transactions
            {
                int received = 0;
                ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                IDestination destination = session.GetQueue(DESTINATION_NAME);

                IMessageConsumer consumer = session.CreateConsumer(destination);
                ITextMessage msg = null;

                while(received <= connection.RedeliveryPolicy.MaximumRedeliveries)
                {
                    msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(6000000));
                    Assert.IsNotNull(msg);

                    if(received > 0)
                    {
                        Assert.IsTrue(msg.NMSRedelivered);
                        Assert.AreEqual(received, msg.Properties.GetLong("NMSXDeliveryCount") - 1);
                    }

                    Interlocked.Increment(ref received);

                    session.Rollback();
                }

                msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(6000));
                Assert.IsNull(msg);

                session.Close();
            }
        }

        [Test]
        public void TestMessageRedelivedMaxRedeliveriesTimesMultipleSessions()
        {
            connection.RedeliveryPolicy.MaximumRedeliveries = 15;
            connection.RedeliveryPolicy.UseCollisionAvoidance = false;
            connection.RedeliveryPolicy.UseExponentialBackOff = false;

            connection.Start();

            PopulateDestination(1, DESTINATION_NAME);

            // Consume messages and Rollback transactions
            {
                int received = 0;

                ISession session = null;
                IDestination destination = null;
                IMessageConsumer consumer = null;
                ITextMessage msg = null;

                while(received <= connection.RedeliveryPolicy.MaximumRedeliveries)
                {
                    session = connection.CreateSession(AcknowledgementMode.Transactional);
                    destination = session.GetQueue(DESTINATION_NAME);
                    consumer = session.CreateConsumer(destination);
                    msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(6000000));
                    Assert.IsNotNull(msg);

                    if(received > 0)
                    {
                        Assert.IsTrue(msg.NMSRedelivered);
                        Assert.AreEqual(received, msg.Properties.GetLong("NMSXDeliveryCount") - 1);
                    }

                    Interlocked.Increment(ref received);

                    session.Rollback();
                    session.Close();
                }

                session = connection.CreateSession(AcknowledgementMode.Transactional);
                destination = session.GetQueue(DESTINATION_NAME);
                consumer = session.CreateConsumer(destination);
                msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(6000));
                Assert.IsNull(msg);
            }
        }

        [Test]
        public void TestValidateRedeliveryCountOnRollback()
        {
            const int numMessages = 1;
            connection.Start();

            PopulateDestination(numMessages, DESTINATION_NAME);

            {
                int received = 0;
                int maxRetries = new RedeliveryPolicy().MaximumRedeliveries;

                while(received < maxRetries)
                {
                    ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                    IDestination destination = session.GetQueue(DESTINATION_NAME);

                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    ITextMessage msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(2000));

                    if(msg != null)
                    {
                        Interlocked.Increment(ref received);
                        Assert.AreEqual(received, msg.Properties.GetLong("NMSXDeliveryCount"),
                                        "redelivery property matches deliveries");
                        session.Rollback();
                    }
                    session.Close();
                }

                ConsumeMessage(maxRetries + 1);
            }
        }

        [Test]
        public void TestValidateRedeliveryCountOnRollbackWithPrefetch0()
        {
            const int numMessages = 1;
            (connection as Connection).PrefetchPolicy.SetAll(0);
            connection.Start();

            PopulateDestination(numMessages, DESTINATION_NAME);

            {
                int received = 0;
                int maxRetries = new RedeliveryPolicy().MaximumRedeliveries;

                while(received < maxRetries)
                {
                    ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                    IDestination destination = session.GetQueue(DESTINATION_NAME);

                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    ITextMessage msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(2000));

                    if(msg != null)
                    {
                        Interlocked.Increment(ref received);
                        Assert.AreEqual(received, msg.Properties.GetLong("NMSXDeliveryCount"),
                                        "redelivery property matches deliveries");
                        session.Rollback();
                    }
                    session.Close();
                }

                ConsumeMessage(maxRetries + 1);
            }
        }

        private void ConsumeMessage(int deliveryCount)
        {
            ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
            IDestination destination = session.GetQueue(DESTINATION_NAME);
            IMessageConsumer consumer = session.CreateConsumer(destination);
            ITextMessage msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(msg);
            Assert.AreEqual(deliveryCount, msg.Properties.GetLong("NMSXDeliveryCount"),
                            "redelivery property matches deliveries");
            session.Commit();
            session.Close();
        }

        [Test]
        public void TestRedeliveryPropertyWithNoRollback()
        {
            const int numMessages = 1;

            PopulateDestination(numMessages, DESTINATION_NAME);
            connection.Close();

            {
                int received = 0;

                int maxRetries = new RedeliveryPolicy().MaximumRedeliveries;

                while(received < maxRetries)
                {
                    connection = CreateConnection();
                    connection.Start();
                    ISession session = connection.CreateSession(AcknowledgementMode.Transactional);
                    IDestination destination = session.GetQueue(DESTINATION_NAME);

                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    ITextMessage msg = (ITextMessage) consumer.Receive(TimeSpan.FromMilliseconds(2000));

                    if(msg != null)
                    {
                        Interlocked.Increment(ref received);
                        Assert.AreEqual(received, msg.Properties.GetLong("NMSXDeliveryCount"),
                                        "redelivery property matches deliveries");
                    }

                    session.Close();
                    connection.Close();
                }

                connection = CreateConnection();
                connection.Start();
                ConsumeMessage(maxRetries + 1);
            }
        }

        private void PopulateDestination(int nbMessages, string destinationName)
        {
            ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IDestination destination = session.GetQueue(destinationName);
            IMessageProducer producer = session.CreateProducer(destination);

            for(int i = 1; i <= nbMessages; i++)
            {
                producer.Send(session.CreateTextMessage("<hello id='" + i + "'/>"));
            }

            producer.Close();
            session.Close();
        }

        private void PopulateDestinationWithInterleavedProducer(int nbMessages, string destinationName)
        {
            ISession session1 = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IDestination destination1 = session1.GetQueue(destinationName);
            IMessageProducer producer1 = session1.CreateProducer(destination1);
            producer1.DeliveryMode = MsgDeliveryMode.NonPersistent;

            ISession session2 = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            IDestination destination2 = session2.GetQueue(destinationName);
            IMessageProducer producer2 = session2.CreateProducer(destination2);
            producer2.DeliveryMode = MsgDeliveryMode.NonPersistent;

            for(int i = 1; i <= nbMessages; i++)
            {
                if(i%2 == 0)
                {
                    producer1.Send(session1.CreateTextMessage("<hello id='" + i + "'/>"));
                }
                else
                {
                    producer2.Send(session2.CreateTextMessage("<hello id='" + i + "'/>"));
                }
            }

            producer1.Close();
            session1.Close();
            producer2.Close();
            session2.Close();
        }

    }
}
