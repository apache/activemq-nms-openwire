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
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Failover;
using Apache.NMS.ActiveMQ.Transport.Tcp;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class FailoverTransactionTest : NMSTestSupport
    {
        private Connection connection;
        private bool interrupted = false;
        private bool resumed = false;
        private bool commitFailed = false;

        private readonly int MSG_COUNT = 2;
        private readonly String destinationName = "FailoverTransactionTestQ";

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            this.connection = null;
            this.interrupted = false;
            this.resumed = false;
            this.commitFailed = false;
        }

        [Test]
        public void FailoverAfterCommitSentTest()
        {
            string uri = "failover:(tcpfaulty://${activemqhost}:61616?transport.useLogging=true)";
            IConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri));
            using(connection = factory.CreateConnection() as Connection)
            {
                connection.ConnectionInterruptedListener +=
                    new ConnectionInterruptedListener(TransportInterrupted);
                connection.ConnectionResumedListener +=
                    new ConnectionResumedListener(TransportResumed);

                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPostProcessor += this.FailOnCommitTransportHook;

                using(ISession session = connection.CreateSession())
                {
                    IDestination destination = session.GetQueue(destinationName);
                    PurgeQueue(connection, destination);
                }

                Tracer.Debug("Test is putting " + MSG_COUNT + " messages on the queue: " + destinationName);

                using(ISession session = connection.CreateSession(AcknowledgementMode.Transactional))
                {
                    IDestination destination = session.GetQueue(destinationName);
                    PutMsgIntoQueue(session, destination, false);

                    try
                    {
                        session.Commit();
                        Assert.Fail("Should have thrown a TransactionRolledBackException");
                    }
                    catch(TransactionRolledBackException)
                    {
                    }
                    catch
                    {
                        Assert.Fail("Should have thrown a TransactionRolledBackException");
                    }
                }

                Assert.IsTrue(this.interrupted);
                Assert.IsTrue(this.resumed);

                Tracer.Debug("Test is attempting to read " + MSG_COUNT +
                             " messages from the queue: " + destinationName);

                using(ISession session = connection.CreateSession())
                {
                    IDestination destination = session.GetQueue(destinationName);
                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    for (int i = 0; i < MSG_COUNT; ++i)
                    {
                        IMessage msg = consumer.Receive(TimeSpan.FromSeconds(5));
                        Assert.IsNotNull(msg, "Should receive message[" + (i + 1) + "] after commit failed once.");
                    }
                }
            }

            Assert.IsTrue(this.interrupted);
            Assert.IsTrue(this.resumed);
        }

        [Test]
        public void FailoverBeforeCommitSentTest()
        {
            string uri = "failover:(tcpfaulty://${activemqhost}:61616?transport.useLogging=true)";
            IConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri));
            using(connection = factory.CreateConnection() as Connection)
            {
                connection.ConnectionInterruptedListener +=
                    new ConnectionInterruptedListener(TransportInterrupted);
                connection.ConnectionResumedListener +=
                    new ConnectionResumedListener(TransportResumed);

                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnCommitTransportHook;

                using(ISession session = connection.CreateSession())
                {
                    IDestination destination = session.GetQueue(destinationName);
                    PurgeQueue(connection, destination);
                }

                Tracer.Debug("Test is putting " + MSG_COUNT + " messages on the queue: " + destinationName);

                using(ISession session = connection.CreateSession(AcknowledgementMode.Transactional))
                {
                    IDestination destination = session.GetQueue(destinationName);
                    PutMsgIntoQueue(session, destination, false);

                    try
                    {
                        session.Commit();
                        Assert.Fail("Should have thrown a TransactionRolledBackException");
                    }
                    catch(TransactionRolledBackException)
                    {
                    }
                    catch
                    {
                        Assert.Fail("Should have thrown a TransactionRolledBackException");
                    }
                }

                Assert.IsTrue(this.interrupted);
                Assert.IsTrue(this.resumed);

                Tracer.Debug("Test is attempting to read a message from" +
                             destinationName + " but no messages are expected");

                using(ISession session = connection.CreateSession())
                {
                    IDestination destination = session.GetQueue(destinationName);
                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    IMessage msg = consumer.Receive(TimeSpan.FromSeconds(5));
                    Assert.IsNull(msg, "Should not receive a message after commit failed.");
                }
            }

            Assert.IsTrue(this.interrupted);
            Assert.IsTrue(this.resumed);
        }

        [Test]
        public void FailoverWithShortLivedProducerTest()
        {
            string uri = "failover:(tcpfaulty://${activemqhost}:61616?transport.useLogging=true)";
            IConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri));
            using(connection = factory.CreateConnection() as Connection)
            {
                connection.ConnectionInterruptedListener +=
                    new ConnectionInterruptedListener(TransportInterrupted);
                connection.ConnectionResumedListener +=
                    new ConnectionResumedListener(TransportResumed);

                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);

                using(ISession session = connection.CreateSession())
                {
                    IDestination destination = session.GetQueue(destinationName);
                    PurgeQueue(connection, destination);
                }

                Tracer.Debug("Test is putting " + MSG_COUNT + " messages on the queue: " + destinationName);

                using(ISession session = connection.CreateSession(AcknowledgementMode.Transactional))
                {
                    IDestination destination = session.GetQueue(destinationName);
                    PutMsgIntoQueue(session, destination, false);
                    tcpFaulty.Close();
                    PutMsgIntoQueue(session, destination, false);
                    session.Commit();
                }

                Assert.IsTrue(this.interrupted);
                Assert.IsTrue(this.resumed);

                Tracer.Debug("Test is attempting to read " + MSG_COUNT +
                             " messages from the queue: " + destinationName);

                using(ISession session = connection.CreateSession())
                {
                    IDestination destination = session.GetQueue(destinationName);
                    IMessageConsumer consumer = session.CreateConsumer(destination);
                    for (int i = 0; i < MSG_COUNT; ++i)
                    {
                        IMessage msg = consumer.Receive(TimeSpan.FromSeconds(5));
                        Assert.IsNotNull(msg, "Should receive message[" + (i + 1) + "] after commit failed once.");
                    }
                }
            }

            Assert.IsTrue(this.interrupted);
            Assert.IsTrue(this.resumed);
        }

        public void TransportInterrupted()
        {
            this.interrupted = true;
        }

        public void TransportResumed()
        {
            this.resumed = true;
        }

        private void PutMsgIntoQueue(ISession session, IDestination destination)
        {
            PutMsgIntoQueue(session, destination, true);
        }

        private void PutMsgIntoQueue(ISession session, IDestination destination, bool commit)
        {
            using(IMessageProducer producer = session.CreateProducer(destination))
            {
                ITextMessage message = session.CreateTextMessage();
                for(int i = 0; i < MSG_COUNT; ++i)
                {
                    message.Text = "Test message " + (i + 1);
                    producer.Send(message);
                }

                if (session.Transacted && commit)
                {
                    session.Commit();
                }
            }
        }

        public void PurgeQueue(IConnection conn, IDestination queue)
        {
            using(ISession session = conn.CreateSession())
            {
                using(IMessageConsumer consumer = session.CreateConsumer(queue))
                while(consumer.Receive(TimeSpan.FromMilliseconds(500)) != null)
                {
                }
            }
        }

        private void BreakConnection()
        {
            TcpTransport transport = this.connection.ITransport.Narrow(typeof(TcpTransport)) as TcpTransport;
            Assert.IsNotNull(transport);
            transport.Close();
        }

        public void FailOnCommitTransportHook(ITransport transport, Command command)
        {
            if (commitFailed)
            {
                return;
            }

            if (command is TransactionInfo)
            {
                TransactionInfo txInfo = command as TransactionInfo;
                if (txInfo.Type == (byte)TransactionType.CommitOnePhase)
                {
                    Tracer.Debug("Closing the TcpTransport to simulate an connection drop.");
                    commitFailed = true;
                    (transport as TcpTransport).Close();
                }
            }
        }

    }
}

