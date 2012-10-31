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
using System.Transactions;

using NUnit.Framework;
using Apache.NMS.Test;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class NetTxTransactionTest : NMSTestSupport
    {
        private const int MSG_COUNT = 50;

        [Test]
        public void TestTransactedProduceAndConsume(
            [Values("tcp://${activemqhost}:61616")]
            string baseConnectionURI)
        {
            INetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(baseConnectionURI));

            using(INetTxConnection connection = factory.CreateNetTxConnection())
            {
                connection.Start();

                using(INetTxSession session = connection.CreateNetTxSession())
                {
                    IDestination destination = session.CreateTemporaryQueue();
                    using(IMessageProducer producer = session.CreateProducer(destination))
                    {
                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            Assert.IsNotNull(Transaction.Current);
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                producer.Send(session.CreateTextMessage("Hello World"));
                            }

                            scoped.Complete();
                        }
                    }

                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        Thread.Sleep(100);

                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                                Assert.IsNotNull(msg, "Message was null for index: " + i);
                            }
                            scoped.Complete();
                        }
                    }

                    // No more messages should be in the Q, non rolled back or otherwise.
                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        Thread.Sleep(100);
                        IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                        Assert.IsNull(msg, "Message was not null.");
                    }

                    session.Close();
                }

                connection.Close();
            }
        }

        [Test]
        public void TestTransactedProduceRollbackAndConsume(
            [Values("tcp://${activemqhost}:61616")]
            string baseConnectionURI)
        {
            INetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(baseConnectionURI));

            using(INetTxConnection connection = factory.CreateNetTxConnection())
            {
                connection.Start();

                using(INetTxSession session = connection.CreateNetTxSession())
                {
                    IDestination destination = session.CreateTemporaryQueue();
                    using(IMessageProducer producer = session.CreateProducer(destination))
                    {
                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            Assert.IsNotNull(Transaction.Current);
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                producer.Send(session.CreateTextMessage("Hello World"));
                            }
                        }
                    }

                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        Thread.Sleep(100);

                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(50));
                                Assert.IsNull(msg, "Message was not null for index: " + i);
                            }
                            scoped.Complete();
                        }
                    }

                    session.Close();
                }

                connection.Close();
            }
        }

        [Test]
        public void TestTransactedProduceConsumeRollbackConsume(
            [Values("tcp://${activemqhost}:61616")]
            string baseConnectionURI)
        {
            INetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(baseConnectionURI));

            using(INetTxConnection connection = factory.CreateNetTxConnection())
            {
                connection.Start();

                using(INetTxSession session = connection.CreateNetTxSession())
                {
                    IDestination destination = session.CreateTemporaryQueue();
                    using(IMessageProducer producer = session.CreateProducer(destination))
                    {
                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            Assert.IsNotNull(Transaction.Current);
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                producer.Send(session.CreateTextMessage("Hello World"));
                            }
                            scoped.Complete();
                        }
                    }

                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        Thread.Sleep(200);

                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                                Assert.IsNotNull(msg, "Message was null for index: " + i);
                            }
                        }

                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                                Assert.IsNotNull(msg, "Message was null for index: " + i);
                            }
                        }
                    }

                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        Thread.Sleep(200);

                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                                Assert.IsNotNull(msg, "Message was null for index: " + i);
                                Assert.IsTrue(msg.NMSRedelivered);
                            }
                            scoped.Complete();
                        }
                    }

                    session.Close();
                }

                connection.Close();
            }
        }

        [Test]
        public void TestTransactedProduceConsumeWithSessionClose(
            [Values("tcp://${activemqhost}:61616")]
            string baseConnectionURI)
        {
            INetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(baseConnectionURI));

            using(INetTxConnection connection = factory.CreateNetTxConnection())
            {
                connection.Start();

                IDestination destination = null;

                using(INetTxSession session = connection.CreateNetTxSession())
                {
                    session.TransactionStartedListener += TransactionStarted;
                    session.TransactionCommittedListener += TransactionCommitted;
                    session.TransactionRolledBackListener += TransactionRolledBack;

                    destination = session.CreateTemporaryQueue();
                    using(IMessageProducer producer = session.CreateProducer(destination))
                    {
                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            Assert.IsFalse(this.transactionStarted);

                            Assert.IsNotNull(Transaction.Current);
                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                producer.Send(session.CreateTextMessage("Hello World"));
                            }

                            Assert.IsTrue(this.transactionStarted, "A TX should have been started by producing");

                            scoped.Complete();
                        }

                        Assert.IsFalse(this.transactionStarted, "TX Should have Committed and cleared Started");
                        Assert.IsTrue(this.transactionCommitted, "TX Should have Committed");
                        Assert.IsFalse(this.transactionRolledBack, "TX Should not have Rolledback");

                        session.Close();
                    }
                }

                using(INetTxSession session = connection.CreateNetTxSession())
                {
                    session.TransactionStartedListener += TransactionStarted;
                    session.TransactionCommittedListener += TransactionCommitted;
                    session.TransactionRolledBackListener += TransactionRolledBack;

                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        using(TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        {
                            Assert.IsFalse(this.transactionStarted);

                            for(int i = 0; i < MSG_COUNT; ++i)
                            {
                                IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                                Assert.IsNotNull(msg, "Message was null for index: " + i);
                            }

                            Assert.IsTrue(this.transactionStarted, "A TX should have been started by consuming");

                            scoped.Complete();
                        }

                        Assert.IsFalse(this.transactionStarted, "TX Should have Committed and cleared Started");
                        Assert.IsTrue(this.transactionCommitted, "TX Should have Committed");
                        Assert.IsFalse(this.transactionRolledBack, "TX Should not have Rolledback");

                        session.Close();
                    }
                }

                using(INetTxSession session = connection.CreateNetTxSession())
                {
                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        for(int i = 0; i < MSG_COUNT; ++i)
                        {
                            IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(50));
                            Assert.IsNull(msg, "Message was not null for index: " + i);
                        }
                    }

                    session.Close();
                }

                connection.Close();
            }
        }

        private bool transactionStarted = false;
        private bool transactionCommitted = false;
        private bool transactionRolledBack = false;

        private void TransactionStarted(ISession session)
        {
            transactionStarted = true;
            transactionCommitted = false;
            transactionRolledBack = false;
        }

        private void TransactionCommitted(ISession session)
        {
            transactionStarted = false;
            transactionCommitted = true;
            transactionRolledBack = false;
        }

        private void TransactionRolledBack(ISession session)
        {
            transactionStarted = false;
            transactionCommitted = false;
            transactionRolledBack = true;
        }

    }
}

