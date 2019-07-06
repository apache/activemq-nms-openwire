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
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Transactions;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Tcp;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    [Category("Manual")]
    class DtcConsumerTransactionsTest : DtcTransactionsTestSupport
    {
        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            this.dtcFactory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));
            this.dtcFactory.ConfiguredResourceManagerId = Guid.NewGuid().ToString();
        }

        [Test]
        public void TestRedelivered()
        {
            // enqueue several messages
            PurgeDatabase();
            PurgeAndFillQueue();

            // receive just one
            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.Start();

                using (INetTxSession session = connection.CreateNetTxSession())
                {
                    IQueue queue = session.GetQueue(testQueueName);

                    // read message from queue and insert into db table
                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                        using (SqlCommand sqlInsertCommand = new SqlCommand())
                        {
                            sqlConnection.Open();
                            sqlInsertCommand.Connection = sqlConnection;

                            ITextMessage message = consumer.Receive(TimeSpan.FromMilliseconds(10000)) as ITextMessage;
                            sqlInsertCommand.CommandText =
                                string.Format("INSERT INTO {0} VALUES ({1})", testTable, Convert.ToInt32(message.Text));
                            sqlInsertCommand.ExecuteNonQuery();

                            scoped.Complete();
                        }
                    }

                    session.Close();
                }
            }

            // check that others message have status redelivered = false
            IConnectionFactory checkFactory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (IConnection connection = checkFactory.CreateConnection())
            {
                connection.Start();

                using (ISession session = connection.CreateSession())
                using (IQueueBrowser browser = session.CreateBrowser(session.GetQueue(testQueueName)))
                {
                    IEnumerator enumerator = browser.GetEnumerator();

                    while (enumerator.MoveNext())
                    {
                        IMessage msg = enumerator.Current as IMessage;
                        Assert.IsNotNull(msg, "message is not in the queue!");
                        Assert.IsFalse(msg.NMSRedelivered, "message is redelivered!");
                    }
                }
            }
        }

        [Test]
        public void TestRedeliveredCase2()
        {
            const int messageCount = 300;
            const int receiveCount = 150;

            // enqueue several messages
            PurgeDatabase();
            PurgeAndFillQueue(messageCount);

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.Start();

                // receive half of total messages
                for (int i = 0; i < receiveCount; i++)
                {
                    using (INetTxSession session = connection.CreateNetTxSession())
                    {
                        IQueue queue = session.GetQueue(testQueueName);

                        // read message from queue and insert into db table
                        using (IMessageConsumer consumer = session.CreateConsumer(queue))
                        {
                            using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                            using (SqlCommand sqlInsertCommand = new SqlCommand())
                            {
                                sqlConnection.Open();
                                sqlInsertCommand.Connection = sqlConnection;

                                ITextMessage message =
                                    consumer.Receive(TimeSpan.FromMilliseconds(10000)) as ITextMessage;
                                sqlInsertCommand.CommandText =
                                    string.Format("INSERT INTO {0} VALUES ({1})", testTable,
                                                  Convert.ToInt32(message.Text));
                                sqlInsertCommand.ExecuteNonQuery();

                                scoped.Complete();
                            }
                        }

                        session.Close();
                    }

                    Tracer.Debug("Completed for loop iteration #" + i);
                }
            }

            // check that others message have status redelivered = false
            IConnectionFactory checkFactory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (IConnection connection = checkFactory.CreateConnection())
            {
                connection.Start();

                using (ISession session = connection.CreateSession())
                using (IQueueBrowser browser = session.CreateBrowser(session.GetQueue(testQueueName)))
                {
                    IEnumerator enumerator = browser.GetEnumerator();

                    while (enumerator.MoveNext())
                    {
                        IMessage msg = enumerator.Current as IMessage;
                        Assert.IsNotNull(msg, "message is not in the queue!");
                        Assert.IsFalse(msg.NMSRedelivered, "message is redelivered!");
                    }
                }
            }
        }

        [Test]
        public void TestRedeliveredCase3()
        {
            const int messageCount = 300;
            const int receiveCount = 150;

            // enqueue several messages
            PurgeDatabase();
            PurgeAndFillQueue(messageCount);

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.Start();

                // receive half of total messages
                using (INetTxSession session = connection.CreateNetTxSession())
                {
                    IQueue queue = session.GetQueue(testQueueName);

                    // read message from queue and insert into db table
                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        for (int i = 0; i < receiveCount; i++)
                        {
                            using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                            using (SqlCommand sqlInsertCommand = new SqlCommand())
                            {
                                sqlConnection.Open();
                                sqlInsertCommand.Connection = sqlConnection;

                                ITextMessage message =
                                    consumer.Receive(TimeSpan.FromMilliseconds(10000)) as ITextMessage;
                                sqlInsertCommand.CommandText =
                                    string.Format("INSERT INTO {0} VALUES ({1})", testTable,
                                                  Convert.ToInt32(message.Text));
                                sqlInsertCommand.ExecuteNonQuery();
                                scoped.Complete();
                            }
                        }
                    }

                    session.Close();
                }
            }

            Tracer.Debug("First stage ok");

            // check that others message have status redelivered = false
            IConnectionFactory checkFactory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (IConnection connection = checkFactory.CreateConnection())
            {
                connection.Start();

                using (ISession session = connection.CreateSession())
                using (IQueueBrowser browser = session.CreateBrowser(session.GetQueue(testQueueName)))
                {
                    IEnumerator enumerator = browser.GetEnumerator();

                    while (enumerator.MoveNext())
                    {
                        IMessage msg = enumerator.Current as IMessage;
                        Assert.IsNotNull(msg, "message is not in the queue!");
                        Assert.IsFalse(msg.NMSRedelivered, "message is redelivered!");
                    }
                }
            }
        }

        [Test]
        public void TestRedeliveredNoComplete()
        {
            const int messageCount = 300;
            const int receiveCount = 150;

            // enqueue several messages
            PurgeDatabase();
            PurgeAndFillQueue(messageCount);

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                // allow no redelivery so that message immediatly goes to the DLQ if first read fails
                connection.RedeliveryPolicy.MaximumRedeliveries = 0;
                connection.Start();

                // receive half of total messages
                using (INetTxSession session = connection.CreateNetTxSession())
                {
                    IQueue queue = session.GetQueue(testQueueName);

                    // read message from queue and insert into db table
                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        for (int i = 0; i < receiveCount; i++)
                        {
                            using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                            using (SqlCommand sqlInsertCommand = new SqlCommand())
                            {
                                sqlConnection.Open();
                                sqlInsertCommand.Connection = sqlConnection;

                                ITextMessage message =
                                    consumer.Receive(TimeSpan.FromMilliseconds(10000)) as ITextMessage;
                                sqlInsertCommand.CommandText =
                                    string.Format("INSERT INTO {0} VALUES ({1})", testTable,
                                                  Convert.ToInt32(message.Text));
                                sqlInsertCommand.ExecuteNonQuery();
                            }
                        }
                    }

                    session.Close();
                }
            }

            Tracer.Debug("First stage ok");

            // check that others message have status redelivered = false
            IConnectionFactory checkFactory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (IConnection connection = checkFactory.CreateConnection())
            {
                connection.Start();

                using (ISession session = connection.CreateSession())
                using (IQueueBrowser browser = session.CreateBrowser(session.GetQueue(testQueueName)))
                {
                    IEnumerator enumerator = browser.GetEnumerator();
                    while (enumerator.MoveNext())
                    {
                        IMessage msg = enumerator.Current as IMessage;
                        Assert.IsNotNull(msg, "message is not in the queue!");
                        Assert.IsFalse(msg.NMSRedelivered, "message is redelivered!");
                    }
                }
            }
        }

        [Test]
        public void TestRecoveryAfterCommitFailsBeforeSent()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnCommitTransportHook;

                ReadFromQueueAndInsertIntoDbWithCommit(connection);

                Thread.Sleep(1000);
            }

            // transaction should not have been commited
            VerifyNoMessagesInQueueNoRecovery();

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsFull();

            // check messages are not present in the queue
            VerifyNoMessagesInQueue();
        }

        [Test]
        public void TestRecoveryAfterCommitFailsAfterSent()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPostProcessor += this.FailOnCommitTransportHook;

                ReadFromQueueAndInsertIntoDbWithCommit(connection);

                Thread.Sleep(1000);
            }

            // transaction should have been commited
            VerifyNoMessagesInQueueNoRecovery();

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsFull();

            // check messages are not present in the queue
            VerifyNoMessagesInQueue();
        }

        [Test]
        public void TestIterativeTransactedConsume()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue(5 * MSG_COUNT);

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromQueueAndInsertIntoDbWithCommit(connection);
                ReadFromQueueAndInsertIntoDbWithCommit(connection);
                ReadFromQueueAndInsertIntoDbWithCommit(connection);
                ReadFromQueueAndInsertIntoDbWithCommit(connection);
                ReadFromQueueAndInsertIntoDbWithCommit(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsFull(5 * MSG_COUNT);

            // check messages are NOT present in the queue
            VerifyNoMessagesInQueueNoRecovery();
        }

        [Test]
        public void TestConsumeWithDBInsertLogLocation()
        {
            const string logLocation = @".\RecoveryDir";
            string newConnectionUri =
                connectionURI + "?nms.RecoveryPolicy.RecoveryLogger.Location=" + logLocation +
                                "&nms.configuredResourceManagerId=" + 
                                dtcFactory.ConfiguredResourceManagerId;

            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            if (Directory.Exists(logLocation))
            {
                Directory.Delete(logLocation, true);
            }

            Directory.CreateDirectory(logLocation);

            dtcFactory = new NetTxConnectionFactory(ReplaceEnvVar(newConnectionUri));

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnCommitTransportHook;

                ReadFromQueueAndInsertIntoDbWithCommit(connection);

                Thread.Sleep(2000);
            }

            Assert.AreEqual(1, Directory.GetFiles(logLocation).Length);

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsFull();

            // check messages are NOT present in the queue
            NetTxTransactionContext.ResetDtcRecovery();
            VerifyBrokerQueueCount(0, newConnectionUri);

            Assert.AreEqual(0, Directory.GetFiles(logLocation).Length);
        }

        [Test]
        public void TestRecoverAfterTransactionScopeAborted()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromQueueAndInsertIntoDbWithScopeAborted(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has NOT commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are present in the queue
            NetTxTransactionContext.ResetDtcRecovery();
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestRecoverAfterRollbackFailWhenScopeAborted()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnRollbackTransportHook;

                ReadFromQueueAndInsertIntoDbWithScopeAborted(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has NOT commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are recovered and present in the queue 
            NetTxTransactionContext.ResetDtcRecovery();
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestRecoverAfterFailOnTransactionBeforePrepareSent()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnPrepareTransportHook;

                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromQueueAndInsertIntoDbWithCommit(connection);

                Thread.Sleep(2000);
            }

            // Messages are visible since no prepare sent
            VerifyBrokerQueueCountNoRecovery();

            // verify sql server has NOT commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are present in the queue
            NetTxTransactionContext.ResetDtcRecovery();
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestRecoverAfterFailOnTransactionAfterPrepareSent()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPostProcessor += this.FailOnPrepareTransportHook;

                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromQueueAndInsertIntoDbWithCommit(connection);

                Thread.Sleep(2000);
            }

            // not visible yet because it must be rolled back
            VerifyNoMessagesInQueueNoRecovery();

            // verify sql server has NOT commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are present in the queue
            NetTxTransactionContext.ResetDtcRecovery();
            VerifyBrokerQueueCount();
        }

        [Test]
        public void MessageShouldEnlistToTheCorrectTransaction()
        {
            const int messageCount = 100;
            const int receiveCount = 100;

            // enqueue several messages
            PurgeDatabase();
            PurgeAndFillQueue(messageCount);

            var enlistment = new TestSinglePhaseCommit();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.Start();

                // receive half of total messages
                using (INetTxSession session = connection.CreateNetTxSession())
                {
                    IQueue queue = session.GetQueue(testQueueName);
                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        for (int i = 0; i < receiveCount; i++)
                        {
                            try
                            {
                                using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                                {
                                    ITextMessage message = consumer.Receive(TimeSpan.FromMilliseconds(10000)) as ITextMessage;

                                    Transaction.Current.EnlistDurable(Guid.NewGuid(), enlistment, EnlistmentOptions.None);
                                    if (new Random().Next(2) == 0)
                                    {
                                        Tracer.InfoFormat("Throwing random Exception for Message {0}", message.NMSMessageId);
                                        throw new Exception();
                                    }

                                    scoped.Complete();
                                }
                            }
                            catch
                            {
                            }

                            Assert.False(enlistment.singlePhaseCommit, "No single phase commit should happen.");
                        }
                    }
                }
            }
        }

        internal class TestSinglePhaseCommit : ISinglePhaseNotification
        {
            public bool singlePhaseCommit = false;

            public void Prepare(PreparingEnlistment preparingEnlistment)
            {
                preparingEnlistment.Prepared();
            }
            public void Commit(Enlistment enlistment)
            {
                enlistment.Done();
            }

            public void Rollback(Enlistment enlistment)
            {
                enlistment.Done();
            }
            public void InDoubt(Enlistment enlistment)
            {
                enlistment.Done();
            }
            public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
            {
                Tracer.Info("Performing invalid single phase commit.");
                singlePhaseCommit = true;
                singlePhaseEnlistment.Committed();
            }
        }

        #region Asynchronous Consumer Inside of a Transaction Test / Example

        private const int BATCH_COUNT = 5;
        private int batchSequence;
        private DependentTransaction batchTxControl;
        private readonly ManualResetEvent awaitBatchProcessingStart = new ManualResetEvent(false);

        [Test]
        public void TestTransactedAsyncConsumption()
        {
            PurgeDatabase();
            PurgeAndFillQueue(MSG_COUNT * BATCH_COUNT);

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            using (NetTxSession session = connection.CreateNetTxSession() as NetTxSession)
            {
                IQueue queue = session.GetQueue(testQueueName);
                IMessageConsumer consumer = session.CreateConsumer(queue);
                consumer.Listener += AsyncTxAwareOnMessage;

                // Be carefull, message are dispatched once this is done, so you could receive
                // a Message outside a TX.  We use the awaitBatchProcessingStart event here to
                // gate te OnMessage callback, once that method returns the Message is ack'd and
                // no longer has a chance to participate in a TX.
                connection.Start();

                for (int i = 0; i < BATCH_COUNT; ++i)
                {
                    using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                    {
                        session.Enlist(Transaction.Current);

                        batchTxControl = Transaction.Current.DependentClone(DependentCloneOption.BlockCommitUntilComplete);
                        awaitBatchProcessingStart.Set();
                        scoped.Complete();
                    }

                    // Reenlisting to fast seems to annoy the DTC.  Also since DTC operations are
                    // async we need to allow a little time for lag so that the last TX actually 
                    // completes before we start a new one.
                    Thread.Sleep(250);
                }
            }

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsFull(MSG_COUNT * BATCH_COUNT);

            // check messages are NOT present in the queue
            VerifyNoMessagesInQueue();
        }

        private void AsyncTxAwareOnMessage(IMessage message)
        {
            awaitBatchProcessingStart.WaitOne();

            try
            {
                using (TransactionScope scoped = new TransactionScope(batchTxControl))
                using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                using (SqlCommand sqlInsertCommand = new SqlCommand())
                {
                    sqlConnection.Open();
                    sqlInsertCommand.Connection = sqlConnection;

                    ITextMessage textMessage = message as ITextMessage;
                    Assert.IsNotNull(message, "missing message");
                    sqlInsertCommand.CommandText =
                        string.Format("INSERT INTO {0} VALUES ({1})", testTable, Convert.ToInt32(textMessage.Text));
                    sqlInsertCommand.ExecuteNonQuery();
                    scoped.Complete();
                }

                if(++batchSequence == MSG_COUNT)
                {
                    batchSequence = 0;
                    awaitBatchProcessingStart.Reset();
                    batchTxControl.Complete();
                }
            }
            catch (Exception e)
            {
                Tracer.Debug("TX;Error from TransactionScope: " + e.Message);
                Tracer.Debug(e.ToString());
            }
        }

        #endregion
    }
}
