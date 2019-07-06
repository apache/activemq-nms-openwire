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
using System.IO;
using System.Threading;
using Apache.NMS.ActiveMQ.Transactions;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Tcp;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    [Category("Manual")]
    class DtcProducerTransactionsTest : DtcTransactionsTestSupport
    {
        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            this.dtcFactory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));
            this.dtcFactory.ConfiguredResourceManagerId = Guid.NewGuid().ToString();
        }

        [Test]
        public void TestRecoverAfterFailOnTransactionCommit()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnCommitTransportHook;

                ReadFromDbAndProduceToQueueWithCommit(connection);

                Thread.Sleep(1000);
            }

            // transaction should not have been commited
            VerifyNoMessagesInQueueNoRecovery();

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are present in the queue
            NetTxTransactionContext.ResetDtcRecovery();
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestRecoverAfterFailOnTransactionPostCommitSend()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPostProcessor += this.FailOnCommitTransportHook;

                ReadFromDbAndProduceToQueueWithCommit(connection);

                Thread.Sleep(1000);
            }

            // transaction should have been commited
            VerifyBrokerQueueCountNoRecovery();

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are present in the queue
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestNoRecoverAfterFailOnTransactionWhenLogDeleted()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            NetTxConnectionFactory netTxFactory = dtcFactory;
            RecoveryFileLogger logger = netTxFactory.RecoveryPolicy.RecoveryLogger as RecoveryFileLogger;
            string logDirectory = logger.Location;

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnCommitTransportHook;

                ReadFromDbAndProduceToQueueWithCommit(connection);

                Thread.Sleep(2000);
            }

            // transaction should not have been commited
            VerifyNoMessagesInQueueNoRecovery();

            // delete all recovery files            
            foreach (string file in Directory.GetFiles(logDirectory, "*.bin"))
            {
                File.Delete(file);
            }

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are NOT present in the queue bacause recovery file has been deleted
            VerifyNoMessagesInQueue();
        }

        [Test]
        public void TestNoRecoverAfterFailOnTransactionWhenLogWriteFails()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            string newConnectionUri = 
                connectionURI + "?nms.RecoveryPolicy.RecoveryLoggerType=harness" +
                                "&nms.configuredResourceManagerId=" +
                                dtcFactory.ConfiguredResourceManagerId;

            dtcFactory = new NetTxConnectionFactory(ReplaceEnvVar(newConnectionUri));

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                IRecoveryLogger logger = (connection as NetTxConnection).RecoveryPolicy.RecoveryLogger;
                Assert.IsNotNull(logger);
                RecoveryLoggerHarness harness = logger as RecoveryLoggerHarness;
                Assert.IsNotNull(harness);

                harness.PreLogRecoveryInfoEvent += FailOnPreLogRecoveryHook;

                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromDbAndProduceToQueueWithCommit(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has not commited the transaction                    
            VerifyDatabaseTableIsFull();

            // check messages are not present in the queue
            VerifyNoMessagesInQueue();
        }

        [Test]
        public void TestRecoverAfterFailOnTransactionBeforePrepareSent()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnPrepareTransportHook;

                ReadFromDbAndProduceToQueueWithCommit(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has not commited the transaction                    
            VerifyDatabaseTableIsFull();

            // check messages are not present in the queue
            VerifyNoMessagesInQueue();
        }

        [Test]
        public void TestRecoverAfterFailOnTransactionDuringPrepareSend()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPostProcessor += this.FailOnPrepareTransportHook;

                ReadFromDbAndProduceToQueueWithCommit(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsFull();

            // check messages are present in the queue
            VerifyNoMessagesInQueue();
        }

        [Test]
        public void TestRecoverAfterTransactionScopeAborted()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromDbAndProduceToQueueWithScopeAborted(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has NOT commited the transaction                    
            VerifyDatabaseTableIsFull();

            // check messages are NOT present in the queue
            VerifyNoMessagesInQueue();
        }

        [Test]
        public void TestRecoverAfterRollbackFailWhenScopeAborted()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ITransport transport = (connection as Connection).ITransport;
                TcpFaultyTransport tcpFaulty = transport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                Assert.IsNotNull(tcpFaulty);
                tcpFaulty.OnewayCommandPreProcessor += this.FailOnRollbackTransportHook;

                ReadFromDbAndProduceToQueueWithScopeAborted(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsFull();

            // before recovering, messages should NOT be present in the queue
            VerifyNoMessagesInQueueNoRecovery();

            // check messages are not present in the queue after recover
            VerifyNoMessagesInQueue();
        }

        [Test]
        public void TestIterativeTransactedProduceWithDBDelete()
        {
            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                PrepareDatabase();
                ReadFromDbAndProduceToQueueWithCommit(connection);

                PrepareDatabase();
                ReadFromDbAndProduceToQueueWithCommit(connection);

                PrepareDatabase();
                ReadFromDbAndProduceToQueueWithCommit(connection);

                PrepareDatabase();
                ReadFromDbAndProduceToQueueWithCommit(connection);

                PrepareDatabase();
                ReadFromDbAndProduceToQueueWithCommit(connection);
            }

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are present in the queue
            VerifyBrokerQueueCount(MSG_COUNT * 5);
        }
    }
}
