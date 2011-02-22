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

using System.IO;
using System.Threading;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Tcp;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test.src.test.csharp
{
    [TestFixture]
    [Category("Manual")]
    class DtcConsumerTransactionsTest : DtcTransactionsTestSupport
    {
        [Test]
        public void TestRecoveryAfterCommitFailsBeforeSent()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
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

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
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

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
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
            const string newConnectionUri =
                connectionURI + "?nms.RecoveryPolicy.RecoveryLogger.Location=" + logLocation;

            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            if (Directory.Exists(logLocation))
            {
                Directory.Delete(logLocation, true);
            }

            Directory.CreateDirectory(logLocation);

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(newConnectionUri));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
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
            VerifyBrokerQueueCount(0, newConnectionUri);

            Assert.AreEqual(0, Directory.GetFiles(logLocation).Length);
        }

        [Test]
        public void TestRecoverAfterTransactionScopeAborted()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromQueueAndInsertIntoDbWithScopeAborted(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has NOT commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are present in the queue
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestRecoverAfterRollbackFailWhenScopeAborted()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
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
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestRecoverAfterFailOnTransactionBeforePrepareSent()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
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
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestRecoverAfterFailOnTransactionAfterPrepareSent()
        {
            // Test initialize - Fills in queue with data to send and clears the DB.
            PurgeDatabase();
            PurgeAndFillQueue();

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
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
            VerifyBrokerQueueCount();
        }

    }
}
