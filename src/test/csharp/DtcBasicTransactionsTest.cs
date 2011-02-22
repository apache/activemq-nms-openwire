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
using Apache.NMS.ActiveMQ.Transactions;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test.src.test.csharp
{
    [TestFixture]
    [Category("Manual")]
    class DtcBasicTransactionsTest : DtcTransactionsTestSupport
    {
        [Test]
        [ExpectedException("Apache.NMS.NMSException")]
        public void TestSessionCreateFailsWithInvalidLogLocation()
        {
            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                NetTxConnection con = connection as NetTxConnection;
                NetTxRecoveryPolicy policy = con.RecoveryPolicy;
                (policy.RecoveryLogger as RecoveryFileLogger).Location = nonExistantPath;
                connection.CreateNetTxSession();
            }
        }

        [Test]
        public void TestTransactedDBReadAndProduce()
        {
            // Test initialize - Fills in DB with data to send.
            PrepareDatabase();

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromDbAndProduceToQueueWithCommit(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has commited the transaction                    
            VerifyDatabaseTableIsEmpty();

            // check messages are present in the queue
            VerifyBrokerQueueCount();
        }

        [Test]
        public void TestTransacteDequeueAndDbWrite()
        {
            // Test initialize - Fills in DB with data to send.
            PurgeDatabase();
            PurgeAndFillQueue();

            INetTxConnectionFactory factory = new NetTxConnectionFactory(ReplaceEnvVar(connectionURI));

            using (INetTxConnection connection = factory.CreateNetTxConnection())
            {
                connection.ExceptionListener += this.OnException;
                connection.Start();

                ReadFromQueueAndInsertIntoDbWithCommit(connection);

                Thread.Sleep(2000);
            }

            // verify sql server has commited the transaction and stored all messages
            VerifyDatabaseTableIsFull();

            // check no messages are present in the queue after commit.
            VerifyNoMessagesInQueueNoRecovery();
        }
    }
}
