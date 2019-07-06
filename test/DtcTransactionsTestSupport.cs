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
using System.Transactions;
using System.Threading;

using NUnit.Framework;
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using System.Data.SqlClient;
using System.Collections;

namespace Apache.NMS.ActiveMQ.Test
{
    // PREREQUISITES to run those tests :
    // - A local instance of sql server 2008 running, with a db (e.g. TestDB) that 
    //   as a table (e.g. TestTable) with a single column (e.g. TestID) of type INT.
    //   The test default to using an SQL Connection string with a user id of 
    //   'user' and the password 'password'
    // - AMQ Server 5.4.2+
    // - NMS 1.5+
    //
    // IMPORTANT
    // Because you cannot perform recovery in a process more than once you cannot
    // run these tests sequentially in the NUnit GUI or NUnit Console runner you
    // must run them one at a time from the console or using a tool like the ReSharper
    // plugin for Visual Studio.
    //

    public class DtcTransactionsTestSupport : NMSTestSupport
    {
        protected const int MSG_COUNT = 5;
        protected string nonExistantPath;
        protected NetTxConnectionFactory dtcFactory;
        
        private ITrace oldTracer;

        protected const string sqlConnectionString =
            // "Data Source=localhost;Initial Catalog=TestDB;User ID=user;Password=password";
            "Data Source=.\\SQLEXPRESS;Initial Catalog=TestDB;Integrated Security = true";
        protected const string testTable = "TestTable";
        protected const string testColumn = "TestID";
        protected const string testQueueName = "TestQueue";
        protected const string connectionURI = "tcpfaulty://${activemqhost}:61616";

        [SetUp]
        public override void SetUp()
        {
            this.oldTracer = Tracer.Trace;
            this.nonExistantPath = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString());

            base.SetUp();

            PurgeDestination();
        }

        [TearDown]
        public override void TearDown()
        {
            DeleteDestination();
            
            base.TearDown();

            Tracer.Trace = this.oldTracer;
        }

        protected void OnException(Exception ex)
        {
            Tracer.DebugFormat("Test Driver received Error Notification: {0}", ex.Message);
        }

        #region Database Utility Methods

        protected static void PrepareDatabase()
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                // remove all data from test table
                using (SqlCommand sqlCommand = new SqlCommand(string.Format("TRUNCATE TABLE {0}", testTable), sqlConnection))
                {
                    sqlCommand.ExecuteNonQuery();
                }

                // add some data to test table
                for (int i = 0; i < MSG_COUNT; ++i)
                {
                    using (SqlCommand sqlCommand = new SqlCommand(
                        string.Format(
                                        "INSERT INTO {0} ({1}) values ({2})",
                                        testTable,
                                        testColumn,
                                        i), sqlConnection))
                    {
                        sqlCommand.ExecuteNonQuery();
                    }
                }

                sqlConnection.Close();
            }
        }

        protected static void PurgeDatabase()
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                // remove all data from test table
                using (SqlCommand sqlCommand = new SqlCommand(string.Format("TRUNCATE TABLE {0}", testTable), sqlConnection))
                {
                    sqlCommand.ExecuteNonQuery();
                }

                sqlConnection.Close();
            }
        }

        protected static IList ExtractDataSet()
        {
            IList entries = new ArrayList();

            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();

                using (SqlCommand sqlReadCommand = new SqlCommand(
                    string.Format("SELECT {0} FROM {1}", testColumn, testTable), sqlConnection))
                using (SqlDataReader reader = sqlReadCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        entries.Add("Hello World " + (int)reader[0]);
                    }
                }
            }

            return entries;
        }

        protected static void VerifyDatabaseTableIsEmpty()
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand sqlCommand = new SqlCommand(
                    string.Format("SELECT COUNT(*) FROM {0}", testTable),
                    sqlConnection);
                int count = (int)sqlCommand.ExecuteScalar();
                Assert.AreEqual(0, count, "wrong number of rows in DB");
            }
        }

        protected static void VerifyDatabaseTableIsFull()
        {
            VerifyDatabaseTableIsFull(MSG_COUNT);
        }

        protected static void VerifyDatabaseTableIsFull(int expected)
        {
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                SqlCommand sqlCommand = new SqlCommand(
                    string.Format("SELECT COUNT(*) FROM {0}", testTable),
                    sqlConnection);
                int count = (int)sqlCommand.ExecuteScalar();
                Assert.AreEqual(expected, count, "wrong number of rows in DB");
            }
        }

        #endregion

        #region Destination Utility Methods

        protected static void DeleteDestination()
        {
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (Connection connection = factory.CreateConnection() as Connection)
            {
                using (ISession session = connection.CreateSession())
                {
                    IQueue queue = session.GetQueue(testQueueName);
                    try
                    {
                        connection.DeleteDestination(queue);
                    }
                    catch
                    {
                    }
                }
            }
        }

        protected static void PurgeDestination()
        {
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (IConnection connection = factory.CreateConnection())
            {
                connection.Start();

                using (ISession session = connection.CreateSession())
                using (IMessageConsumer consumer = session.CreateConsumer(session.GetQueue(testQueueName)))
                {
                    IMessage recvd;
                    while ((recvd = consumer.Receive(TimeSpan.FromMilliseconds(3000))) != null)
                    {
                        Tracer.Debug("Setup Purged Message: " + recvd);
                    }
                }
            }
        }

        protected static void PurgeAndFillQueue()
        {
            PurgeAndFillQueue(MSG_COUNT);
        }

        protected static void PurgeAndFillQueue(int msgCount)
        {
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (IConnection connection = factory.CreateConnection())
            {
                connection.Start();

                using (ISession session = connection.CreateSession())
                {
                    IQueue queue = session.GetQueue(testQueueName);

                    // empty queue
                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        while ((consumer.Receive(TimeSpan.FromMilliseconds(2000))) != null)
                        {
                        }
                    }

                    // enqueue several messages
                    using (IMessageProducer producer = session.CreateProducer(queue))
                    {
                        producer.DeliveryMode = MsgDeliveryMode.Persistent;

                        for (int i = 0; i < msgCount; i++)
                        {
                            producer.Send(session.CreateTextMessage(i.ToString()));
                        }
                    }
                }
            }
        }

        #endregion

        #region Broker Queue State Validation Routines

        protected static void VerifyBrokerQueueCountNoRecovery()
        {
            VerifyBrokerQueueCountNoRecovery(MSG_COUNT);
        }

        protected static void VerifyBrokerQueueCountNoRecovery(int expectedNumberOfMessages)
        {
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (IConnection connection = factory.CreateConnection())
            {
                // check messages are present in the queue
                using (ISession session = connection.CreateSession())
                {
                    IQueue queue = session.GetQueue(testQueueName);

                    using (IQueueBrowser browser = session.CreateBrowser(queue))
                    {
                        connection.Start();
                        int count = 0;
                        IEnumerator enumerator = browser.GetEnumerator();

                        while(enumerator.MoveNext())
                        {
                            IMessage msg = enumerator.Current as IMessage;
                            Assert.IsNotNull(msg, "message is not in the queue !");
                            count++;
                        }

                        // count should match the expected count
                        Assert.AreEqual(expectedNumberOfMessages, count);
                    }
                }
            }
        }

        protected void VerifyBrokerQueueCount()
        {
            VerifyBrokerQueueCount(MSG_COUNT, connectionURI);
        }

        protected void VerifyBrokerQueueCount(int expectedCount)
        {
            VerifyBrokerQueueCount(expectedCount, connectionURI);
        }

        protected void VerifyBrokerQueueCount(string connectionUri)
        {
            VerifyBrokerQueueCount(MSG_COUNT, connectionUri);
        }

        protected void VerifyBrokerQueueCount(int expectedCount, string connectionUri)
        {           
            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                // check messages are present in the queue
                using (INetTxSession session = connection.CreateNetTxSession())
                {
                    IQueue queue = session.GetQueue(testQueueName);

                    using (IQueueBrowser browser = session.CreateBrowser(queue))
                    {
                        connection.Start();
                        int count = 0;
                        IEnumerator enumerator = browser.GetEnumerator();

                        while (enumerator.MoveNext())
                        {
                            IMessage msg = enumerator.Current as IMessage;
                            Assert.IsNotNull(msg, "message is not in the queue !");
                            count++;
                        }

                        // count should match the expected count
                        Assert.AreEqual(expectedCount, count);
                    }
                }
            }
        }

        protected void VerifyNoMessagesInQueueNoRecovery()
        {
            VerifyBrokerQueueCountNoRecovery(0);
        }

        protected void VerifyNoMessagesInQueue()
        {
            VerifyBrokerQueueCount(0);
        }

        protected static void VerifyBrokerStateNoRecover(int expectedNumberOfMessages)
        {
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionURI));

            using (IConnection connection = factory.CreateConnection())
            {
                // check messages are present in the queue
                using (ISession session = connection.CreateSession())
                {
                    IDestination queue = session.GetQueue(testQueueName);

                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        connection.Start();
                        IMessage msg;

                        for (int i = 0; i < expectedNumberOfMessages; ++i)
                        {
                            msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                            Assert.IsNotNull(msg, "message is not in the queue !");
                        }

                        // next message should be empty
                        msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                        Assert.IsNull(msg, "message found but not expected !");
                        consumer.Close();
                    }
                }

                connection.Close();
            }
        }

        protected void VerifyBrokerHasMessagesInQueue(string connectionURI)
        {
            using (INetTxConnection connection = dtcFactory.CreateNetTxConnection())
            {
                // check messages are present in the queue
                using (INetTxSession session = connection.CreateNetTxSession())
                {
                    IDestination queue = session.GetQueue(testQueueName);

                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        connection.Start();

                        for (int i = 0; i < MSG_COUNT; ++i)
                        {
                            IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(2000));
                            Assert.IsNotNull(msg, "message is not in the queue !");
                        }

                        consumer.Close();
                    }
                }
            }
        }


        #endregion

        #region Transport Hools for controlling failure point.

        public void FailOnPrepareTransportHook(ITransport transport, Command command)
        {
            if (command is TransactionInfo)
            {
                TransactionInfo txInfo = command as TransactionInfo;
                if (txInfo.Type == (byte)TransactionType.Prepare)
                {
                    Thread.Sleep(1000);
                    Tracer.Debug("Throwing Error on Prepare.");
                    throw new Exception("Error writing Prepare command");
                }
            }
        }

        public void FailOnRollbackTransportHook(ITransport transport, Command command)
        {
            if (command is TransactionInfo)
            {
                TransactionInfo txInfo = command as TransactionInfo;
                if (txInfo.Type == (byte)TransactionType.Rollback)
                {
                    Tracer.Debug("Throwing Error on Rollback.");
                    throw new Exception("Error writing Rollback command");
                }
            }
        }

        public void FailOnCommitTransportHook(ITransport transport, Command command)
        {
            if (command is TransactionInfo)
            {
                TransactionInfo txInfo = command as TransactionInfo;
                if (txInfo.Type == (byte)TransactionType.CommitTwoPhase)
                {
                    Tracer.Debug("Throwing Error on Commit.");
                    throw new Exception("Error writing Commit command");
                }
            }
        }

        #endregion

        #region Recovery Harness Hooks for controlling failure conditions

        public void FailOnPreLogRecoveryHook(XATransactionId xid, byte[] recoveryInformatio)
        {
            Tracer.Debug("Throwing Error before the Recovery Information is Logged.");
            throw new Exception("Intentional Error Logging Recovery Information");
        }

        #endregion

        #region Produce Messages use cases

        protected static void ReadFromDbAndProduceToQueueWithCommit(INetTxConnection connection)
        {
            IList entries = ExtractDataSet();

            using (INetTxSession session = connection.CreateNetTxSession(true))
            {
                IQueue queue = session.GetQueue(testQueueName);

                // enqueue several messages read from DB
                try
                {
                    using (IMessageProducer producer = session.CreateProducer(queue))
                    {
                        producer.DeliveryMode = MsgDeliveryMode.Persistent;

                        using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                        {
                            sqlConnection.Open();

                            Assert.IsNotNull(Transaction.Current);

                            Tracer.DebugFormat("Sending {0} messages to Broker in this TX", entries.Count);
                            foreach (string textBody in entries)
                            {
                                producer.Send(session.CreateTextMessage(textBody));
                            }

                            using (SqlCommand sqlDeleteCommand = new SqlCommand(
                                string.Format("DELETE FROM {0}", testTable), sqlConnection))
                            {
                                int count = sqlDeleteCommand.ExecuteNonQuery();
                                Assert.AreEqual(entries.Count, count, "wrong number of rows deleted");
                            }

                            scoped.Complete();
                        }
                    }
                }
                catch (Exception e) // exception thrown in TransactionContext.Commit(Enlistment enlistment)
                {
                    Tracer.Debug("TX;Error from TransactionScope: " + e.Message);
                    Tracer.Debug(e.ToString());
                }
            }
        }

        protected static void ReadFromDbAndProduceToQueueWithScopeAborted(INetTxConnection connection)
        {
            IList entries = ExtractDataSet();

            using (INetTxSession session = connection.CreateNetTxSession(true))
            {
                IQueue queue = session.GetQueue(testQueueName);

                // enqueue several messages read from DB
                try
                {
                    using (IMessageProducer producer = session.CreateProducer(queue))
                    {
                        producer.DeliveryMode = MsgDeliveryMode.Persistent;

                        using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                        {
                            sqlConnection.Open();

                            Assert.IsNotNull(Transaction.Current);

                            Tracer.DebugFormat("Sending {0} messages to Broker in this TX", entries.Count);
                            foreach (string textBody in entries)
                            {
                                producer.Send(session.CreateTextMessage(textBody));
                            }

                            using (SqlCommand sqlDeleteCommand = new SqlCommand(
                                string.Format("DELETE FROM {0}", testTable), sqlConnection))
                            {
                                int count = sqlDeleteCommand.ExecuteNonQuery();
                                Assert.AreEqual(entries.Count, count, "wrong number of rows deleted");
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Tracer.Debug("TX;Error from TransactionScope: " + e.Message);
                    Tracer.Debug(e.ToString());
                }
            }
        }

        #endregion

        #region Consume Messages Use Cases

        protected static void ReadFromQueueAndInsertIntoDbWithCommit(INetTxConnection connection)
        {
            using (INetTxSession session = connection.CreateNetTxSession(true))
            {
                IQueue queue = session.GetQueue(testQueueName);

                // read message from queue and insert into db table
                try
                {
                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                        using (SqlCommand sqlInsertCommand = new SqlCommand())
                        {
                            sqlConnection.Open();
                            sqlInsertCommand.Connection = sqlConnection;

                            Assert.IsNotNull(Transaction.Current);

                            for (int i = 0; i < MSG_COUNT; i++)
                            {
                                ITextMessage message = consumer.Receive() as ITextMessage;
                                Assert.IsNotNull(message, "missing message");
                                sqlInsertCommand.CommandText =
                                    string.Format("INSERT INTO {0} VALUES ({1})", testTable, Convert.ToInt32(message.Text));
                                sqlInsertCommand.ExecuteNonQuery();
                            }

                            scoped.Complete();
                        }
                    }
                }
                catch (Exception e)
                {
                    Tracer.Debug("TX;Error from TransactionScope: " + e.Message);
                    Tracer.Debug(e.ToString());
                }
            }
        }

        protected static void ReadFromQueueAndInsertIntoDbWithScopeAborted(INetTxConnection connection)
        {
            using (INetTxSession session = connection.CreateNetTxSession(true))
            {
                IQueue queue = session.GetQueue(testQueueName);

                // read message from queue and insert into db table
                try
                {
                    using (IMessageConsumer consumer = session.CreateConsumer(queue))
                    {
                        using (TransactionScope scoped = new TransactionScope(TransactionScopeOption.RequiresNew))
                        using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
                        using (SqlCommand sqlInsertCommand = new SqlCommand())
                        {
                            sqlConnection.Open();
                            sqlInsertCommand.Connection = sqlConnection;
                            Assert.IsNotNull(Transaction.Current);

                            for (int i = 0; i < MSG_COUNT; i++)
                            {
                                ITextMessage message = consumer.Receive() as ITextMessage;
                                Assert.IsNotNull(message, "missing message");

                                sqlInsertCommand.CommandText =
                                    string.Format("INSERT INTO {0} VALUES ({1})", testTable, Convert.ToInt32(message.Text));
                                sqlInsertCommand.ExecuteNonQuery();
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Tracer.Debug("TX;Error from TransactionScope: " + e.Message);
                    Tracer.Debug(e.ToString());
                }
            }
        }

        #endregion
    }
}
