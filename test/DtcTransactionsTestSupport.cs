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
using System.Xml.Linq;
using NUnit.Framework;
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using System.Data.SqlClient;
using System.Collections;
using System.Data;

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

        protected static string createDbConnectionString = string.Empty;
        protected static string createTableConnectionString = string.Empty;
        protected static string testDbName = string.Empty;
        protected static string testTable = string.Empty;
        protected static string testColumn = string.Empty;
        protected static string testQueueName = string.Empty;
        protected static string testDbFileNameLocation = string.Empty;
        protected static string connectionUri = string.Empty;

        [SetUp]
        public override void SetUp()
        {
            var currentFilePath = Directory.GetCurrentDirectory();
            var xElement = XElement.Load(currentFilePath + "\\test\\TestDbConfig.xml");

            var testDbConnectionString = xElement.Element("createDbConnectionString");
            if (testDbConnectionString != null) createDbConnectionString = testDbConnectionString.Attribute("name")?.Value;

            var testTableConnectionString = xElement.Element("createTableConnectionString");
            if (testTableConnectionString != null) createTableConnectionString = testTableConnectionString.Attribute("name")?.Value;

            var table = xElement.Element("testSqlTable");
            if (table != null) testTable = table.Attribute("name")?.Value;

            var column = xElement.Element("testSqlColumn");
            if (column != null) testColumn = column.Attribute("name")?.Value;

            var dbName = xElement.Element("testDbName");
            if (dbName != null) testDbName = dbName.Attribute("name")?.Value;

            var queueName = xElement.Element("testSqlQueueName");
            if (queueName != null) testQueueName = queueName.Attribute("name")?.Value;

            var fileName = xElement.Element("dbFileNameLocation");
            if (fileName != null) testDbFileNameLocation = fileName.Attribute("name")?.Value;

            var conUri = xElement.Element("connectionURI");
            if (conUri != null) connectionUri = conUri.Attribute("name")?.Value;

            this.oldTracer = Tracer.Trace;
            this.nonExistantPath = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString());

            base.SetUp();

            if (!CheckDatabaseExists(createDbConnectionString, testDbName))
            {
                CreateTestDb();
                CreateTestTable();
            }


            PurgeDestination();
        }

        [TearDown]
        public override void TearDown()
        {
            DeleteDestination();
            
            base.TearDown();

            Tracer.Trace = this.oldTracer;
        }

        private bool CheckDatabaseExists(string sqlTmpConnectionString, string databaseName)
        {
            bool result;
            try
            {
                var tmpConn = new SqlConnection(sqlTmpConnectionString);

                var sqlCreateDbQuery = $"SELECT database_id FROM sys.databases WHERE Name = '{databaseName}'";

                using (tmpConn)
                {
                    using (var sqlCmd = new SqlCommand(sqlCreateDbQuery, tmpConn))
                    {
                        tmpConn.Open();

                        var resultObj = sqlCmd.ExecuteScalar();

                        var databaseId = 0;

                        if (resultObj != null)
                        {
                            int.TryParse(resultObj.ToString(), out databaseId);
                        }

                        tmpConn.Close();

                        result = (databaseId > 0);
                    }
                }
            }
            catch (Exception)
            {
                result = false;
            }

            return result;
        }

        private void CreateTestDb()
        {
            //var createConnection = new SqlConnection("Data Source=WKRKL-F1493EW;Trusted_Connection=yes;User ID=Rafal.Bak;Password=Miesiac*2");
            var createConnection = new SqlConnection(createDbConnectionString);

            var createDb = "CREATE DATABASE " + testDbName + " ON PRIMARY " +
                           "(NAME = " + testDbName + ", " +
                           "FILENAME = '" + testDbFileNameLocation + "\\MyDatabaseData.mdf', " +
                           "SIZE = 8192KB, MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB) " +
                           "LOG ON (NAME = TestDB_log, " +
                           "FILENAME = '" + testDbFileNameLocation + "\\MyDatabaseLog.ldf', " +
                           "SIZE = 8192KB, " +
                           "MAXSIZE = 2048GB," +
                           "FILEGROWTH = 65536KB) ";

            var createDbCommand = new SqlCommand(createDb, createConnection);
            try
            {
                createConnection.Open();
                createDbCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (createConnection.State == ConnectionState.Open)
                {
                    createConnection.Close();
                }
            }
        }

        private void CreateTestTable()
        {
            var createConnection = new SqlConnection(createTableConnectionString);

            var createTable = "CREATE TABLE [dbo].[" + testTable + "]([" + testColumn +
                              "][nchar](10) NULL ) ON [PRIMARY]";

            var createTableCommand = new SqlCommand(createTable, createConnection);
            try
            {
                createConnection.Open();
                createTableCommand.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            finally
            {
                if (createConnection.State == ConnectionState.Open)
                {
                    createConnection.Close();
                }
            }
        }

        protected void OnException(Exception ex)
        {
            Tracer.DebugFormat("Test Driver received Error Notification: {0}", ex.Message);
        }

        #region Database Utility Methods

        protected static void PrepareDatabase()
        {
            using (var sqlConnection = new SqlConnection(createDbConnectionString))
            {
                sqlConnection.Open();

                // remove all data from test table
                using (SqlCommand sqlCommand = new SqlCommand($"TRUNCATE TABLE {testTable}", sqlConnection))
                {
                    sqlCommand.ExecuteNonQuery();
                }

                // add some data to test table
                for (var i = 0; i < MSG_COUNT; ++i)
                {
                    using (var sqlCommand = new SqlCommand(
                        $"INSERT INTO {testTable} ({testColumn}) values ({i})", sqlConnection))
                    {
                        sqlCommand.ExecuteNonQuery();
                    }
                }

                sqlConnection.Close();
            }
        }

        protected static void PurgeDatabase()
        {
            using (var sqlConnection = new SqlConnection(createDbConnectionString))
            {
                sqlConnection.Open();

                // remove all data from test table
                using (var sqlCommand = new SqlCommand($"TRUNCATE TABLE {testTable}", sqlConnection))
                {
                    sqlCommand.ExecuteNonQuery();
                }

                sqlConnection.Close();
            }
        }

        protected static IList ExtractDataSet()
        {
            IList entries = new ArrayList();

            using (var sqlConnection = new SqlConnection(createDbConnectionString))
            {
                sqlConnection.Open();

                using (var sqlReadCommand = new SqlCommand(
                    $"SELECT {testColumn} FROM {testTable}", sqlConnection))
                using (var reader = sqlReadCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        entries.Add("Hello World " + (string)reader[0]);
                    }
                }
            }

            return entries;
        }

        protected static void VerifyDatabaseTableIsEmpty()
        {
            using (var sqlConnection = new SqlConnection(createDbConnectionString))
            {
                sqlConnection.Open();
                var sqlCommand = new SqlCommand(
                    $"SELECT COUNT(*) FROM {testTable}",
                    sqlConnection);
                var count = (int)sqlCommand.ExecuteScalar();
                Assert.AreEqual(0, count, "wrong number of rows in DB");
            }
        }

        protected static void VerifyDatabaseTableIsFull()
        {
            VerifyDatabaseTableIsFull(MSG_COUNT);
        }

        protected static void VerifyDatabaseTableIsFull(int expected)
        {
            using (var sqlConnection = new SqlConnection(createDbConnectionString))
            {
                sqlConnection.Open();
                var sqlCommand = new SqlCommand(
                    $"SELECT COUNT(*) FROM {testTable}",
                    sqlConnection);
                var count = (int)sqlCommand.ExecuteScalar();
                Assert.AreEqual(expected, count, "wrong number of rows in DB");
            }
        }

        #endregion

        #region Destination Utility Methods

        protected static void DeleteDestination()
        {
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionUri));

            using (var connection = factory.CreateConnection() as Connection)
            {
                using (var session = connection.CreateSession())
                {
                    var queue = session.GetQueue(testQueueName);
                    try
                    {
                        connection.DeleteDestination(queue);
                    }
                    catch(Exception e)
                    {
                        throw new Exception(e.Message);
                    }
                }
            }
        }

        protected static void PurgeDestination()
        {
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionUri));

            using (var connection = factory.CreateConnection())
            {
                connection.Start();

                using (var session = connection.CreateSession())
                using (var consumer = session.CreateConsumer(session.GetQueue(testQueueName)))
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
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionUri));

            using (var connection = factory.CreateConnection())
            {
                connection.Start();

                using (var session = connection.CreateSession())
                {
                    var queue = session.GetQueue(testQueueName);

                    // empty queue
                    using (var consumer = session.CreateConsumer(queue))
                    {
                        while ((consumer.Receive(TimeSpan.FromMilliseconds(2000))) != null)
                        {
                        }
                    }

                    // enqueue several messages
                    using (var producer = session.CreateProducer(queue))
                    {
                        producer.DeliveryMode = MsgDeliveryMode.Persistent;

                        for (var i = 0; i < msgCount; i++)
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
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionUri));

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
            VerifyBrokerQueueCount(MSG_COUNT, connectionUri);
        }

        protected void VerifyBrokerQueueCount(int expectedCount)
        {
            VerifyBrokerQueueCount(expectedCount, connectionUri);
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
            IConnectionFactory factory = new ConnectionFactory(ReplaceEnvVar(connectionUri));

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
                        using (SqlConnection sqlConnection = new SqlConnection(createDbConnectionString))
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
                        using (SqlConnection sqlConnection = new SqlConnection(createDbConnectionString))
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
                        using (SqlConnection sqlConnection = new SqlConnection(createDbConnectionString))
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
                        using (SqlConnection sqlConnection = new SqlConnection(createDbConnectionString))
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
