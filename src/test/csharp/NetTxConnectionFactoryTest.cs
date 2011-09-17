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

using NUnit.Framework;
using Apache.NMS.Test;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Transactions;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class NetTxConnectionFactoryTest : NMSTestSupport
    {
        [Test]
        [TestCase("tcp://${activemqhost}:61616")]
        [TestCase("tcp://${activemqhost}:61616")]
        [TestCase("tcp://${activemqhost}:61616/0.0.0.0:0")]
        [TestCase("tcp://${activemqhost}:61616?connection.asyncclose=false")]
        [TestCase("failover:tcp://${activemqhost}:61616")]
        [TestCase("failover:(tcp://${activemqhost}:61616)")]
        [TestCase("failover:(tcp://${activemqhost}:61616,tcp://${activemqhost}:61616)")]
        [TestCase("failover://(tcp://${activemqhost}:61616)?transport.initialReconnectDelay=100")]
        [TestCase("failover:(tcp://${activemqhost}:61616)?connection.asyncSend=true")]
        [TestCase("failover:(tcp://${activemqhost}:61616)?transport.timeout=100&connection.asyncSend=true")]
        [TestCase("failover:tcp://${activemqhost}:61616?keepAlive=false&wireFormat.maxInactivityDuration=1000")]
        [TestCase("failover:(tcp://${activemqhost}:61616?keepAlive=false&wireFormat.maxInactivityDuration=1000)")]
        [TestCase("failover:(tcp://${activemqhost}:61616?keepAlive=false&wireFormat.maxInactivityDuration=1000)?connection.asyncclose=false")]
        [TestCase("failover:(tcp://${activemqhost}:61616?keepAlive=true&wireFormat.maxInactivityDuration=300000&wireFormat.tcpNoDelayEnabled=true)?initialReconnectDelay=100&randomize=false&timeout=15000")]
        public void TestURI(string connectionURI)
        {
            {
                Uri uri = URISupport.CreateCompatibleUri(NMSTestSupport.ReplaceEnvVar(connectionURI));
                NetTxConnectionFactory factory = new NetTxConnectionFactory(uri);
                Assert.IsNotNull(factory);
                using(IConnection connection = factory.CreateConnection("", ""))
                {
                    Assert.IsNotNull(connection);
                    
                    using(ISession session = connection.CreateSession())
                    {
                        IDestination destination = session.CreateTemporaryTopic();
                        using(IMessageProducer producer = session.CreateProducer(destination))
                        {
                            producer.Close();
                        }
                        
                        using(IMessageConsumer consumer = session.CreateConsumer(destination))
                        {
                            consumer.Close();
                        }
                        
                        session.Close();
                    }
                    
                    connection.Close();
                }
            }

            {
                NetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));
                Assert.IsNotNull(factory);
                using(IConnection connection = factory.CreateConnection("", ""))
                {
                    Assert.IsNotNull(connection);

                    using(ISession session = connection.CreateSession())
                    {
                        Assert.IsNotNull(session as INetTxSession);

                        IDestination destination = session.CreateTemporaryTopic();
                        using(IMessageProducer producer = session.CreateProducer(destination))
                        {
                            producer.Close();
                        }
                        
                        using(IMessageConsumer consumer = session.CreateConsumer(destination))
                        {
                            consumer.Close();
                        }
                        
                        session.Close();
                    }
                    
                    connection.Close();
                }
            }
        }       
        
        [Test, Sequential]
        public void TestConnectionFactorySetParams(
            [Values("tcp://${activemqhost}:61616", "activemq:tcp://${activemqhost}:61616")]
            string connectionURI,
            [Values(AcknowledgementMode.ClientAcknowledge, AcknowledgementMode.AutoAcknowledge)]
            AcknowledgementMode ackMode,
            [Values(true, false)]
            bool asyncSend,
            [Values(true, false)]
            bool alwaysSyncSend,
            [Values(true, false)]
            bool asyncClose,
            [Values(true, false)]
            bool copyMessageOnSend,
            [Values(3000, 1000)]
            int requestTimeout,
            [Values(true, false)]
            bool sendAcksAsync,
            [Values(true, false)]
            bool dispatchAsync)
        {
            NetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));

            factory.AcknowledgementMode = ackMode;
            factory.AsyncSend = asyncSend;
            factory.AlwaysSyncSend = alwaysSyncSend;
            factory.AsyncClose = asyncClose;
            factory.CopyMessageOnSend = copyMessageOnSend;
            factory.RequestTimeout = requestTimeout;
            factory.SendAcksAsync = sendAcksAsync;
            factory.DispatchAsync = dispatchAsync;

            using(Connection connection = factory.CreateConnection() as Connection)
            {
                Assert.AreEqual(ackMode, connection.AcknowledgementMode);
                Assert.AreEqual(asyncSend, connection.AsyncSend);
                Assert.AreEqual(alwaysSyncSend, connection.AlwaysSyncSend);
                Assert.AreEqual(asyncClose, connection.AsyncClose);
                Assert.AreEqual(copyMessageOnSend, connection.CopyMessageOnSend);
                Assert.AreEqual(requestTimeout, connection.RequestTimeout.TotalMilliseconds);
                Assert.AreEqual(sendAcksAsync, connection.SendAcksAsync);
                Assert.AreEqual(dispatchAsync, connection.DispatchAsync);
            }
        }

        [Test, Sequential]
        public void TestConnectionFactoryParseParams(
            [Values("tcp://${activemqhost}:61616", "activemq:tcp://${activemqhost}:61616")]
            string baseConnectionURI,
            [Values(AcknowledgementMode.ClientAcknowledge, AcknowledgementMode.AutoAcknowledge)]
            AcknowledgementMode ackMode,
            [Values(true, false)]
            bool asyncSend,
            [Values(true, false)]
            bool alwaysSyncSend,
            [Values(true, false)]
            bool asyncClose,
            [Values(true, false)]
            bool copyMessageOnSend,
            [Values(3000, 1000)]
            int requestTimeout,
            [Values(true, false)]
            bool sendAcksAsync,
            [Values(true, false)]
            bool dispatchAsync)
        {
            string connectionURI = string.Format("{0}?" +
                                   "connection.AckMode={1}&" +
                                   "connection.AsyncSend={2}&" +
                                   "connection.AlwaysSyncSend={3}&" +
                                   "connection.AsyncClose={4}&" +
                                   "connection.CopyMessageOnSend={5}&" +
                                   "connection.RequestTimeout={6}&" +
                                   "connection.SendAcksAsync={7}&" +
                                   "connection.DispatchAsync={8}",
                                   baseConnectionURI, ackMode, asyncSend, alwaysSyncSend, asyncClose, copyMessageOnSend, requestTimeout, sendAcksAsync, dispatchAsync);

            NetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));

            using(Connection connection = factory.CreateConnection() as Connection)
            {
                Assert.AreEqual(ackMode, connection.AcknowledgementMode);
                Assert.AreEqual(asyncSend, connection.AsyncSend);
                Assert.AreEqual(alwaysSyncSend, connection.AlwaysSyncSend);
                Assert.AreEqual(asyncClose, connection.AsyncClose);
                Assert.AreEqual(copyMessageOnSend, connection.CopyMessageOnSend);
                Assert.AreEqual(requestTimeout, connection.RequestTimeout.TotalMilliseconds);
                Assert.AreEqual(sendAcksAsync, connection.SendAcksAsync);
                Assert.AreEqual(dispatchAsync, connection.DispatchAsync);
            }
        }

        [Test]
        public void TestConfigureRecoveryPolicyLoggerType(
            [Values("tcp://${activemqhost}:61616?nms.RecoveryPolicy.RecoveryLoggerType=file")]
            string baseConnectionURI)
        {
            INetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(baseConnectionURI));

            using(INetTxConnection connection = factory.CreateNetTxConnection())
            {
                NetTxConnection netTxConnection = connection as NetTxConnection;

                Assert.IsNotNull(netTxConnection);
                NetTxRecoveryPolicy policy = netTxConnection.RecoveryPolicy;

                Assert.AreEqual("file", policy.RecoveryLoggerType);

                RecoveryFileLogger logger = policy.RecoveryLogger as RecoveryFileLogger;

                Assert.IsNotNull(logger);
                Assert.AreEqual(Directory.GetCurrentDirectory(), logger.Location);
            }
        }

        [Test]
        [TestCase("/var/log/nms/recovery/", true)]
        [TestCase("/var/temp/log/nms/recovery/", false)]
        [TestCase("C:\\Transactions\\RecoveryLogs", true)]
        [TestCase("\\\\ServerName\\Transactions\\RecoveryLogs", true)]
        public void TestConfigureRecoveryPolicyLogger(string location, bool autoCreate)
        {
            string testuri = string.Format("activemq:tcp://${{activemqhost}}:61616" +
                                           "?nms.RecoveryPolicy.RecoveryLoggerType=file" +
                                           "&nms.RecoveryPolicy.RecoveryLogger.Location={0}" +
                                           "&nms.RecoveryPolicy.RecoveryLogger.AutoCreateLocation={1}",
                                           location, autoCreate);

            INetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(testuri));

            using(INetTxConnection connection = factory.CreateNetTxConnection())
            {
                NetTxConnection netTxConnection = connection as NetTxConnection;

                Assert.IsNotNull(netTxConnection);
                NetTxRecoveryPolicy policy = netTxConnection.RecoveryPolicy;

                Assert.AreEqual("file", policy.RecoveryLoggerType);

                RecoveryFileLogger logger = policy.RecoveryLogger as RecoveryFileLogger;

                Assert.IsNotNull(logger);
                Assert.AreEqual(location, logger.Location);
                Assert.AreEqual(autoCreate, logger.AutoCreateLocation);
            }
        }
				
    	[Test]
        [TestCase("/var/log/nms/recovery/", true)]
        [TestCase("/var/temp/log/nms/recovery/", false)]
        public void TestConfigureRecoveryPolicyLoggerUsingDefaultLogger(string location, bool autoCreate)
        {
            string testuri = string.Format("activemq:tcp://${{activemqhost}}:61616" +
                                           "?nms.RecoveryPolicy.RecoveryLogger.Location={0}" +
                                           "&nms.RecoveryPolicy.RecoveryLogger.AutoCreateLocation={1}",
                                           location, autoCreate);

            INetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(testuri));

            using(INetTxConnection connection = factory.CreateNetTxConnection())
            {
                NetTxConnection netTxConnection = connection as NetTxConnection;

                Assert.IsNotNull(netTxConnection);
                NetTxRecoveryPolicy policy = netTxConnection.RecoveryPolicy;

                Assert.AreEqual("file", policy.RecoveryLoggerType);

                RecoveryFileLogger logger = policy.RecoveryLogger as RecoveryFileLogger;

                Assert.IsNotNull(logger);
                Assert.AreEqual(location, logger.Location);
                Assert.AreEqual(autoCreate, logger.AutoCreateLocation);
            }
        }
		
        [Test]
        [ExpectedException( "Apache.NMS.NMSException" )]
        public void TestConfigureRecoveryPolicyLoggerTypeWithInvalidType(
            [Values("tcp://${activemqhost}:61616?nms.RecoveryPolicy.RecoveryLoggerType=invalid")]
            string baseConnectionURI)
        {
            INetTxConnectionFactory factory = new NetTxConnectionFactory(NMSTestSupport.ReplaceEnvVar(baseConnectionURI));

            using(IConnection connection = factory.CreateConnection()){}
        }
    }
}


