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
    public class FailoverTransactionTest
    {
        private Connection connection;
        private bool interrupted = false;
        private bool resumed = false;

//        [Test]
//        public void FailoverBeforeCommitSentTest()
//        {
//            string uri = "failover:(tcp://${activemqhost}:61616)";
//            IConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri));
//            using(connection = factory.CreateConnection() as Connection)
//            {
//                connection.ConnectionInterruptedListener +=
//                    new ConnectionInterruptedListener(TransportInterrupted);
//                connection.ConnectionResumedListener +=
//                    new ConnectionResumedListener(TransportResumed);
//
//                connection.Start();
//                using(ISession session = connection.CreateSession())
//                {
//                    IDestination destination = session.GetQueue("Test?consumer.prefetchSize=1");
//                    PurgeQueue(connection, destination);
//                    PutMsgIntoQueue(session, destination);
//
//                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
//                    {
//                        consumer.Listener += OnMessage;
//                        BreakConnection();
//                        WaitForMessagesToArrive();
//                    }
//                }
//            }
//
//            Assert.IsTrue(this.interrupted);
//            Assert.IsTrue(this.resumed);
//        }
//
//        public void TransportInterrupted()
//        {
//            this.interrupted = true;
//        }
//
//        public void TransportResumed()
//        {
//            this.resumed = true;
//        }

    }
}

