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
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ.Transport.Tcp;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class AMQNET366Test : NMSTestSupport
    {
        private IConnection connection;
        private bool connectionClosed = false;
        private readonly String connectionUri = "activemq:tcpfaulty://${activemqhost}:61616";

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();
        }

        [Test, Timeout(60000)]
        public void TestConnection()
        {
            IConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionUri));

            using (connection = factory.CreateConnection())
            using (ISession session = connection.CreateSession())
            {
                IDestination destination = SessionUtil.GetDestination(session, "queue://TEST.test.in");
                using (IMessageConsumer consumer = session.CreateConsumer(destination))
                {
                    Connection amqConnection = connection as Connection;
                    connection.ExceptionListener += ConnectionException;

                    consumer.Listener += OnMessage;

                    TcpFaultyTransport transport = amqConnection.ITransport.Narrow(typeof(TcpFaultyTransport)) as TcpFaultyTransport;
                    Assert.IsNotNull(transport);
                    transport.OnewayCommandPreProcessor += FailOnKeepAlive;

                    Thread.Sleep(TimeSpan.FromSeconds(2));

                    connection.Start();

                    int count = 30;
                    while (count-- > 0)
                    {
                        if (!connectionClosed)
                        {
                            Thread.Sleep(TimeSpan.FromSeconds(3));
                        }
                    }

                    Assert.IsTrue(connectionClosed);
                }
            }
        }

        public void FailOnKeepAlive(ITransport transport, Command command)
        {
            if (command.IsKeepAliveInfo)
            {
                throw new IOException("Simulated Transport Failure");
            }
        }

        protected void OnMessage(IMessage receivedMsg)
        {
            var textMessage = receivedMsg as ITextMessage;

            if (textMessage == null)
            {
                Tracer.Info("null");
            }
            else
            {
                Tracer.Info(textMessage.Text);
            }
        }

        private void ConnectionException(Exception e)
        {
            Tracer.Debug("Connection signalled an Exception");
            connection.Close();
            this.connectionClosed = true;
        }
    }
}
