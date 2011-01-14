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
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Tcp;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class TcpFaultyTransportTest : NMSTestSupport
    {
        private bool preProcessorFired;
        private bool postProcessorFired;

        public TcpFaultyTransportTest()
        {
        }

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            preProcessorFired = false;
            postProcessorFired = false;
        }

        [TearDown]
        public override void TearDown()
        {
        }

        public void OnPreProcessCommand(ITransport transport, Command command)
        {
            this.preProcessorFired = true;
        }

        public void OnPostProcessCommand(ITransport transport, Command command)
        {
            this.postProcessorFired = true;
        }

        [Test, Sequential]
        public void TestConnectUsingBasicTransport(
            [Values("tcpfaulty://${activemqhost}:61616", "activemq:tcpfaulty://${activemqhost}:61616")]
            string connectionURI)
        {
            ConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));

            using(Connection connection = factory.CreateConnection() as Connection)
            {
                ITransport transport = connection.ITransport.Narrow(typeof(TcpFaultyTransport)) as ITransport;
                Assert.IsNotNull(transport);

                TcpFaultyTransport testee = transport as TcpFaultyTransport;
                testee.OnewayCommandPreProcessor += new CommandHandler(this.OnPreProcessCommand);
                testee.OnewayCommandPostProcessor += new CommandHandler(this.OnPostProcessCommand);

                using(ISession session = connection.CreateSession())
                {
                    Assert.IsTrue(session.Transacted == false);
                }

                Assert.IsTrue(this.preProcessorFired);
                Assert.IsTrue(this.postProcessorFired);
            }
        }

    }
}

