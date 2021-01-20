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
using Apache.NMS.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class BrokerToNMSExceptionsTest : NMSTestSupport
    {
        private readonly String connectionURI = "tcp://${activemqhost}:61616";

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();
        }

        [TearDown]
        public override void TearDown()
        {
            base.TearDown();
        }

        [Test]
        public void InvalidSelectorExceptionTest()
        {
            using(IConnection connection = CreateConnection())
            using(ISession session = connection.CreateSession(AcknowledgementMode.IndividualAcknowledge))
            {
                ITemporaryQueue queue = session.CreateTemporaryQueue();

                try
                {
                    session.CreateConsumer(queue, "3+5");
                    Assert.Fail("Should throw an InvalidSelectorException");
                }
                catch(InvalidSelectorException)
                {
                }
            }
        }

        [Test]
        public void InvalidClientIdExceptionTest()
        {
            Uri uri = URISupport.CreateCompatibleUri(NMSTestSupport.ReplaceEnvVar(connectionURI));
            ConnectionFactory factory = new ConnectionFactory(uri);
            Assert.IsNotNull(factory);
            using(IConnection connection = factory.CreateConnection())
            {
                connection.ClientId = "FOO";
                connection.Start();

                try
                {
                    IConnection connection2 = factory.CreateConnection();
                    connection2.ClientId = "FOO";
                    connection2.Start();
                    Assert.Fail("Should throw an InvalidSelectorException");
                }
                catch(InvalidClientIDException)
                {
                }
            }
        }
    }
}

