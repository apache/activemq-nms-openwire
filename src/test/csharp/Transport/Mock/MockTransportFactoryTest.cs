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
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Mock;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class MockTransportFactoryTest
    {
        [Test]
        public void CreateMockTransportTest()
        {
            MockTransportFactory factory = new MockTransportFactory();
            
            Uri location = new Uri("mock://0.0.0.0:61616");
            
            ITransport transport = factory.CreateTransport(location);
            
            Assert.IsNotNull(transport);
			Assert.IsInstanceOf<MockTransport>(transport.Narrow(typeof(MockTransport)));
            MockTransport mock = (MockTransport) transport.Narrow(typeof(MockTransport));

            Assert.IsTrue( mock.IsConnected );
            Assert.IsFalse( mock.IsFaultTolerant );
        }
        
        [Test]
        public void CreateMockTransportWithParamsTest()
        {
            MockTransportFactory factory = new MockTransportFactory();
            
            Uri location = new Uri("mock://0.0.0.0:61616?transport.failOnSendMessage=true&transport.numSentMessagesBeforeFail=20");
            
            MockTransport transport = (MockTransport) factory.CompositeConnect(location);
            
            Assert.IsNotNull(transport);
            Assert.IsTrue(transport.FailOnSendMessage);
            Assert.AreEqual(20, transport.NumSentMessagesBeforeFail);
        }

        [Test]
        [ExpectedException( "Apache.NMS.ActiveMQ.IOException" )]        
        public void CreationFailMockTransportTest()
        {
            MockTransportFactory factory = new MockTransportFactory();
            
            Uri location = new Uri("mock://0.0.0.0:61616?transport.failOnCreate=true");
            
            factory.CreateTransport(location);
        }
        
    }
}
