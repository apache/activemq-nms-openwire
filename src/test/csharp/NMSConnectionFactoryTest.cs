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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Mock;
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class NMSConnectionFactoryTest
	{
        private static String username = "guest";
        private static String password = "guest";
        private ConnectionInfo info = null;

		[Test]
		[TestCase("tcp://${activemqhost}:61616")]
		[TestCase("activemq:tcp://${activemqhost}:61616")]
		[TestCase("activemq:tcp://${activemqhost}:61616/0.0.0.0:0")]
		[TestCase("activemq:tcp://${activemqhost}:61616?connection.asyncclose=false")]
		[TestCase("activemq:failover:tcp://${activemqhost}:61616")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616)")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616,tcp://${activemqhost}:61616)")]
		[TestCase("activemq:failover://(tcp://${activemqhost}:61616)?initialReconnectDelay=100")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616)?connection.asyncSend=true")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616)?timeout=100&connection.asyncSend=true")]

#if false
		[TestCase("activemq:discovery:multicast://default")]
		[TestCase("activemq:discovery:(multicast://default)")]
		[TestCase("activemq:failover:discovery:multicast://default")]
		[TestCase("activemq:failover:discovery:(multicast://default)")]
		[TestCase("activemq:failover:(discovery:(multicast://default))")]
#endif

		[TestCase("activemq:tcp://${activemqhost}:61616/InvalidHost:0", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:tcp://${activemqhost}:61616/0.0.0.0:-1", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("tcp://InvalidHost:61616", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:tcp://InvalidHost:61616", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:tcp://InvalidHost:61616?connection.asyncclose=false", ExpectedException = typeof(NMSConnectionException))]

		[TestCase("tcp://${activemqhost}:61616?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:tcp://${activemqhost}:61616?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:failover:tcp://${activemqhost}:61616?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616)?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616,tcp://${activemqbackuphost}:61616)?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]

		[TestCase("ftp://${activemqhost}:61616", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("http://${activemqhost}:61616", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("discovery://${activemqhost}:6155", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("sms://${activemqhost}:61616", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:multicast://${activemqhost}:6155", ExpectedException = typeof(NMSConnectionException))]
		[TestCase("activemq:(tcp://${activemqhost}:61616)?connection.asyncClose=false", ExpectedException = typeof(NMSConnectionException))]

		[TestCase("(tcp://${activemqhost}:61616,tcp://${activemqhost}:61616)", ExpectedException = typeof(UriFormatException))]
		[TestCase("tcp://${activemqhost}:61616,tcp://${activemqhost}:61616", ExpectedException = typeof(UriFormatException))]
		public void TestURI(string connectionURI)
		{
			NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));
			Assert.IsNotNull(factory);
			Assert.IsNotNull(factory.ConnectionFactory);
			using(IConnection connection = factory.CreateConnection("", ""))
			{
				Assert.IsNotNull(connection);
			}
		}

        [Test]
        public void TestConnectionSendsAuthenticationData()
        {
            NMSConnectionFactory factory = new NMSConnectionFactory("activemq:mock://localhost:61616");
            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using(Connection connection = factory.CreateConnection(username, password) as Connection)
            {
                Assert.IsNotNull(connection);

                MockTransport transport = (MockTransport) connection.ITransport.Narrow(typeof(MockTransport));

                transport.OutgoingCommand = new CommandHandler(OnOutgoingCommand);

                connection.Start();

                Thread.Sleep(1000);
                
                Assert.IsNotNull(this.info);
                Assert.AreEqual(username, info.UserName);
                Assert.AreEqual(password, info.Password);
            }
        }

        public void OnOutgoingCommand(ITransport transport, Command command)
        {
            if(command.IsConnectionInfo)
            {
                this.info = command as ConnectionInfo;
            }
        }

        [Test]
        public void TestURIForPrefetchHandling()
        {
            string uri1 = "activemq:tcp://${activemqhost}:61616" +
                          "?nms.PrefetchPolicy.queuePrefetch=1" +
                          "&nms.PrefetchPolicy.queueBrowserPrefetch=2" +
                          "&nms.PrefetchPolicy.topicPrefetch=3" +
                          "&nms.PrefetchPolicy.durableTopicPrefetch=4" +
                          "&nms.PrefetchPolicy.maximumPendingMessageLimit=5";

            string uri2 = "activemq:tcp://${activemqhost}:61616" +
                          "?nms.PrefetchPolicy.queuePrefetch=112" +
                          "&nms.PrefetchPolicy.queueBrowserPrefetch=212" +
                          "&nms.PrefetchPolicy.topicPrefetch=312" +
                          "&nms.PrefetchPolicy.durableTopicPrefetch=412" +
                          "&nms.PrefetchPolicy.maximumPendingMessageLimit=512";

            NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri1));

            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using(IConnection connection = factory.CreateConnection("", ""))
            {
                Assert.IsNotNull(connection);

                Connection amqConnection = connection as Connection;
                Assert.AreEqual(1, amqConnection.PrefetchPolicy.QueuePrefetch);
                Assert.AreEqual(2, amqConnection.PrefetchPolicy.QueueBrowserPrefetch);
                Assert.AreEqual(3, amqConnection.PrefetchPolicy.TopicPrefetch);
                Assert.AreEqual(4, amqConnection.PrefetchPolicy.DurableTopicPrefetch);
                Assert.AreEqual(5, amqConnection.PrefetchPolicy.MaximumPendingMessageLimit);
            }

            factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri2));

            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using(IConnection connection = factory.CreateConnection("", ""))
            {
                Assert.IsNotNull(connection);

                Connection amqConnection = connection as Connection;
                Assert.AreEqual(112, amqConnection.PrefetchPolicy.QueuePrefetch);
                Assert.AreEqual(212, amqConnection.PrefetchPolicy.QueueBrowserPrefetch);
                Assert.AreEqual(312, amqConnection.PrefetchPolicy.TopicPrefetch);
                Assert.AreEqual(412, amqConnection.PrefetchPolicy.DurableTopicPrefetch);
                Assert.AreEqual(512, amqConnection.PrefetchPolicy.MaximumPendingMessageLimit);
            }
        }
    }
}
