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
using System.Net.Sockets;
using Apache.NMS.Test;
using NUnit.Framework;
using NUnit.Framework.Extensions;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class NMSConnectionFactoryTest
	{
		[RowTest]
		[Row("tcp://${activemqhost}:61616")]
		[Row("activemq:tcp://${activemqhost}:61616")]
		[Row("activemq:tcp://${activemqhost}:61616?connection.asyncclose=false")]
		[Row("activemq:failover:tcp://${activemqhost}:61616")]
		[Row("activemq:failover:(tcp://${activemqhost}:61616)")]
		[Row("activemq:failover:(tcp://${activemqhost}:61616,tcp://${activemqhost}:61616)")]
#if false
		[Row("activemq:discovery:multicast://default")]
		[Row("activemq:discovery:(multicast://default)")]
		[Row("activemq:failover:discovery:multicast://default")]
		[Row("activemq:failover:discovery:(multicast://default)")]
		[Row("activemq:failover:(discovery:(multicast://default))")]
#endif
	
		[Row("tcp://InvalidHost:61616", ExpectedException = typeof(NMSConnectionException))]
		[Row("activemq:tcp://InvalidHost:61616", ExpectedException = typeof(NMSConnectionException))]
		[Row("activemq:tcp://InvalidHost:61616?connection.asyncclose=false", ExpectedException = typeof(NMSConnectionException))]

		[Row("tcp://${activemqhost}:61616?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]
		[Row("activemq:tcp://${activemqhost}:61616?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]
		[Row("activemq:failover:tcp://${activemqhost}:61616?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]
		[Row("activemq:failover:(tcp://${activemqhost}:61616)?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]
		[Row("activemq:failover:(tcp://${activemqhost}:61616,tcp://${activemqbackuphost}:61616)?connection.InvalidParameter=true", ExpectedException = typeof(NMSConnectionException))]

		[Row("ftp://${activemqhost}:61616", ExpectedException = typeof(NMSConnectionException))]
		[Row("http://${activemqhost}:61616", ExpectedException = typeof(NMSConnectionException))]
		[Row("discovery://${activemqhost}:6155", ExpectedException = typeof(NMSConnectionException))]
		[Row("sms://${activemqhost}:61616", ExpectedException = typeof(NMSConnectionException))]
		[Row("activemq:multicast://${activemqhost}:6155", ExpectedException = typeof(NMSConnectionException))]
		[Row("activemq:(tcp://${activemqhost}:61616)?connection.asyncClose=false", ExpectedException = typeof(NMSConnectionException))]

        [Row("(tcp://${activemqhost}:61616,tcp://${activemqhost}:61616)", ExpectedException = typeof(UriFormatException))]
		[Row("tcp://${activemqhost}:61616,tcp://${activemqhost}:61616", ExpectedException = typeof(UriFormatException))]
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
