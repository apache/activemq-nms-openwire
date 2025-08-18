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
using System.Threading.Tasks;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Mock;
using Apache.NMS.Test;
using NUnit.Framework;
using NUnit.Framework.Constraints;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class NMSConnectionFactoryTest
	{
        private static String username = "guest";
        private static String password = "guest";
        private ConnectionInfo info = null;

		[Test, Timeout(20_000)]
		[TestCase("tcp://${activemqhost}:61616")]
        [TestCase("activemq:tcp://${activemqhost}:61616")]
        [TestCase("activemqnettx:tcp://${activemqhost}:61616")]
		[TestCase("activemq:tcp://${activemqhost}:61616/0.0.0.0:0")]
		[TestCase("activemq:tcp://${activemqhost}:61616?connection.asyncclose=false")]
		[TestCase("activemq:failover:tcp://${activemqhost}:61616")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616)")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616,tcp://${activemqhost}:61616)")]
		[TestCase("activemq:failover://(tcp://${activemqhost}:61616)?transport.initialReconnectDelay=100")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616)?connection.asyncSend=true")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616)?transport.timeout=100&connection.asyncSend=true")]
		[TestCase("activemq:failover:tcp://${activemqhost}:61616?keepAlive=false&wireFormat.maxInactivityDuration=1000")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616?keepAlive=false&wireFormat.maxInactivityDuration=1000)")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616?keepAlive=false&wireFormat.maxInactivityDuration=1000)?connection.asyncclose=false")]

#if false
		[TestCase("activemq:discovery:multicast://default")]
		[TestCase("activemq:discovery:(multicast://default)")]
		[TestCase("activemq:failover:discovery:multicast://default")]
		[TestCase("activemq:failover:discovery:(multicast://default)")]
		[TestCase("activemq:failover:(discovery:(multicast://default))")]
#endif
		public void TestURI(string connectionURI)
		{
			NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));
			Assert.IsNotNull(factory);
			Assert.IsNotNull(factory.ConnectionFactory);
			using(IConnection connection = factory.CreateConnection(username, password))
			{
				Assert.IsNotNull(connection);
				connection.Close();
			}
		}

        [TestCase("activemq:tcp://${activemqhost}:61616/InvalidHost:0")]
		[TestCase("activemq:tcp://${activemqhost}:61616/0.0.0.0:-1")]
		[TestCase("tcp://InvalidHost:61616")]
		[TestCase("activemq:tcp://InvalidHost:61616")]
		[TestCase("activemq:tcp://InvalidHost:61616?connection.asyncclose=false")]

		[TestCase("tcp://${activemqhost}:61616?connection.InvalidParameter=true")]
		[TestCase("activemq:tcp://${activemqhost}:61616?connection.InvalidParameter=true")]
		[TestCase("activemq:failover:tcp://${activemqhost}:61616?connection.InvalidParameter=true")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616)?connection.InvalidParameter=true")]
		[TestCase("activemq:failover:(tcp://${activemqhost}:61616,tcp://${activemqbackuphost}:61616)?connection.InvalidParameter=true")]

		[TestCase("ftp://${activemqhost}:61616")]
		[TestCase("http://${activemqhost}:61616")]
		[TestCase("discovery://${activemqhost}:6155")]
		[TestCase("sms://${activemqhost}:61616")]
		[TestCase("activemq:multicast://${activemqhost}:6155")]
		[TestCase("activemq:(tcp://${activemqhost}:61616)?connection.asyncClose=false")]
        public void TestURIFailures_NMSConnectionException(string connectionURI)
        {
            Assert.Throws<NMSConnectionException>(() =>
            {
                NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));
                Assert.IsNotNull(factory);
                Assert.IsNotNull(factory.ConnectionFactory);
                using(IConnection connection = factory.CreateConnection(username, password))
                {
                    Assert.IsNotNull(connection);
                    connection.Close();
                }
            });
        }

        [Test, Timeout(20_000)]
        [TestCase("(tcp://${activemqhost}:61616,tcp://${activemqhost}:61616)")]
        [TestCase("tcp://${activemqhost}:61616,tcp://${activemqhost}:61616")]
        public void TestURIFailures_UriFormatException(string connectionURI)
        {
            Assert.Throws<UriFormatException>(() =>
            {
                NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(connectionURI));
                Assert.IsNotNull(factory);
                Assert.IsNotNull(factory.ConnectionFactory);
                using(IConnection connection = factory.CreateConnection(username, password))
                {
                    Assert.IsNotNull(connection);
                    connection.Close();
                }
            });
        }

        [Test, Timeout(20_000)]
        public void TestConnectionSendsAuthenticationData()
        {
            NMSConnectionFactory factory = new NMSConnectionFactory("activemq:mock://localhost:61616");
            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using(Connection connection = factory.CreateConnection(username, password) as Connection)
            {
                Assert.IsNotNull(connection);

				MockTransport transport = (MockTransport) connection.ITransport.Narrow(typeof(MockTransport));

                transport.OutgoingCommand = new CommandHandlerAsync(OnOutgoingCommand);

                connection.Start();

                Thread.Sleep(1000);
                
                Assert.IsNotNull(this.info);
                Assert.AreEqual(username, info.UserName);
                Assert.AreEqual(password, info.Password);
				
				connection.Close();
            }
        }

        public async Task OnOutgoingCommand(ITransport transport, Command command)
        {
            if(command.IsConnectionInfo)
            {
                this.info = command as ConnectionInfo;
            }
            await Task.CompletedTask;
        }

        [Test, Timeout(20_000)]
		[TestCase(1, 2, 3, 4, 5)]
		[TestCase(112, 212, 312, 412, 512)]
        public void TestURIForPrefetchHandling(int queuePreFetch, int queueBrowserPrefetch, int topicPrefetch, int durableTopicPrefetch, int maximumPendingMessageLimit)
        {
	        string testuri = "activemq:tcp://${activemqhost}:61616" +
	                         $"?nms.PrefetchPolicy.queuePrefetch={queuePreFetch}" +
	                         $"&nms.PrefetchPolicy.queueBrowserPrefetch={queueBrowserPrefetch}" +
	                         $"&nms.PrefetchPolicy.topicPrefetch={topicPrefetch}" +
	                         $"&nms.PrefetchPolicy.durableTopicPrefetch={durableTopicPrefetch}" +
	                         $"&nms.PrefetchPolicy.maximumPendingMessageLimit={maximumPendingMessageLimit}";

            NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(testuri));

            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using(IConnection connection = factory.CreateConnection(username, password))
            {
                Assert.IsNotNull(connection);

                Connection amqConnection = connection as Connection;
                Assert.AreEqual(queuePreFetch, amqConnection.PrefetchPolicy.QueuePrefetch);
                Assert.AreEqual(queueBrowserPrefetch, amqConnection.PrefetchPolicy.QueueBrowserPrefetch);
                Assert.AreEqual(topicPrefetch, amqConnection.PrefetchPolicy.TopicPrefetch);
                Assert.AreEqual(durableTopicPrefetch, amqConnection.PrefetchPolicy.DurableTopicPrefetch);
                Assert.AreEqual(maximumPendingMessageLimit, amqConnection.PrefetchPolicy.MaximumPendingMessageLimit);

				connection.Close();
			}
        }
		
        [Test, Timeout(20_000)]
		[TestCase(0)]
		[TestCase(1)]
		[TestCase(1000)]
        public void TestURIForPrefetchHandlingOfAll(int allPreFetch)
        {
	        string testuri = "activemq:tcp://${activemqhost}:61616" +
	                         $"?nms.PrefetchPolicy.all={allPreFetch}";

            NMSConnectionFactory factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(testuri));

            Assert.IsNotNull(factory);
            Assert.IsNotNull(factory.ConnectionFactory);
            using(IConnection connection = factory.CreateConnection(username, password))
            {
                Assert.IsNotNull(connection);

                Connection amqConnection = connection as Connection;
                Assert.AreEqual(allPreFetch, amqConnection.PrefetchPolicy.QueuePrefetch);
                Assert.AreEqual(allPreFetch, amqConnection.PrefetchPolicy.QueueBrowserPrefetch);
                Assert.AreEqual(allPreFetch, amqConnection.PrefetchPolicy.TopicPrefetch);
                Assert.AreEqual(allPreFetch, amqConnection.PrefetchPolicy.DurableTopicPrefetch);

				connection.Close();
			}
        }

        [Test, Timeout(20_000)]
        public void TestSetDeserializationPolicy()
        {
	        string baseUri = "activemq:tcp://${activemqhost}:61616";
	        string configuredUri = baseUri +
	                               "?nms.deserializationPolicy.allowList=a,b,c" +
	                               "&nms.deserializationPolicy.denyList=c,d,e";

	        var factory = new NMSConnectionFactory(NMSTestSupport.ReplaceEnvVar(configuredUri));

	        Assert.IsNotNull(factory);
	        Assert.IsNotNull(factory.ConnectionFactory);
	        using IConnection connection = factory.CreateConnection(username, password);
	        Assert.IsNotNull(connection);
	        var amqConnection = connection as Connection;
	        var deserializationPolicy = amqConnection.DeserializationPolicy as NmsDefaultDeserializationPolicy;
	        Assert.IsNotNull(deserializationPolicy);
	        Assert.AreEqual("a,b,c", deserializationPolicy.AllowList);
	        Assert.AreEqual("c,d,e", deserializationPolicy.DenyList);
	        connection.Close();
        }
    }
}
