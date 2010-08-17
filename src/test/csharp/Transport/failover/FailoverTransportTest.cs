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
using System.Collections.Generic;
using System.Threading;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Failover;
using Apache.NMS.ActiveMQ.Transport.Mock;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class FailoverTransportTest
	{
        private List<Command> received;
        private List<Exception> exceptions;

        int sessionIdx = 1;
        int consumerIdx = 1;
        int producerIdx = 1;
        
        public void OnException(ITransport transport, Exception exception)
        {
            Tracer.Debug("Test: Received Exception from Transport: " + exception );
            exceptions.Add( exception );
        }
        
        public void OnCommand(ITransport transport, Command command)
        {
            Tracer.Debug("Test: Received Command from Transport: " + command );
            received.Add( command );
        }

        [SetUp]
        public void init()
        {
            this.received = new List<Command>();
            this.exceptions = new List<Exception>();
            this.sessionIdx = 1;
            this.consumerIdx = 1;
            this.producerIdx = 1;
        }

        [Test]
        public void FailoverTransportCreateTest()
        {
            Uri uri = new Uri("failover:(mock://localhost:61616)?randomize=false");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport(uri);
            Assert.IsNotNull(transport);

            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);
          
            transport.Start();

            Thread.Sleep(1000);
            Assert.IsTrue(failover.IsConnected);
            
            transport.Stop();
            transport.Dispose();
        }

        [Test]
        public void FailoverTransportWithBackupsTest()
        {
            Uri uri = new Uri("failover:(mock://localhost:61616,mock://localhost:61618)?randomize=false&backup=true");
        
            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);
            Assert.IsTrue(failover.Backup);

            transport.Start();

            Thread.Sleep(1000);
            Assert.IsTrue(failover.IsConnected);
            
            transport.Stop();      
            transport.Dispose();
        }

        [Test]
        public void FailoverTransportCreateFailOnCreateTest()
        {
            Uri uri = new Uri("failover:(mock://localhost:61616?transport.failOnCreate=true)?" +
                              "useExponentialBackOff=false&maxReconnectAttempts=3&initialReconnectDelay=100");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsTrue(failover.MaxReconnectAttempts == 3);

            transport.Start();

            Thread.Sleep(2000);
            Assert.IsNotEmpty(this.exceptions);
            Assert.IsFalse(failover.IsConnected);

            transport.Stop();
            transport.Dispose();
        }

        [Test]
        public void FailoverTransportFailOnSendMessageTest()
        {
            Uri uri = new Uri("failover:(mock://localhost:61616?transport.failOnCreate=true)?" +
                              "useExponentialBackOff=false&maxReconnectAttempts=3&initialReconnectDelay=100");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsTrue(failover.MaxReconnectAttempts == 3);

            transport.Start();

            try{
                ActiveMQMessage message = new ActiveMQMessage();
                transport.Oneway(message);

                Assert.Fail("Oneway call should block and then throw.");
            }
            catch(Exception)
            {
            }

            Assert.IsNotEmpty(this.exceptions);
            Assert.IsFalse(failover.IsConnected);
            
            transport.Stop();
            transport.Dispose();
        }

        [Test]
        public void FailoverTransportFailingBackupsTest()
        {
            Uri uri = new Uri(
                "failover:(mock://localhost:61616," +
                          "mock://localhost:61618?transport.failOnCreate=true)?randomize=false&backup=true");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsTrue(failover.Backup = true);

            transport.Start();

            Thread.Sleep(2000);
            
            Assert.IsTrue(failover.IsConnected);

            transport.Stop();
            transport.Dispose();
        }

        [Test]
        public void FailoverTransportSendOnewayMessageTest()
        {
            int numMessages = 1000;
            Uri uri = new Uri(
                "failover:(mock://localhost:61616)?randomize=false");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);

            transport.Start();

            Thread.Sleep(1000);

            Assert.IsTrue(failover.IsConnected);

            MockTransport mock = null;
            while(mock == null ) {
                mock = (MockTransport) transport.Narrow(typeof(MockTransport));
				Thread.Sleep(50);
			}
            mock.OutgoingCommand = new CommandHandler(OnCommand);

            ActiveMQMessage message = new ActiveMQMessage();
            for(int i = 0; i < numMessages; ++i) {
                transport.Oneway(message);
            }

            Thread.Sleep(2000);

            Assert.IsTrue(this.received.Count == numMessages);

            transport.Stop();
            transport.Dispose();
        }

        [Test]
        public void FailoverTransportSendRequestTest()
        {
            Uri uri = new Uri(
                "failover:(mock://localhost:61616)?randomize=false");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);

            transport.Start();

            Thread.Sleep(1000);

            Assert.IsTrue(failover.IsConnected);

            MockTransport mock = null;
            while(mock == null ) {
                mock = (MockTransport) transport.Narrow(typeof(MockTransport));
				Thread.Sleep(50);
			}
            mock.OutgoingCommand = new CommandHandler(OnCommand);

            ActiveMQMessage message = new ActiveMQMessage();
            
            transport.Request(message);
            transport.Request(message);
            transport.Request(message);
            transport.Request(message);

            Thread.Sleep(1000);

            Assert.IsTrue(this.received.Count == 4);

            transport.Stop();
            transport.Dispose();
        }

        [Test]
        public void FailoverTransportSendOnewayFailTest()
        {
            Uri uri = new Uri(
                "failover:(mock://localhost:61616?failOnSendMessage=true," +
                          "mock://localhost:61618)?randomize=false");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);

            transport.Start();

            Thread.Sleep(1000);

            Assert.IsTrue(failover.IsConnected);

            MockTransport mock = null;
            while(mock == null ) {
                mock = (MockTransport) transport.Narrow(typeof(MockTransport));
				Thread.Sleep(50);
			}
            mock.OutgoingCommand = new CommandHandler(OnCommand);

            ActiveMQMessage message = new ActiveMQMessage();
            
            transport.Oneway(message);
            transport.Oneway(message);
            transport.Oneway(message);
            transport.Oneway(message);

            Thread.Sleep(1000);

            Assert.IsTrue(this.received.Count == 4);

            transport.Stop();
            transport.Dispose();
        }

        [Test]
        public void FailoverTransportSendOnewayTimeoutTest()
        {
            Uri uri = new Uri(
                "failover:(mock://localhost:61616?failOnCreate=true)?timeout=1000");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.AreEqual(1000, failover.Timeout);

            transport.Start();

            Thread.Sleep(1000);

            ActiveMQMessage message = new ActiveMQMessage();
            
            try
            {
                transport.Oneway(message);
                Assert.Fail("Should have thrown an IOException after timeout.");
            }
            catch
            {
            }

            transport.Stop();
            transport.Dispose();
        }
        
        [Test]
        public void FailoverTransportSendRequestFailTest()
        {
            Uri uri = new Uri(
                "failover:(mock://localhost:61616?failOnSendMessage=true," +
                          "mock://localhost:61618)?randomize=false");

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);

            transport.Start();

            Thread.Sleep(1000);

            Assert.IsTrue(failover.IsConnected);

            MockTransport mock = null;
            while(mock == null ) {
                mock = (MockTransport) transport.Narrow(typeof(MockTransport));
				Thread.Sleep(50);
			}
            mock.OutgoingCommand = new CommandHandler(OnCommand);

            ActiveMQMessage message = new ActiveMQMessage();
            
            transport.Request(message);
            transport.Request(message);
            transport.Request(message);
            transport.Request(message);

            Thread.Sleep(1000);

            Assert.IsTrue(this.received.Count == 4);

            transport.Stop();
            transport.Dispose();
        }

        [Test]
        public void OpenWireCommandsTest() {
        
            Uri uri = new Uri("failover:(mock://localhost:61616)?randomize=false");
        
            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);
        
            transport.Start();

            Thread.Sleep(1000);

            Assert.IsTrue(failover.IsConnected);
        
            ConnectionInfo connection = createConnection();
            transport.Request( connection );
            SessionInfo session1 = createSession( connection );
            transport.Request( session1 );
            SessionInfo session2 = createSession( connection );
            transport.Request( session2 );
            ConsumerInfo consumer1 = createConsumer( session1 );
            transport.Request( consumer1 );
            ConsumerInfo consumer2 = createConsumer( session1 );
            transport.Request( consumer2 );
            ConsumerInfo consumer3 = createConsumer( session2 );
            transport.Request( consumer3 );
        
            ProducerInfo producer1 = createProducer( session2 );
            transport.Request( producer1 );
        
            // Remove the Producers
            disposeOf( producer1, transport );
        
            // Remove the Consumers
            disposeOf( consumer1, transport );
            disposeOf( consumer2, transport );
            disposeOf( consumer3, transport );
        
            // Remove the Session instances.
            disposeOf( session1, transport );
            disposeOf( session2, transport );
        
            // Indicate that we are done.
            ShutdownInfo shutdown = new ShutdownInfo();
            transport.Oneway(shutdown);
        
            transport.Stop();
            transport.Dispose();
        }

        protected ConnectionInfo createConnection() {
        
            ConnectionId id = new ConnectionId();
            id.Value = Guid.NewGuid().ToString();
        
            ConnectionInfo info = new ConnectionInfo();
            info.ClientId = Guid.NewGuid().ToString();
            info.ConnectionId = id;
        
            return info;
        }

        SessionInfo createSession( ConnectionInfo parent ) {
        
            SessionId id = new SessionId();
            id.ConnectionId = parent.ConnectionId.Value;
            id.Value = sessionIdx++;
        
            SessionInfo info = new SessionInfo();
            info.SessionId = id;
        
            return info;
        }
        
        ConsumerInfo createConsumer( SessionInfo parent ) {
                
            ConsumerId id = new ConsumerId();
            id.ConnectionId = parent.SessionId.ConnectionId;
            id.SessionId = parent.SessionId.Value;
            id.Value = consumerIdx++;
        
            ConsumerInfo info = new ConsumerInfo();
            info.ConsumerId = id;
        
            return info;
        }
        
        ProducerInfo createProducer( SessionInfo parent ) {
                
            ProducerId id = new ProducerId();
            id.ConnectionId = parent.SessionId.ConnectionId;
            id.SessionId = parent.SessionId.Value;
            id.Value = producerIdx++;
        
            ProducerInfo info = new ProducerInfo();
            info.ProducerId = id;
        
            return info;
        }
        
        void disposeOf( SessionInfo session, ITransport transport ) {
        
            RemoveInfo command = new RemoveInfo();
            command.ObjectId = session.SessionId;
            transport.Oneway( command );
        }
        
        void disposeOf( ConsumerInfo consumer, ITransport transport ) {
        
            RemoveInfo command = new RemoveInfo();
            command.ObjectId = consumer.ConsumerId;
            transport.Oneway( command );
        }
        
        void disposeOf( ProducerInfo producer, ITransport transport ) {
        
            RemoveInfo command = new RemoveInfo();
            command.ObjectId = producer.ProducerId;
            transport.Oneway( command );
        }

		[Test]
		public void TestFailoverTransportConnectionControlHandling()
		{
            Uri uri = new Uri("failover:(mock://localhost:61613)?randomize=false");

			string reconnectTo = "mock://localhost:61616?transport.name=Reconnected";
			string connectedBrokers = "mock://localhost:61616?transport.name=Broker1," +
                                      "mock://localhost:61617?transport.name=Broker2";

			ConnectionControl cmd = new ConnectionControl();
			cmd.FaultTolerant = true;
			cmd.ReconnectTo = reconnectTo;
			cmd.ConnectedBrokers = connectedBrokers;

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport( uri );
            Assert.IsNotNull( transport );
            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);
        
            transport.Start();

            MockTransport mock = null;
            while(mock == null ) {
                mock = (MockTransport) transport.Narrow(typeof(MockTransport));
				Thread.Sleep(50);
			}

            mock.InjectCommand(cmd);

            failover.Remove(true, new Uri[] {new Uri("mock://localhost:61613")});

            Thread.Sleep(1000);

            mock = null;

            while(mock == null) {
                mock = (MockTransport) transport.Narrow(typeof(MockTransport));
				Thread.Sleep(50);
            }

            Assert.AreEqual("Reconnected", mock.Name);

		}
	}
}
