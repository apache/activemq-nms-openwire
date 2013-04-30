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
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Failover;
using Apache.NMS.ActiveMQ.Transport.Tcp;
using Apache.NMS.ActiveMQ.Transport.Mock;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class FailoverTransportTest
	{
		private List<Command> sent;
		private List<Command> received;
		private List<Exception> exceptions;

		private const int MAX_ATTEMPTS = 30;
        private const int MESSAGE_COUNT = 5;
        private Connection connection;
        private int msgCount = 5;
        private bool interrupted = false;
        private bool resumed = false;
        protected AutoResetEvent semaphore = new AutoResetEvent(false);

        int sessionIdx = 1;
		int consumerIdx = 1;
		int producerIdx = 1;

		private void OnException(ITransport transport, Exception exception)
		{
			Tracer.Debug("Test: Received Exception from Transport: " + exception);
			exceptions.Add(exception);
		}

		private void OnCommand(ITransport transport, Command command)
		{
			Tracer.DebugFormat("Test: Received Command from Transport: {0}", command);
			received.Add(command);
		}

		private void OnOutgoingCommand(ITransport transport, Command command)
		{
			Tracer.DebugFormat("FailoverTransportTest::OnOutgoingCommand - {0}", command);
			sent.Add(command);
		}

		private void OnResumed(ITransport sender)
		{
			Tracer.DebugFormat("FailoverTransportTest::OnResumed - {0}", sender.RemoteAddress);
			// Ensure the current mock transport has the correct outgoing command handler
			MockTransport mock = sender as MockTransport;
			Assert.IsNotNull(mock);
			mock.OutgoingCommand = OnOutgoingCommand;
		}

		private void OnInterrupted(ITransport sender)
		{
		}

		private void VerifyCommandHandlerSetting(ITransport transport, MockTransport mock)
		{
			// Walk the stack of wrapper transports.
			ITransport failoverTransportTarget = mock.Command.Target as ITransport;
			Assert.IsNotNull(failoverTransportTarget);
			ITransport mutexTransportTarget = failoverTransportTarget.Command.Target as ITransport;
			Assert.IsNotNull(mutexTransportTarget);
			ITransport responseCorrelatorTransportTarget = mutexTransportTarget.Command.Target as ITransport;
			Assert.IsNotNull(responseCorrelatorTransportTarget);
			Assert.AreEqual(transport.Command.Target, responseCorrelatorTransportTarget.Command.Target);
		}

		[SetUp]
		public void init()
		{
			sent = new List<Command>();
			received = new List<Command>();
			exceptions = new List<Exception>();
			sessionIdx = 1;
			consumerIdx = 1;
			producerIdx = 1;
            this.connection = null;
            this.msgCount = MESSAGE_COUNT;
            this.interrupted = false;
            this.resumed = false;
		}

		[Test]
		public void FailoverTransportCreateTest()
		{
			Uri uri = new Uri("failover:(mock://localhost:61616)?transport.randomize=false");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);

				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover, "Failed to create Transport");
				Assert.IsFalse(failover.Randomize, "Failed to properly set Randomize flag");

				transport.Start();

				Thread.Sleep(2000);
				Assert.IsTrue(failover.IsConnected, "Transport should be connected");
			}
		}

		[Test]
		public void FailoverTransportWithBackupsTest()
		{
			Uri uri = new Uri("failover:(mock://localhost:61616,mock://localhost:61618)?transport.randomize=false&transport.backup=true");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize);
				Assert.IsTrue(failover.Backup);

				transport.Start();
				Thread.Sleep(1000);
				Assert.IsTrue(failover.IsConnected);
			}
		}

		[Test]
		public void FailoverTransportCreateFailOnCreateTest()
		{
			Uri uri = new Uri("failover:(mock://localhost:61616?transport.failOnCreate=true)?" +
							  "transport.useExponentialBackOff=false&transport.maxReconnectAttempts=3&transport.initialReconnectDelay=100");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.UseExponentialBackOff);
				Assert.AreEqual(3, failover.MaxReconnectAttempts);
				Assert.AreEqual(100, failover.InitialReconnectDelay);

				transport.Start();
				Thread.Sleep(2000);
				Assert.IsNotEmpty(this.exceptions);
				Assert.IsFalse(failover.IsConnected);
			}
		}

        [Test]
        public void FailoverTransportCreateFailOnCreateTest2()
        {
            Uri uri = new Uri("failover:(mock://localhost:61616?transport.failOnCreate=true)?" +
                              "transport.useExponentialBackOff=false&transport.startupMaxReconnectAttempts=3&transport.initialReconnectDelay=100");
            FailoverTransportFactory factory = new FailoverTransportFactory();

            using(ITransport transport = factory.CreateTransport(uri))
            {
                Assert.IsNotNull(transport);
                transport.Command = OnCommand;
                transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

                FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
                Assert.IsNotNull(failover);
                Assert.IsFalse(failover.UseExponentialBackOff);
                Assert.AreEqual(3, failover.StartupMaxReconnectAttempts);
                Assert.AreEqual(100, failover.InitialReconnectDelay);

                transport.Start();
                Thread.Sleep(2000);
                Assert.IsNotEmpty(this.exceptions);
                Assert.IsFalse(failover.IsConnected);
            }
        }

		[Test]
		public void FailoverTransportFailOnSendMessageTest()
		{
			Uri uri = new Uri("failover:(mock://localhost:61616?transport.failOnCreate=true)?" +
							  "transport.useExponentialBackOff=false&transport.maxReconnectAttempts=3&transport.initialReconnectDelay=100");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.UseExponentialBackOff);
				Assert.AreEqual(3, failover.MaxReconnectAttempts);
				Assert.AreEqual(100, failover.InitialReconnectDelay);

				transport.Start();

				ActiveMQMessage message = new ActiveMQMessage();
				Assert.Throws<IOException>(delegate() { transport.Oneway(message); }, "Oneway call should block and then throw.");

				Assert.IsNotEmpty(this.exceptions);
				Assert.IsFalse(failover.IsConnected);
			}
		}

		[Test]
		public void FailoverTransportFailingBackupsTest()
		{
			Uri uri = new Uri(
				"failover:(mock://localhost:61616," +
						  "mock://localhost:61618?transport.failOnCreate=true)?transport.randomize=false&transport.backup=true");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize);
				Assert.IsTrue(failover.Backup);

				transport.Start();
				Thread.Sleep(2000);
				Assert.IsTrue(failover.IsConnected);
			}
		}

		[Test]
		public void FailoverTransportSendOnewayMessageTest()
		{
			int numMessages = 1000;
			Uri uri = new Uri("failover:(mock://localhost:61616)?transport.randomize=false");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize);

				failover.Resumed = OnResumed;

				transport.Start();
				Thread.Sleep(1000);
				Assert.IsTrue(failover.IsConnected);

				// Ensure the current mock transport has the correct outgoing command handler
				MockTransport mock = transport.Narrow(typeof(MockTransport)) as MockTransport;
				Assert.IsNotNull(mock);
				Assert.AreEqual(61616, mock.RemoteAddress.Port);

				VerifyCommandHandlerSetting(transport, mock);
				mock.OutgoingCommand = OnOutgoingCommand;

				ActiveMQMessage message = new ActiveMQMessage();
				for(int i = 0; i < numMessages; ++i)
				{
					transport.Oneway(message);
				}

				Thread.Sleep(1000);
				Assert.AreEqual(numMessages, this.sent.Count);
			}
		}

		[Test]
		public void FailoverTransportSendRequestTest()
		{
			Uri uri = new Uri("failover:(mock://localhost:61616)?transport.randomize=false");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize);

				failover.Resumed = OnResumed;

				transport.Start();
				Thread.Sleep(1000);
				Assert.IsTrue(failover.IsConnected);

				// Ensure the current mock transport has the correct outgoing command handler
				MockTransport mock = transport.Narrow(typeof(MockTransport)) as MockTransport;
				Assert.IsNotNull(mock);
				Assert.AreEqual(61616, mock.RemoteAddress.Port);
				VerifyCommandHandlerSetting(transport, mock);
				mock.OutgoingCommand = OnOutgoingCommand;

				ActiveMQMessage message = new ActiveMQMessage();
				int numMessages = 4;

				for(int i = 0; i < numMessages; ++i)
				{
					transport.Request(message);
				}

				Thread.Sleep(1000);
				Assert.AreEqual(numMessages, this.sent.Count);
			}
		}

		[Test]
		public void FailoverTransportSendOnewayFailTest()
		{
			Uri uri = new Uri(
				"failover:(mock://localhost:61616?transport.failOnSendMessage=true," +
						  "mock://localhost:61618)?transport.randomize=false");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize);

				failover.Resumed = OnResumed;

				transport.Start();
				Thread.Sleep(1000);
				Assert.IsTrue(failover.IsConnected);

				// Ensure the current mock transport has the correct outgoing command handler
				MockTransport mock = transport.Narrow(typeof(MockTransport)) as MockTransport;
				Assert.IsNotNull(mock);
				Assert.AreEqual(61616, mock.RemoteAddress.Port);
				VerifyCommandHandlerSetting(transport, mock);
				mock.OutgoingCommand = OnOutgoingCommand;

				ActiveMQMessage message = new ActiveMQMessage();
				int numMessages = 4;

				for(int i = 0; i < numMessages; ++i)
				{
					transport.Oneway(message);
					// Make sure we switched to second failover
					mock = transport.Narrow(typeof(MockTransport)) as MockTransport;
					Assert.IsNotNull(mock);
					Assert.AreEqual(61618, mock.RemoteAddress.Port);
				}

				Thread.Sleep(1000);
				Assert.AreEqual(numMessages, this.sent.Count);
			}
		}

		[Test]
		public void FailoverTransportSendOnewayTimeoutTest()
		{
			Uri uri = new Uri(
				"failover:(mock://localhost:61616?transport.failOnCreate=true)?transport.timeout=1000");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.AreEqual(1000, failover.Timeout);

				transport.Start();
				Thread.Sleep(1000);

				ActiveMQMessage message = new ActiveMQMessage();
				Assert.Throws<IOException>(delegate() { transport.Oneway(message); });
			}
		}

		[Test]
		public void FailoverTransportSendRequestFailTest()
		{
			Uri uri = new Uri(
				"failover:(mock://localhost:61616?transport.failOnSendMessage=true," +
						  "mock://localhost:61618)?transport.randomize=false");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize);

				failover.Resumed = OnResumed;

				transport.Start();
				Thread.Sleep(1000);
				Assert.IsTrue(failover.IsConnected);

				// Ensure the current mock transport has the correct outgoing command handler
				MockTransport mock = transport.Narrow(typeof(MockTransport)) as MockTransport;
				Assert.IsNotNull(mock);
				VerifyCommandHandlerSetting(transport, mock);
				mock.OutgoingCommand = OnOutgoingCommand;

				ActiveMQMessage message = new ActiveMQMessage();
				int numMessages = 4;

				for(int i = 0; i < numMessages; ++i)
				{
					transport.Request(message);
				}

				Thread.Sleep(1000);
				Assert.AreEqual(numMessages, this.sent.Count);
			}
		}

		[Test]
		public void TestFailoverTransportConnectionControlHandling()
		{
			Uri uri = new Uri("failover:(mock://localhost:61613)?transport.randomize=false");
			string connectedBrokers = "mock://localhost:61616?transport.name=Reconnected," +
									  "mock://localhost:61617?transport.name=Broker1," +
									  "mock://localhost:61618?transport.name=Broker2";
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize);

				const int MAX_ATTEMPTS = 50;

				transport.Start();
				
				for(int i = 0; i < MAX_ATTEMPTS; ++i)
				{
					if(failover.IsConnected)
					{
						break;
					}
					
					Thread.Sleep(100);
				}
				
				Assert.IsTrue(failover.IsConnected);

				// Ensure the current mock transport has the correct outgoing command handler
				MockTransport mock = transport.Narrow(typeof(MockTransport)) as MockTransport;
				Assert.IsNotNull(mock);
				Assert.AreEqual(61613, mock.RemoteAddress.Port);
				VerifyCommandHandlerSetting(transport, mock);
				mock.OutgoingCommand = OnOutgoingCommand;

				mock.InjectCommand(new ConnectionControl()
				{
					FaultTolerant = true,
					ConnectedBrokers = connectedBrokers,
					RebalanceConnection = true
				});

				// Give a bit of time for the Command to actually be processed.
				Thread.Sleep(2000);

				mock = null;
				
				for(int i = 0; i < MAX_ATTEMPTS; ++i)
				{
					mock = transport.Narrow(typeof(MockTransport)) as MockTransport;
					if(mock != null)
					{
						break;
					}
					
					Thread.Sleep(100);
				}
				
				Assert.IsNotNull(mock, "Error reconnecting to failover broker.");
				Assert.AreEqual(61616, mock.RemoteAddress.Port);
				Assert.AreEqual("Reconnected", mock.Name);
			}
		}

		[Test]
		public void TestPriorityBackupConfig() 
		{
		    Uri uri = new Uri("failover:(mock://localhost:61616,mock://localhost:61618)"+
			                  "?transport.randomize=false&transport.priorityBackup=true");

			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize, "Randomize should be false");
				Assert.IsTrue(failover.PriorityBackup, "Prioirity Backup not set.");

		    	transport.Start();

				for(int i = 0; i < MAX_ATTEMPTS; ++i)
				{
					if(failover.IsConnected)
					{
						break;
					}
					
					Thread.Sleep(100);
				}
				
				Assert.IsTrue(failover.IsConnected);
				Assert.IsTrue(failover.IsConnectedToPriority);
			}
		}

		[Test]
		public void TestPriorityBackupConfigPriorityURIsList() 
		{
		    Uri uri = new Uri("failover:(mock://localhost:61616,mock://localhost:61618)" +
			                  "?transport.randomize=false&transport.priorityBackup=true&" +
			                  "transport.priorityURIs=mock://localhost:61616,mock://localhost:61618");

			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize, "Randomize should be false");
				Assert.IsTrue(failover.PriorityBackup, "Prioirity Backup not set.");

				String priorityURIs = failover.PriorityURIs;
	            String[] tokens = priorityURIs.Split(new Char[] { ',' });
				Assert.AreEqual(2, tokens.Length, "Bad priorityURIs string: " + priorityURIs);

		    	transport.Start();

				for(int i = 0; i < MAX_ATTEMPTS; ++i)
				{
					if(failover.IsConnected)
					{
						break;
					}
					
					Thread.Sleep(100);
				}
				
				Assert.IsTrue(failover.IsConnected);
				Assert.IsTrue(failover.IsConnectedToPriority);
			}
		}

		[Test]
		public void OpenWireCommandsTest()
		{
			Uri uri = new Uri("failover:(mock://localhost:61616)?transport.randomize=false");
			FailoverTransportFactory factory = new FailoverTransportFactory();

			using(ITransport transport = factory.CreateTransport(uri))
			{
				Assert.IsNotNull(transport);
				transport.Command = OnCommand;
				transport.Exception = OnException;
				transport.Resumed = OnResumed;
				transport.Interrupted = OnInterrupted;

				FailoverTransport failover =  transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				Assert.IsNotNull(failover);
				Assert.IsFalse(failover.Randomize);

				transport.Start();
				Thread.Sleep(1000);
				Assert.IsTrue(failover.IsConnected);

				ConnectionInfo connection = createConnection();
				transport.Request(connection);
				SessionInfo session1 = createSession(connection);
				transport.Request(session1);
				SessionInfo session2 = createSession(connection);
				transport.Request(session2);
				ConsumerInfo consumer1 = createConsumer(session1);
				transport.Request(consumer1);
				ConsumerInfo consumer2 = createConsumer(session1);
				transport.Request(consumer2);
				ConsumerInfo consumer3 = createConsumer(session2);
				transport.Request(consumer3);

				ProducerInfo producer1 = createProducer(session2);
				transport.Request(producer1);

				// Remove the Producers
				disposeOf(transport, producer1);

				// Remove the Consumers
				disposeOf(transport, consumer1);
				disposeOf(transport, consumer2);
				disposeOf(transport, consumer3);

				// Remove the Session instances.
				disposeOf(transport, session1);
				disposeOf(transport, session2);

				// Indicate that we are done.
				ShutdownInfo shutdown = new ShutdownInfo();
				transport.Oneway(shutdown);
			}
		}

		protected ConnectionInfo createConnection()
		{
			return new ConnectionInfo()
			{
				ClientId = Guid.NewGuid().ToString(),
				ConnectionId = new ConnectionId()
				{
					Value = Guid.NewGuid().ToString()
				}
			};
		}

		protected SessionInfo createSession(ConnectionInfo parent)
		{
			return new SessionInfo()
			{
				SessionId = new SessionId()
				{
					ConnectionId = parent.ConnectionId.Value,
					Value = sessionIdx++
				}
			};
		}

		protected ConsumerInfo createConsumer(SessionInfo parent)
		{
			return new ConsumerInfo()
			{
				ConsumerId = new ConsumerId()
				{
					ConnectionId = parent.SessionId.ConnectionId,
					SessionId = parent.SessionId.Value,
					Value = consumerIdx++
				}
			};
		}

		protected ProducerInfo createProducer(SessionInfo parent)
		{
			return new ProducerInfo()
			{
				ProducerId = new ProducerId()
				{
					ConnectionId = parent.SessionId.ConnectionId,
					SessionId = parent.SessionId.Value,
					Value = producerIdx++
				}
			};
		}

		protected void disposeOf(ITransport transport, SessionInfo session)
		{
			transport.Oneway(new RemoveInfo() { ObjectId = session.SessionId });
		}

		protected void disposeOf(ITransport transport, ConsumerInfo consumer)
		{
			transport.Oneway(new RemoveInfo() { ObjectId = consumer.ConsumerId });
		}

		protected void disposeOf(ITransport transport, ProducerInfo producer)
		{
			transport.Oneway(new RemoveInfo() { ObjectId = producer.ProducerId });
		}

        [Test]
        public void FailoverTransportFailOnProcessingReceivedMessageTest()
        {
            string uri = "failover:(tcp://${activemqhost}:61616)";
            IConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri));
            using(connection = factory.CreateConnection() as Connection )
            {
                connection.ConnectionInterruptedListener +=
                    new ConnectionInterruptedListener(TransportInterrupted);
                connection.ConnectionResumedListener +=
                    new ConnectionResumedListener(TransportResumed);

                connection.Start();
                using(ISession session = connection.CreateSession())
                {
                    IDestination destination = session.GetQueue("Test?consumer.prefetchSize=1");
                    PurgeQueue(connection, destination);
                    PutMsgIntoQueue(session, destination);

                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    {
                        consumer.Listener += OnMessage;
                        BreakConnection();
                        WaitForMessagesToArrive();
                    }
                }
            }

            Assert.IsTrue(this.interrupted);
            Assert.IsTrue(this.resumed);
        }

		[Test]
		public void FailStartupMaxReconnectAttempts()
		{
			// Connect to valid machine, but on invalid port that doesn't have a broker listening.
			string uri = "failover:(tcp://localhost:31313)?transport.StartupMaxReconnectAttempts=3";
			IConnectionFactory factory = new ConnectionFactory(NMSTestSupport.ReplaceEnvVar(uri));
			IConnection failConnection = factory.CreateConnection();
			try
			{
				failConnection.Start();
				Assert.Fail("Should not have connected to broker.");
			}
			catch(Apache.NMS.ActiveMQ.ConnectionClosedException)
			{
			}
			catch(Apache.NMS.NMSConnectionException)
			{
			}
			catch(Exception e)
			{
                Assert.Fail("Wrong Exception Thrown after max reconnect attempts.\n" +
				            "Exception thrown type: " + e.GetType());
			}
			finally
			{
				try
				{
					failConnection.Stop();
				}
				catch(Exception)
				{
                    Assert.Fail("Connection closed exception thrown while closing a connection.");
				}
				finally
				{
					try 
					{
						failConnection.Dispose();
					}
					catch(Exception)
					{
	                    Assert.Fail("Connection closed exception thrown while closing a connection.");
					}
				}
			}
		}
		
		public void TransportInterrupted()
        {
            this.interrupted = true;
        }

        public void TransportResumed()
        {
            this.resumed = true;
        }

        public void OnMessage(IMessage message)
        {
            var textMsg = message as ITextMessage;

            if(textMsg == null)
            {
                return;
            }

            msgCount--;

            // just process the first message for 10 seconds to give some time main thread
            // to restart ActiveMq broker
            if(msgCount == MESSAGE_COUNT - 1)
            {
                Thread.Sleep(10000);
            }

            if(msgCount == 0)
            {
                // if all messages were consumed then we are fine
                semaphore.Set();
            }
        }

        private void PutMsgIntoQueue(ISession session, IDestination destination)
        {
            using(IMessageProducer producer = session.CreateProducer(destination))
            {
                ITextMessage message = session.CreateTextMessage();
                for(int i = 0; i < msgCount; ++i)
                {
                    message.Text = "Test message " + (i + 1);
                    producer.Send(message);
                }
            }
        }

        public void PurgeQueue(IConnection conn, IDestination queue)
        {
            ISession session = conn.CreateSession();
            IMessageConsumer consumer = session.CreateConsumer(queue);
            while(consumer.Receive(TimeSpan.FromMilliseconds(500)) != null)
            {
            }
            consumer.Close();
            session.Close();
        }

        private void BreakConnection()
        {
            TcpTransport transport = this.connection.ITransport.Narrow(typeof(TcpTransport)) as TcpTransport;
            Assert.IsNotNull(transport);
            transport.Close();
        }

        protected void WaitForMessagesToArrive()
        {
            semaphore.WaitOne(30000, true);
            Assert.AreEqual(0, msgCount);
        }
	}
}
