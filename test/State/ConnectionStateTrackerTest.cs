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
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.State;
using Apache.NMS.ActiveMQ.Transport;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
	public class ConnectionStateTrackerTest
	{
	    class TrackingTransport : ITransport 
		{
	        public LinkedList<Command> connections = new LinkedList<Command>();
	        public LinkedList<Command> sessions = new LinkedList<Command>();
	        public LinkedList<Command> producers = new LinkedList<Command>();
	        public LinkedList<Command> consumers = new LinkedList<Command>();
	        public LinkedList<Command> messages = new LinkedList<Command>();
	        public LinkedList<Command> messagePulls = new LinkedList<Command>();

			public FutureResponse AsyncRequest(Command command)
			{
				return null;
			}

			public Response Request(Command command)
			{
				return null;
			}

			public Response Request(Command command, TimeSpan timeout)
			{
				return null;
			}

        	public Object Narrow(Type type)
			{
				return null;
			}

	        public void Start() 
			{
			}

			public bool IsStarted
			{
				get { return true; }
			}

	        public void Stop() 
			{
			}

	        public void Dispose() 
			{
			}

	        public void Oneway(Command command) 
			{
	            if (command.IsConnectionInfo) 
				{
	                connections.AddLast(command);
	            }
				else if (command.IsSessionInfo) 
				{
	                sessions.AddLast(command);
	            }
				else if (command.IsProducerInfo) 
				{
	                producers.AddLast(command);
	            }
				else if (command.IsConsumerInfo) 
				{
	                consumers.AddLast(command);
	            }
				else if (command.IsMessage) 
				{
	                messages.AddLast(command);
	            }
				else if (command.IsMessagePull) 
				{
	                messagePulls.AddLast(command);
	            }
	        }

			public int Timeout
			{
				get { return 0; }
				set {}
			}

			public int AsyncTimeout
			{
				get { return 0; }
				set {}
			}

			public CommandHandler Command
			{
				get { return null; }
				set {}
			}

			public ExceptionHandler Exception
			{
				get { return null; }
				set {}
			}

			public InterruptedHandler Interrupted
			{
				get { return null; }
				set {}
			}

			public ResumedHandler Resumed
			{
				get { return null; }
				set {}
			}

			public bool IsDisposed
			{
				get { return false; }
			}

	        public bool IsFaultTolerant
	        {
				get { return false; }
	        }

	        public bool IsConnected
	        {
				get { return false; }
	        }

	        public Uri RemoteAddress
	        {
				get { return null; }
	        }

		    public bool IsReconnectSupported
			{
				get { return false; }
			}

		    public bool IsUpdateURIsSupported
			{
				get { return false; }
			}

			public void UpdateURIs(bool rebalance, Uri[] updatedURIs)
			{
			}

	        public IWireFormat WireFormat
	        {
				get { return null; }
	        }

	    };

	    class ConnectionData 
		{
	        public ConnectionInfo connection;
	        public SessionInfo session;
	        public ConsumerInfo consumer;
	        public ProducerInfo producer;
	    };

	    private ConnectionData CreateConnectionState(ConnectionStateTracker tracker) 
		{
	        ConnectionData conn = new ConnectionData();

	        ConnectionId connectionId = new ConnectionId();
	        connectionId.Value = "CONNECTION";
	        conn.connection = new ConnectionInfo();
	        conn.connection.ConnectionId = connectionId;

	        SessionId sessionId = new SessionId();
	        sessionId.ConnectionId = "CONNECTION";
	        sessionId.Value = 12345;
	        conn.session = new SessionInfo();
	        conn.session.SessionId = sessionId;

	        ConsumerId consumerId = new ConsumerId();
	        consumerId.ConnectionId = "CONNECTION";
	        consumerId.SessionId = 12345;
	        consumerId.Value = 42;
	        conn.consumer = new ConsumerInfo();
	        conn.consumer.ConsumerId = consumerId;

	        ProducerId producerId = new ProducerId();
	        producerId.ConnectionId = "CONNECTION";
	        producerId.SessionId = 12345;
	        producerId.Value = 42;

	        conn.producer = new ProducerInfo();
	        conn.producer.ProducerId = producerId;

	        tracker.ProcessAddConnection(conn.connection);
	        tracker.ProcessAddSession(conn.session);
	        tracker.ProcessAddConsumer(conn.consumer);
	        tracker.ProcessAddProducer(conn.producer);

	        return conn;
	    }

	    void ClearConnectionState(ConnectionStateTracker tracker, ConnectionData conn) 
		{
	        tracker.ProcessRemoveProducer(conn.producer.ProducerId);
	        tracker.ProcessRemoveConsumer(conn.consumer.ConsumerId);
	        tracker.ProcessRemoveSession(conn.session.SessionId);
	        tracker.ProcessRemoveConnection(conn.connection.ConnectionId);
	    }

		[SetUp]
		public void SetUp()
		{
		}

		[Test]
		public void TestConnectionStateTracker()
		{
			ConnectionStateTracker tracker = new ConnectionStateTracker();
		    ConnectionData conn = CreateConnectionState(tracker);
		    ClearConnectionState(tracker, conn);
		}

		[Test]
		public void TestMessageCache()
		{
		    TrackingTransport transport = new TrackingTransport();
		    ConnectionStateTracker tracker = new ConnectionStateTracker();
		    tracker.TrackMessages = true;

		    ConnectionData conn = CreateConnectionState(tracker);

			tracker.MaxCacheSize = 4;

		    int sequenceId = 1;

		    for (int i = 0; i < 10; ++i) 
			{
		        MessageId id = new MessageId();
		        id.ProducerId = conn.producer.ProducerId;
		        id.ProducerSequenceId = sequenceId++;
		        Message message = new Message();
		        message.MessageId = id;

		        tracker.ProcessMessage(message);
		        tracker.TrackBack(message);
		    }

		    tracker.DoRestore(transport);

			Assert.AreEqual(4, transport.messages.Count);
		}

		[Test]
		public void TestMessagePullCache()
		{
		    TrackingTransport transport = new TrackingTransport();
		    ConnectionStateTracker tracker = new ConnectionStateTracker();
		    tracker.TrackMessages = true;

			tracker.MaxCacheSize = 10;
		    ConnectionData conn = CreateConnectionState(tracker);

		    for (int i = 0; i < 100; ++i) 
			{
		        MessagePull pull = new MessagePull();
		        ActiveMQDestination destination = new ActiveMQTopic("TEST" + i);
		        pull.ConsumerId = conn.consumer.ConsumerId;
		        pull.Destination = destination;
		        tracker.ProcessMessagePull(pull);
				tracker.TrackBack(pull);
		    }

		    tracker.DoRestore(transport);

		    Assert.AreEqual(10, transport.messagePulls.Count);
		}

		[Test]
		public void TestMessagePullCache2()
		{
		    TrackingTransport transport = new TrackingTransport();
		    ConnectionStateTracker tracker = new ConnectionStateTracker();
		    tracker.TrackMessages = true;

			tracker.MaxCacheSize = 10;
		    ConnectionData conn = CreateConnectionState(tracker);

		    for (int i = 0; i < 100; ++i) 
			{
		        MessagePull pull = new MessagePull();
		        ActiveMQDestination destination = new ActiveMQTopic("TEST");
		        pull.ConsumerId = conn.consumer.ConsumerId;
		        pull.Destination = destination;
		        tracker.ProcessMessagePull(pull);
				tracker.TrackBack(pull);
		    }

		    tracker.DoRestore(transport);

		    Assert.AreEqual(1, transport.messagePulls.Count);
		}
	}
}

