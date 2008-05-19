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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Util;
using Apache.NMS;
using System;
using System.Collections;

namespace Apache.NMS.ActiveMQ
{
	/// <summary>
	/// Represents a connection with a message broker
	/// </summary>
	public class Connection : IConnection
	{
		private readonly Uri brokerUri;
		private ITransport transport;
		private readonly ConnectionInfo info;
		private AcknowledgementMode acknowledgementMode = AcknowledgementMode.AutoAcknowledge;
		private BrokerInfo brokerInfo; // from broker
		private WireFormatInfo brokerWireFormatInfo; // from broker
		private readonly IList sessions = ArrayList.Synchronized(new ArrayList());
		private bool asyncSend = false;
		private bool connected = false;
		private bool closed = false;
		private bool closing = false;
		private long sessionCounter = 0;
		private long temporaryDestinationCounter = 0;
		private long localTransactionCounter;
		private readonly AtomicBoolean started = new AtomicBoolean(true);
		private bool disposed = false;
		
		public Connection(Uri connectionUri, ITransport transport, ConnectionInfo info)
		{
			this.brokerUri = connectionUri;
			this.info = info;
			this.transport = transport;
			this.transport.Command = new CommandHandler(OnCommand);
			this.transport.Exception = new ExceptionHandler(OnException);
			this.transport.Start();
		}

		~Connection()
		{
			Dispose(false);
		}

		public event ExceptionListener ExceptionListener;


		public bool IsStarted
		{
			get { return started.Value; }
		}

		#region Properties

		/// <summary>
		/// This property indicates whether or not async send is enabled.
		/// </summary>
		public bool AsyncSend
		{
			get { return asyncSend; }
			set { asyncSend = value; }
		}

		/// <summary>
		/// This property sets the acknowledgment mode for the connection.
		/// The URI parameter connection.ackmode can be set to a string value
		/// that maps to the enumeration value.
		/// </summary>
		public string AckMode
		{
			set { this.acknowledgementMode = NMSConvert.ToAcknowledgementMode(value); }
		}

		#endregion

		/// <summary>
		/// Starts asynchronous message delivery of incoming messages for this connection.
		/// Synchronous delivery is unaffected.
		/// </summary>
		public void Start()
		{
			CheckConnected();
			if(started.CompareAndSet(false, true))
			{
				foreach(Session session in sessions)
				{
					session.StartAsyncDelivery(null);
				}
			}
		}

		/// <summary>
		/// Temporarily stop asynchronous delivery of inbound messages for this connection.
		/// The sending of outbound messages is unaffected.
		/// </summary>
		public void Stop()
		{
			CheckConnected();
			if(started.CompareAndSet(true, false))
			{
				foreach(Session session in sessions)
				{
					session.StopAsyncDelivery();
				}
			}
		}
		
		/// <summary>
		/// Creates a new session to work on this connection
		/// </summary>
		public ISession CreateSession()
		{
			return CreateSession(acknowledgementMode);
		}
		
		/// <summary>
		/// Creates a new session to work on this connection
		/// </summary>
		public ISession CreateSession(AcknowledgementMode sessionAcknowledgementMode)
		{
			SessionInfo info = CreateSessionInfo(sessionAcknowledgementMode);
			SyncRequest(info);
			Session session = new Session(this, info, sessionAcknowledgementMode);

			// Set properties on session using parameters prefixed with "session."
			System.Collections.Specialized.StringDictionary map = URISupport.ParseQuery(this.brokerUri.Query);
			URISupport.SetProperties(session, map, "session.");

			sessions.Add(session);
			return session;
		}

		public void RemoveSession(Session session)
		{
			DisposeOf(session.SessionId);

			if(!closing)
			{
				sessions.Remove(session);
			}
		}

		public void Close()
		{
			lock(this)
			{
				if(closed)
				{
					return;
				}

				try
				{
					closing = true;
					foreach(Session session in sessions)
					{
						session.Close();
					}
					sessions.Clear();

					DisposeOf(ConnectionId);
					transport.Oneway(new ShutdownInfo());
					transport.Dispose();
				}
				catch(Exception ex)
				{
					Tracer.ErrorFormat("Error during connection close: {0}", ex);
				}

				transport = null;
				closed = true;
				closing = false;
			}
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected void Dispose(bool disposing)
		{
			if(disposed)
			{
				return;
			}

			if(disposing)
			{
				// Dispose managed code here.
			}

			try
			{
				// For now we do not distinguish between Dispose() and Close().
				// In theory Dispose should possibly be lighter-weight and perform a (faster)
				// disorderly close.
				Close();
			}
			catch
			{
				// Ignore network errors.
			}

			disposed = true;
		}
		
		// Properties

		public Uri BrokerUri
		{
			get { return brokerUri; }
		}
		
		public ITransport ITransport
		{
			get { return transport; }
			set { this.transport = value; }
		}

		public AcknowledgementMode AcknowledgementMode
		{
			get { return acknowledgementMode; }
			set { this.acknowledgementMode = value; }
		}
		
		public string ClientId
		{
			get { return info.ClientId; }
			set
			{
				if(connected)
				{
					throw new NMSException("You cannot change the ClientId once the Connection is connected");
				}
				info.ClientId = value;
			}
		}
		
		public ConnectionId ConnectionId
		{
			get { return info.ConnectionId; }
		}
		
		public BrokerInfo BrokerInfo
		{
			get { return brokerInfo; }
		}
		
		public WireFormatInfo BrokerWireFormat
		{
			get { return brokerWireFormatInfo; }
		}
		
		// Implementation methods

		/// <summary>
		/// Performs a synchronous request-response with the broker
		/// </summary>
		public Response SyncRequest(Command command)
		{
			CheckConnected();
			Response response = transport.Request(command);
			if(response is ExceptionResponse)
			{
				ExceptionResponse exceptionResponse = (ExceptionResponse) response;
				BrokerError brokerError = exceptionResponse.Exception;
				throw new BrokerException(brokerError);
			}
			return response;
		}

		public void OneWay(Command command)
		{
			CheckConnected();
			transport.Oneway(command);
		}

		public void DisposeOf(DataStructure objectId)
		{
			RemoveInfo command = new RemoveInfo();
			command.ObjectId = objectId;
			// Ensure that the object is disposed to avoid potential race-conditions
			// of trying to re-create the same object in the broker faster than
			// the broker can dispose of the object.
			SyncRequest(command);
		}

		/// <summary>
		/// Creates a new temporary destination name
		/// </summary>
		public String CreateTemporaryDestinationName()
		{
			lock(this)
			{
				return info.ConnectionId.Value + ":" + (++temporaryDestinationCounter);
			}
		}
		
		/// <summary>
		/// Creates a new local transaction ID
		/// </summary>
		public LocalTransactionId CreateLocalTransactionId()
		{
			LocalTransactionId id= new LocalTransactionId();
			id.ConnectionId = ConnectionId;
			lock(this)
			{
				id.Value = (++localTransactionCounter);
			}
			return id;
		}
		
		protected void CheckConnected()
		{
			if(closed)
			{
				throw new ConnectionClosedException();
			}

			if(!connected)
			{
				connected = true;
				// now lets send the connection and see if we get an ack/nak
				if(null == SyncRequest(info))
				{
					closed = true;
					connected = false;
					throw new ConnectionClosedException();
				}
			}
		}
		
		/// <summary>
		/// Handle incoming commands
		/// </summary>
		/// <param name="commandTransport">An ITransport</param>
		/// <param name="command">A  Command</param>
		protected void OnCommand(ITransport commandTransport, Command command)
		{
			if(command is MessageDispatch)
			{
				DispatchMessage((MessageDispatch) command);
			}
			else if(command is WireFormatInfo)
			{
				this.brokerWireFormatInfo = (WireFormatInfo) command;
			}
			else if(command is BrokerInfo)
			{
				this.brokerInfo = (BrokerInfo) command;
			}
			else if(command is ShutdownInfo)
			{
				//ShutdownInfo info = (ShutdownInfo)command;
				if(!closing && !closed)
				{
					OnException(commandTransport, new NMSException("Broker closed this connection."));
				}
			}
			else
			{
				Tracer.Error("Unknown command: " + command);
			}
		}

		protected void DispatchMessage(MessageDispatch dispatch)
		{
			bool dispatched = false;

			foreach(Session session in sessions)
			{
				if(session.DispatchMessage(dispatch.ConsumerId, dispatch.Message))
				{
					dispatched = true;
					break;
				}
			}

			if(!dispatched)
			{
				Tracer.Error("No such consumer active: " + dispatch.ConsumerId);
			}
		}

		protected void OnException(ITransport sender, Exception exception)
		{
			Tracer.ErrorFormat("Transport Exception: {0}", exception.ToString());
			if(ExceptionListener != null)
			{
				try
				{
					ExceptionListener(exception);
				}
				catch
				{
					sender.Dispose();
				}
			}
		}

		internal void OnSessionException(Session sender, Exception exception)
		{
			Tracer.ErrorFormat("Session Exception: {0}", exception.ToString());
			if(ExceptionListener != null)
			{
				try
				{
					ExceptionListener(exception);
				}
				catch
				{
					sender.Close();
				}
			}
		}
		
		protected SessionInfo CreateSessionInfo(AcknowledgementMode sessionAcknowledgementMode)
		{
			SessionInfo answer = new SessionInfo();
			SessionId sessionId = new SessionId();
			sessionId.ConnectionId = info.ConnectionId.Value;
			lock(this)
			{
				sessionId.Value = ++sessionCounter;
			}
			answer.SessionId = sessionId;
			return answer;
		}
		
	}
}
