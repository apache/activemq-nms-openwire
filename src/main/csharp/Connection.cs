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
using System.Collections;
using System.Threading;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.Util;

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
		private TimeSpan requestTimeout;
		private BrokerInfo brokerInfo; // from broker
		private WireFormatInfo brokerWireFormatInfo; // from broker
		private readonly IList sessions = ArrayList.Synchronized(new ArrayList());
		/// <summary>
		/// Private object used for synchronization, instead of public "this"
		/// </summary>
		private readonly object myLock = new object();
		private bool asyncSend = false;
		private bool asyncClose = true;
		private bool connected = false;
		private bool closed = false;
		private bool closing = false;
		private int sessionCounter = 0;
		private int temporaryDestinationCounter = 0;
		private int localTransactionCounter;
		private readonly AtomicBoolean started = new AtomicBoolean(false);
		private bool disposed = false;

		public Connection(Uri connectionUri, ITransport transport, ConnectionInfo info)
		{
			this.brokerUri = connectionUri;
			this.info = info;
			this.requestTimeout = transport.RequestTimeout;
			this.transport = transport;
			this.transport.Command = new CommandHandler(OnCommand);
			this.transport.Exception = new ExceptionHandler(OnException);
		}

		~Connection()
		{
			Dispose(false);
		}

		public event ExceptionListener ExceptionListener;

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
		/// This property indicates whether or not async close is enabled.
		/// When the connection is closed, it will either send a synchronous
		/// DisposeOf command to the broker and wait for confirmation (if true),
		/// or it will send the DisposeOf command asynchronously.
		/// </summary>
		public bool AsyncClose
		{
			get { return asyncClose; }
			set { asyncClose = value; }
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
				lock(sessions.SyncRoot)
				{
					foreach(Session session in sessions)
					{
						session.StartAsyncDelivery();
					}
				}
			}
		}

		/// <summary>
		/// This property determines if the asynchronous message delivery of incoming
		/// messages has been started for this connection.
		/// </summary>
		public bool IsStarted
		{
			get { return started.Value; }
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
				lock(sessions.SyncRoot)
				{
					foreach(Session session in sessions)
					{
						session.StopAsyncDelivery();
					}
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
			SyncRequest(info, this.RequestTimeout);
			Session session = new Session(this, info, sessionAcknowledgementMode);

			// Set properties on session using parameters prefixed with "session."
			URISupport.CompositeData c = URISupport.parseComposite(this.brokerUri);
			URISupport.SetProperties(session, c.Parameters, "session.");

			if(IsStarted)
			{
				session.StartAsyncDelivery();
			}

			sessions.Add(session);
			return session;
		}

		public void RemoveSession(Session session)
		{
			DisposeOf(session.SessionId);

			if(!this.closing)
			{
				sessions.Remove(session);
			}
		}

		public void Close()
		{
			lock(myLock)
			{
				if(this.closed)
				{
					return;
				}

				try
				{
					this.closing = true;
					lock(sessions.SyncRoot)
					{
						foreach(Session session in sessions)
						{
							session.Close();
						}
					}
					sessions.Clear();

					if(connected)
					{
						DisposeOf(ConnectionId);
						transport.Oneway(new ShutdownInfo());
					}

					transport.Dispose();
				}
				catch(Exception ex)
				{
					Tracer.ErrorFormat("Error during connection close: {0}", ex);
				}
				finally
				{
					this.transport = null;
					this.closed = true;
					this.connected = false;
					this.closing = false;
				}
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

		public TimeSpan RequestTimeout
		{
			get { return this.requestTimeout; }
			set { this.requestTimeout = value; }
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
		///

		public Response SyncRequest(Command command)
		{
			return SyncRequest(command, this.RequestTimeout);
		}

		public Response SyncRequest(Command command, TimeSpan requestTimeout)
		{
			CheckConnected();
			Response response = transport.Request(command, requestTimeout);
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
			if(asyncClose)
			{
				Tracer.Info("Asynchronously closing Connection.");
				OneWay(command);
			}
			else
			{
				// Ensure that the object is disposed to avoid potential race-conditions
				// of trying to re-create the same object in the broker faster than
				// the broker can dispose of the object.  Allow up to 5 seconds to process.
				try
				{
					Tracer.Info("Synchronously closing Connection...");
					SyncRequest(command, TimeSpan.FromSeconds(5));
				}
				catch // (BrokerException)
				{
					// Ignore exceptions while shutting down.
				}
			}
		}

		/// <summary>
		/// Creates a new temporary destination name
		/// </summary>
		public String CreateTemporaryDestinationName()
		{
			return info.ConnectionId.Value + ":" + Interlocked.Increment(ref temporaryDestinationCounter);
		}

		/// <summary>
		/// Creates a new local transaction ID
		/// </summary>
		public LocalTransactionId CreateLocalTransactionId()
		{
			LocalTransactionId id = new LocalTransactionId();
			id.ConnectionId = ConnectionId;
			id.Value = Interlocked.Increment(ref localTransactionCounter);
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
			else if(command is KeepAliveInfo)
			{
				OnKeepAliveCommand(commandTransport, (KeepAliveInfo) command);
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
				if(!closing && !closed)
				{
					OnException(commandTransport, new NMSException("Broker closed this connection."));
				}
			}
			else if(command is ConnectionError)
			{
				if(!closing && !closed)
				{
					ConnectionError connectionError = (ConnectionError) command;
					BrokerError brokerError = connectionError.Exception;
					string message = "Broker connection error.";
					string cause = "";

					if(null != brokerError)
					{
						message = brokerError.Message;
						if(null != brokerError.Cause)
						{
							cause = brokerError.Cause.Message;
						}
					}

					OnException(commandTransport, new NMSConnectionException(message, cause));
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

            // Override the Message's Destination with the one from the Dispatch since in the
            // case of a virtual Topic the correct destination ack is the one from the Dispatch.
            // This is a bit of a hack since we should really be sending the entire dispatch to
            // the Consumer.
            dispatch.Message.Destination = dispatch.Destination;

			lock(sessions.SyncRoot)
			{
				foreach(Session session in sessions)
				{
					if(session.DispatchMessage(dispatch.ConsumerId, dispatch.Message))
					{
						dispatched = true;
						break;
					}
				}
			}

			if(!dispatched)
			{
				Tracer.Error("No such consumer active: " + dispatch.ConsumerId);
			}
		}

		protected void OnKeepAliveCommand(ITransport commandTransport, KeepAliveInfo info)
		{
			Tracer.Info("Keep alive message received.");
			if(info.ResponseRequired)
			{
				try
				{
					info.ResponseRequired = false;
					OneWay(info);
				}
				catch(Exception ex)
				{
					if(!closing && !closed)
					{
						OnException(commandTransport, ex);
					}
				}
			}
		}

		protected void OnException(ITransport sender, Exception exception)
		{
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
			sessionId.Value = Interlocked.Increment(ref sessionCounter);
			answer.SessionId = sessionId;
			return answer;
		}
	}
}
