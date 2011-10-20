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
using System.Diagnostics;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Threads;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Failover;
using Apache.NMS.ActiveMQ.Util;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ
{
	/// <summary>
	/// Represents a connection with a message broker
	/// </summary>
	public class Connection : IConnection
	{
		private static readonly IdGenerator CONNECTION_ID_GENERATOR = new IdGenerator();

		// Uri configurable options.
		private AcknowledgementMode acknowledgementMode = AcknowledgementMode.AutoAcknowledge;
		private bool asyncSend = false;
		private bool alwaysSyncSend = false;
		private bool asyncClose = true;
		private bool useCompression = false;
		private bool copyMessageOnSend = true;
		private bool sendAcksAsync = false;
		private bool dispatchAsync = true;
		private int producerWindowSize = 0;
		private bool messagePrioritySupported=true;
        private bool watchTopicAdviosires = true;

		private bool userSpecifiedClientID;
		private readonly Uri brokerUri;
		private ITransport transport;
		private readonly ConnectionInfo info;
		private TimeSpan requestTimeout; // from connection factory
		private BrokerInfo brokerInfo; // from broker
		private readonly CountDownLatch brokerInfoReceived = new CountDownLatch(1);
		private WireFormatInfo brokerWireFormatInfo; // from broker
		private readonly IList sessions = ArrayList.Synchronized(new ArrayList());
		private readonly IDictionary producers = Hashtable.Synchronized(new Hashtable());
		private readonly IDictionary dispatchers = Hashtable.Synchronized(new Hashtable());
        private readonly IDictionary tempDests = Hashtable.Synchronized(new Hashtable());
		private readonly object connectedLock = new object();
		private readonly Atomic<bool> connected = new Atomic<bool>(false);
		private readonly Atomic<bool> closed = new Atomic<bool>(false);
		private readonly Atomic<bool> closing = new Atomic<bool>(false);
		private readonly Atomic<bool> transportFailed = new Atomic<bool>(false);
		private Exception firstFailureError = null;
		private int sessionCounter = 0;
		private int temporaryDestinationCounter = 0;
		private int localTransactionCounter;
		private readonly Atomic<bool> started = new Atomic<bool>(false);
		private ConnectionMetaData metaData = null;
		private bool disposed = false;
		private IRedeliveryPolicy redeliveryPolicy;
		private PrefetchPolicy prefetchPolicy = new PrefetchPolicy();
		private ICompressionPolicy compressionPolicy = new CompressionPolicy();
		private readonly IdGenerator clientIdGenerator;
        private int consumerIdCounter = 0;
		private volatile CountDownLatch transportInterruptionProcessingComplete;
		private readonly MessageTransformation messageTransformation;
		private readonly ThreadPoolExecutor executor = new ThreadPoolExecutor();
        private AdvisoryConsumer advisoryConsumer = null;

		public Connection(Uri connectionUri, ITransport transport, IdGenerator clientIdGenerator)
		{
			this.brokerUri = connectionUri;
			this.clientIdGenerator = clientIdGenerator;

			this.transport = transport;
			this.transport.Command = new CommandHandler(OnCommand);
			this.transport.Exception = new ExceptionHandler(OnTransportException);
			this.transport.Interrupted = new InterruptedHandler(OnTransportInterrupted);
			this.transport.Resumed = new ResumedHandler(OnTransportResumed);

			ConnectionId id = new ConnectionId();
			id.Value = CONNECTION_ID_GENERATOR.GenerateId();

			this.info = new ConnectionInfo();
			this.info.ConnectionId = id;
			this.info.FaultTolerant = transport.IsFaultTolerant;

			this.messageTransformation = new ActiveMQMessageTransformation(this);
		}

		~Connection()
		{
			Dispose(false);
		}

		/// <summary>
		/// A delegate that can receive transport level exceptions.
		/// </summary>
		public event ExceptionListener ExceptionListener;

		/// <summary>
		/// An asynchronous listener that is notified when a Fault tolerant connection
		/// has been interrupted.
		/// </summary>
		public event ConnectionInterruptedListener ConnectionInterruptedListener;

		/// <summary>
		/// An asynchronous listener that is notified when a Fault tolerant connection
		/// has been resumed.
		/// </summary>
		public event ConnectionResumedListener ConnectionResumedListener;

		private ConsumerTransformerDelegate consumerTransformer;
		public ConsumerTransformerDelegate ConsumerTransformer
		{
			get { return this.consumerTransformer; }
			set { this.consumerTransformer = value; }
		}

		private ProducerTransformerDelegate producerTransformer;
		public ProducerTransformerDelegate ProducerTransformer
		{
			get { return this.producerTransformer; }
			set { this.producerTransformer = value; }
		}

		#region Properties

		public String UserName
		{
			get { return this.info.UserName; }
			set { this.info.UserName = value; }
		}

		public String Password
		{
			get { return this.info.Password; }
			set { this.info.Password = value; }
		}

		/// <summary>
		/// This property indicates what version of the Protocol we are using to
		/// communicate with the Broker, if not set we return the lowest version
		/// number to indicate we support only the basic command set.
		/// </summary>
		public int ProtocolVersion
		{
			get
			{
				if(brokerWireFormatInfo != null)
				{
					return brokerWireFormatInfo.Version;
				}

				return 1;
			}
		}

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
		/// This property indicates whether or not async sends are used for
		/// message acknowledgement messages.  Sending Acks async can improve
		/// performance but may decrease reliability.
		/// </summary>
		public bool SendAcksAsync
		{
			get { return sendAcksAsync; }
			set { sendAcksAsync = value; }
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

		/// <summary>
		/// This property is the maximum number of bytes in memory that a producer will transmit
		/// to a broker before waiting for acknowledgement messages from the broker that it has
		/// accepted the previously sent messages. In other words, this how you configure the
		/// producer flow control window that is used for async sends where the client is responsible
		/// for managing memory usage. The default value of 0 means no flow control at the client
		/// </summary>
		public int ProducerWindowSize
		{
			get { return producerWindowSize; }
			set { producerWindowSize = value; }
		}

		/// <summary>
		/// This property forces all messages that are sent to be sent synchronously overriding
		/// any usage of the AsyncSend flag. This can reduce performance in some cases since the
		/// only messages we normally send synchronously are Persistent messages not sent in a
		/// transaction. This options guarantees that no send will return until the broker has
		/// acknowledge receipt of the message
		/// </summary>
		public bool AlwaysSyncSend
		{
			get { return alwaysSyncSend; }
			set { alwaysSyncSend = value; }
		}

		/// <summary>
		/// This property indicates whether Message's should be copied before being sent via
		/// one of the Connection's send methods.  Copying the Message object allows the user
		/// to resuse the Object over for another send.  If the message isn't copied performance
		/// can improve but the user must not reuse the Object as it may not have been sent
		/// before they reset its payload.
		/// </summary>
		public bool CopyMessageOnSend
		{
			get { return copyMessageOnSend; }
			set { copyMessageOnSend = value; }
		}

		/// <summary>
		/// Enable or Disable the use of Compression on Message bodies.  When enabled all
		/// messages have their body compressed using the Deflate compression algorithm.
		/// The recipient of the message must support the use of message compression as well
		/// otherwise the receiving client will receive a message whose body appears in the
		/// compressed form.
		/// </summary>
		public bool UseCompression
		{
			get { return this.useCompression; }
			set { this.useCompression = value; }
		}

		/// <summary>
		/// Indicate whether or not the resources of this Connection should support the
		/// Message Priority value of incoming messages and dispatch them accordingly.
		/// When disabled Message are always dispatched to Consumers in FIFO order.
		/// </summary>
		public bool MessagePrioritySupported
		{
			get { return this.messagePrioritySupported; }
			set { this.messagePrioritySupported = value; }
		}

		public IConnectionMetaData MetaData
		{
			get { return this.metaData ?? (this.metaData = new ConnectionMetaData()); }
		}

		public Uri BrokerUri
		{
			get { return brokerUri; }
		}

		public ITransport ITransport
		{
			get { return transport; }
			set { this.transport = value; }
		}

		public bool TransportFailed
		{
			get { return this.transportFailed.Value; }
		}

		public Exception FirstFailureError
		{
			get { return this.firstFailureError; }
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

		/// <summary>
		/// synchronously or asynchronously by the broker.
		/// </summary>
		public bool DispatchAsync
		{
			get { return this.dispatchAsync; }
			set { this.dispatchAsync = value; }
		}

        public bool WatchTopicAdvisories
        {
            get { return this.watchTopicAdviosires; }
            set { this.watchTopicAdviosires = value; }
        }

		public string ClientId
		{
			get { return info.ClientId; }
			set
			{
				if(this.connected.Value)
				{
					throw new NMSException("You cannot change the ClientId once the Connection is connected");
				}

				this.info.ClientId = value;
				this.userSpecifiedClientID = true;
				CheckConnected();
			}
		}

		/// <summary>
		/// The Default Client Id used if the ClientId property is not set explicity.
		/// </summary>
		public string DefaultClientId
		{
			set
			{
				this.info.ClientId = value;
				this.userSpecifiedClientID = true;
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

		public String ResourceManagerId
		{
			get
			{
				this.brokerInfoReceived.await();
				return brokerInfo.BrokerId.Value;
			}
		}

		/// <summary>
		/// Get/or set the redelivery policy for this connection.
		/// </summary>
		public IRedeliveryPolicy RedeliveryPolicy
		{
			get { return this.redeliveryPolicy; }
			set { this.redeliveryPolicy = value; }
		}

		public PrefetchPolicy PrefetchPolicy
		{
			get { return this.prefetchPolicy; }
			set { this.prefetchPolicy = value; }
		}

		public ICompressionPolicy CompressionPolicy
		{
			get { return this.compressionPolicy; }
			set { this.compressionPolicy = value; }
		}

		internal MessageTransformation MessageTransformation
		{
			get { return this.messageTransformation; }
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
						session.Start();
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
						session.Stop();
					}
				}
			}
		}

		/// <summary>
		/// Creates a new session to work on this connection
		/// </summary>
		public ISession CreateSession()
		{
			return CreateAtiveMQSession(acknowledgementMode);
		}

		/// <summary>
		/// Creates a new session to work on this connection
		/// </summary>
		public ISession CreateSession(AcknowledgementMode sessionAcknowledgementMode)
		{
			return CreateAtiveMQSession(sessionAcknowledgementMode);
		}

		protected virtual Session CreateAtiveMQSession(AcknowledgementMode ackMode)
		{
			CheckConnected();
			return new Session(this, NextSessionId, ackMode);
		}

		internal void AddSession(Session session)
		{
			if(!this.closing.Value)
			{
				sessions.Add(session);
			}
		}

		internal void RemoveSession(Session session)
		{
			if(!this.closing.Value)
			{
				sessions.Remove(session);
			}
		}

		internal void addDispatcher( ConsumerId id, IDispatcher dispatcher )
		{
			if(!this.closing.Value)
			{
				this.dispatchers.Add( id, dispatcher );
			}
		}

		internal void removeDispatcher( ConsumerId id )
		{
			if(!this.closing.Value)
			{
				this.dispatchers.Remove( id );
			}
		}

		internal void addProducer( ProducerId id, MessageProducer producer )
		{
			if(!this.closing.Value)
			{
				this.producers.Add( id, producer );
			}
		}

		internal void removeProducer( ProducerId id )
		{
			if(!this.closing.Value)
			{
				this.producers.Remove( id );
			}
		}

		public void Close()
		{
			if(!this.closed.Value && !transportFailed.Value)
			{
				this.Stop();
			}

			lock(connectedLock)
			{
				if(this.closed.Value)
				{
					return;
				}

				try
				{
					Tracer.Info("Connection.Close(): Closing Connection Now.");
					this.closing.Value = true;

                    if(this.advisoryConsumer != null)
                    {
                        this.advisoryConsumer.Dispose();
                        this.advisoryConsumer = null;
                    }

					lock(sessions.SyncRoot)
					{
						foreach(Session session in sessions)
						{
							session.Shutdown();
						}
					}
					sessions.Clear();

                    foreach(ActiveMQTempDestination dest in this.tempDests.Values)
                    {
                        dest.Delete();
                    }

					// Connected is true only when we've successfully sent our ConnectionInfo
					// to the broker, so if we haven't announced ourselves there's no need to
					// inform the broker of a remove, and if the transport is failed, why bother.
					if(connected.Value && !transportFailed.Value)
					{
						DisposeOf(ConnectionId);
						ShutdownInfo shutdowninfo = new ShutdownInfo();
						transport.Oneway(shutdowninfo);
					}

					executor.Shutdown();

					Tracer.Info("Connection: Disposing of the Transport.");
					transport.Dispose();
				}
				catch(Exception ex)
				{
					Tracer.ErrorFormat("Error during connection close: {0}", ex);
				}
				finally
				{
					if(executor != null)
					{
						executor.Shutdown();
					}

					this.transport = null;
					this.closed.Value = true;
					this.connected.Value = false;
					this.closing.Value = false;
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
				Close();
			}
			catch
			{
				// Ignore network errors.
			}

			disposed = true;
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

		/// <summary>
		/// Performs a synchronous request-response with the broker for requested timeout duration.
		/// </summary>
		/// <param name="command"></param>
		/// <param name="requestTimeout"></param>
		/// <returns></returns>
		public Response SyncRequest(Command command, TimeSpan requestTimeout)
		{
			CheckConnected();

			try
			{
				Response response = transport.Request(command, requestTimeout);
				if(response is ExceptionResponse)
				{
					ExceptionResponse exceptionResponse = (ExceptionResponse) response;
					BrokerError brokerError = exceptionResponse.Exception;
					throw new BrokerException(brokerError);
				}
				return response;
			}
			catch(Exception ex)
			{
				throw NMSExceptionSupport.Create(ex);
			}
		}

		public void Oneway(Command command)
		{
			CheckConnected();

			try
			{
				transport.Oneway(command);
			}
			catch(Exception ex)
			{
				throw NMSExceptionSupport.Create(ex);
			}
		}

		private void DisposeOf(DataStructure objectId)
		{
			try
			{
				RemoveInfo command = new RemoveInfo();
				command.ObjectId = objectId;
				if(asyncClose)
				{
					Tracer.Info("Asynchronously disposing of Connection.");
					if(connected.Value)
					{
						transport.Oneway(command);
						Tracer.Info("Oneway command sent to broker.");
					}
				}
				else
				{
					// Ensure that the object is disposed to avoid potential race-conditions
					// of trying to re-create the same object in the broker faster than
					// the broker can dispose of the object.  Allow up to 5 seconds to process.
					Tracer.Info("Synchronously disposing of Connection.");
					SyncRequest(command, TimeSpan.FromSeconds(5));
					Tracer.Info("Synchronously closed Connection.");
				}
			}
			catch // (BrokerException)
			{
				// Ignore exceptions while shutting down.
			}
		}

		/// <summary>
		/// Check and ensure that the connection object is connected.  If it is not
		/// connected or is closed or closing, a ConnectionClosedException is thrown.
		/// </summary>
		internal void CheckConnected()
		{
			if(closed.Value)
			{
				throw new ConnectionClosedException();
			}

			if(!connected.Value)
			{
				DateTime timeoutTime = DateTime.Now + this.RequestTimeout;
				int waitCount = 1;

				while(true)
				{
					if(Monitor.TryEnter(connectedLock))
					{
						try
						{
							if(closed.Value || closing.Value)
							{
								break;
							}
							else if(!connected.Value)
							{
								if(!this.userSpecifiedClientID)
								{
									this.info.ClientId = this.clientIdGenerator.GenerateId();
								}

								try
								{
									if(null != transport)
									{
										// Send the connection and see if an ack/nak is returned.
										Response response = transport.Request(this.info, this.RequestTimeout);
										if(!(response is ExceptionResponse))
										{
											connected.Value = true;
                                            if(this.watchTopicAdviosires)
                                            {
                                                ConsumerId id = new ConsumerId(
                                                    new SessionId(info.ConnectionId, -1),
                                                    Interlocked.Increment(ref this.consumerIdCounter));
                                                this.advisoryConsumer = new AdvisoryConsumer(this, id);
                                            }
										}
									}
								}
								catch
								{
								}
							}
						}
						finally
						{
							Monitor.Exit(connectedLock);
						}
					}

					if(connected.Value || closed.Value || closing.Value || DateTime.Now > timeoutTime)
					{
						break;
					}

					// Back off from being overly aggressive.  Having too many threads
					// aggressively trying to connect to a down broker pegs the CPU.
					Thread.Sleep(5 * (waitCount++));
				}

				if(!connected.Value)
				{
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
			if(command.IsMessageDispatch)
			{
				WaitForTransportInterruptionProcessingToComplete();
				DispatchMessage((MessageDispatch) command);
			}
			else if(command.IsKeepAliveInfo)
			{
				OnKeepAliveCommand(commandTransport, (KeepAliveInfo) command);
			}
			else if(command.IsWireFormatInfo)
			{
				this.brokerWireFormatInfo = (WireFormatInfo) command;
			}
			else if(command.IsBrokerInfo)
			{
				this.brokerInfo = (BrokerInfo) command;
				this.brokerInfoReceived.countDown();
			}
			else if(command.IsShutdownInfo)
			{
				if(!closing.Value && !closed.Value)
				{
					OnException(new NMSException("Broker closed this connection."));
				}
			}
			else if(command.IsProducerAck)
			{
				ProducerAck ack = (ProducerAck) command as ProducerAck;
				if(ack.ProducerId != null)
				{
					MessageProducer producer = producers[ack.ProducerId] as MessageProducer;
					if(producer != null)
					{
						if(Tracer.IsDebugEnabled)
						{
							Tracer.Debug("Connection: Received a new ProducerAck -> " + ack);
						}

						producer.OnProducerAck(ack);
					}
				}
			}
			else if(command.IsConnectionError)
			{
				if(!closing.Value && !closed.Value)
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

					OnException(new NMSConnectionException(message, cause));
				}
			}
			else
			{
				Tracer.Error("Unknown command: " + command);
			}
		}

		protected void DispatchMessage(MessageDispatch dispatch)
		{
			lock(dispatchers.SyncRoot)
			{
				if(dispatchers.Contains(dispatch.ConsumerId))
				{
					IDispatcher dispatcher = (IDispatcher) dispatchers[dispatch.ConsumerId];

					// Can be null when a consumer has sent a MessagePull and there was
					// no available message at the broker to dispatch or when signalled
					// that the end of a Queue browse has been reached.
					if(dispatch.Message != null)
					{
						dispatch.Message.ReadOnlyBody = true;
						dispatch.Message.ReadOnlyProperties = true;
						dispatch.Message.RedeliveryCounter = dispatch.RedeliveryCounter;
					}

					dispatcher.Dispatch(dispatch);

					return;
				}
			}

			Tracer.Error("No such consumer active: " + dispatch.ConsumerId);
		}

		protected void OnKeepAliveCommand(ITransport commandTransport, KeepAliveInfo info)
		{
			Tracer.Debug("Keep alive message received.");

			try
			{
				if(connected.Value)
				{
					Tracer.Debug("Returning KeepAliveInfo Response.");
					info.ResponseRequired = false;
					transport.Oneway(info);
				}
			}
			catch(Exception ex)
			{
				if(!closing.Value && !closed.Value)
				{
					OnException(ex);
				}
			}
		}

		internal void OnAsyncException(Exception error)
		{
			if(!this.closed.Value && !this.closing.Value)
			{
				if(this.ExceptionListener != null)
				{
					if(!(error is NMSException))
					{
						error = NMSExceptionSupport.Create(error);
					}
					NMSException e = (NMSException) error;

					// Called in another thread so that processing can continue
					// here, ensures no lock contention.
					executor.QueueUserWorkItem(AsyncCallExceptionListener, e);
				}
				else
				{
					Tracer.Debug("Async exception with no exception listener: " + error);
				}
			}
		}

		private void AsyncCallExceptionListener(object error)
		{
			NMSException exception = error as NMSException;
			this.ExceptionListener(exception);
		}

		internal void OnTransportException(ITransport source, Exception cause)
		{
			this.OnException(cause);
		}

		internal void OnException(Exception error)
		{
			// Will fire an exception listener callback if there's any set.
			OnAsyncException(error);

			if(!this.closing.Value && !this.closed.Value)
			{
				// Perform the actual work in another thread to avoid lock contention
				// and allow the caller to continue on in its error cleanup.
				executor.QueueUserWorkItem(AsyncOnExceptionHandler, error);
			}
		}

		private void AsyncOnExceptionHandler(object error)
		{
			Exception cause = error as Exception;

			MarkTransportFailed(cause);

			try
			{
				this.transport.Dispose();
			}
			catch(Exception ex)
			{
				Tracer.Debug("Caught Exception While disposing of Transport: " + ex);
			}

			this.brokerInfoReceived.countDown();

			IList sessionsCopy = null;
			lock(this.sessions.SyncRoot)
			{
				sessionsCopy = new ArrayList(this.sessions);
			}

			// Use a copy so we don't concurrently modify the Sessions list if the
			// client is closing at the same time.
			foreach(Session session in sessionsCopy)
			{
				try
				{
					session.Shutdown();
				}
				catch(Exception ex)
				{
					Tracer.Debug("Caught Exception While disposing of Sessions: " + ex);
				}
			}
		}

		private void MarkTransportFailed(Exception error)
		{
			this.transportFailed.Value = true;
			if(this.firstFailureError == null)
			{
				this.firstFailureError = error;
			}
		}

		protected void OnTransportInterrupted(ITransport sender)
		{
			Tracer.Debug("Connection: Transport has been Interrupted.");

			this.transportInterruptionProcessingComplete = new CountDownLatch(dispatchers.Count);
			if(Tracer.IsDebugEnabled)
			{
				Tracer.Debug("transport interrupted, dispatchers: " + dispatchers.Count);
			}

			SignalInterruptionProcessingNeeded();

			foreach(Session session in this.sessions)
			{
				try
				{
					session.ClearMessagesInProgress();
				}
				catch(Exception ex)
				{
					Tracer.Warn("Exception while clearing messages: " + ex.Message);
					Tracer.Warn(ex.StackTrace);
				}
			}

			if(this.ConnectionInterruptedListener != null && !this.closing.Value)
			{
				try
				{
					this.ConnectionInterruptedListener();
				}
				catch
				{
				}
			}
		}

		protected void OnTransportResumed(ITransport sender)
		{
			Tracer.Debug("Transport has resumed normal operation.");

			if(this.ConnectionResumedListener != null && !this.closing.Value)
			{
				try
				{
					this.ConnectionResumedListener();
				}
				catch
				{
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

		protected SessionId NextSessionId
		{
			get { return new SessionId(this.info.ConnectionId, Interlocked.Increment(ref this.sessionCounter)); }
		}

		public ActiveMQTempDestination CreateTemporaryDestination(bool topic)
		{
			ActiveMQTempDestination destination = null;

			if(topic)
			{
				destination = new ActiveMQTempTopic(
					info.ConnectionId.Value + ":" + Interlocked.Increment(ref temporaryDestinationCounter));
			}
			else
			{
				destination = new ActiveMQTempQueue(
					info.ConnectionId.Value + ":" + Interlocked.Increment(ref temporaryDestinationCounter));
			}

			DestinationInfo command = new DestinationInfo();
			command.ConnectionId = ConnectionId;
			command.OperationType = DestinationInfo.ADD_OPERATION_TYPE; // 0 is add
			command.Destination = destination;

			this.SyncRequest(command);

			destination.Connection = this;
            this.AddTempDestination(destination);

			return destination;
		}

		protected void CreateTemporaryDestination(ActiveMQDestination tempDestination)
		{
		}

		public void DeleteTemporaryDestination(IDestination destination)
		{
            CheckClosedOrFailed();

            ActiveMQTempDestination temp = destination as ActiveMQTempDestination;

            foreach(Session session in this.sessions)
            {
                if(session.IsInUse(temp))
                {
                    throw new NMSException("A consumer is consuming from the temporary destination");
                }
            }

            this.tempDests.Remove(destination as ActiveMQTempDestination);
			this.DeleteDestination(destination);
		}

		public void DeleteDestination(IDestination destination)
		{
			DestinationInfo command = new DestinationInfo();
			command.ConnectionId = this.ConnectionId;
			command.OperationType = DestinationInfo.REMOVE_OPERATION_TYPE; // 1 is remove
			command.Destination = (ActiveMQDestination) destination;

			this.Oneway(command);
		}

		private void WaitForTransportInterruptionProcessingToComplete()
		{
			CountDownLatch cdl = this.transportInterruptionProcessingComplete;
			if(cdl != null)
			{
				if(!closed.Value && cdl.Remaining > 0)
				{
					Tracer.Warn("dispatch paused, waiting for outstanding dispatch interruption " +
								"processing (" + cdl.Remaining + ") to complete..");
					cdl.await(TimeSpan.FromSeconds(10));
				}

				SignalInterruptionProcessingComplete();
			}
		}

		internal void TransportInterruptionProcessingComplete()
		{
			CountDownLatch cdl = this.transportInterruptionProcessingComplete;
			if(cdl != null)
			{
				cdl.countDown();
				try
				{
					SignalInterruptionProcessingComplete();
				}
				catch
				{
				}
			}
		}

		private void SignalInterruptionProcessingComplete()
		{
			CountDownLatch cdl = this.transportInterruptionProcessingComplete;
			if(cdl.Remaining == 0)
			{
				if(Tracer.IsDebugEnabled)
				{
					Tracer.Debug("transportInterruptionProcessingComplete for: " + this.info.ConnectionId);
				}

				this.transportInterruptionProcessingComplete = null;

				FailoverTransport failoverTransport = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;
				if(failoverTransport != null)
				{
					failoverTransport.ConnectionInterruptProcessingComplete(this.info.ConnectionId);
					if(Tracer.IsDebugEnabled)
					{
						Tracer.Debug("notified failover transport (" + failoverTransport +
									 ") of interruption completion for: " + this.info.ConnectionId);
					}
				}
			}
		}

		private void SignalInterruptionProcessingNeeded()
		{
			FailoverTransport failoverTransport = transport.Narrow(typeof(FailoverTransport)) as FailoverTransport;

			if(failoverTransport != null)
			{
				failoverTransport.StateTracker.TransportInterrupted(this.info.ConnectionId);
				if(Tracer.IsDebugEnabled)
				{
					Tracer.Debug("notified failover transport (" + failoverTransport +
								 ") of pending interruption processing for: " + this.info.ConnectionId);
				}
			}
		}

        internal void AddTempDestination(ActiveMQTempDestination dest)
        {
            // .NET lacks a putIfAbsent operation for Maps.
            lock(tempDests.SyncRoot)
            {
                if(!this.tempDests.Contains(dest))
                {
                    this.tempDests.Add(dest, dest);
                }
            }
        }

        internal void RemoveTempDestination(ActiveMQTempDestination dest)
        {
            this.tempDests.Remove(dest);
        }

        internal bool IsTempDestinationActive(ActiveMQTempDestination dest)
        {
            if(this.advisoryConsumer == null)
            {
                return true;
            }

            return this.tempDests.Contains(dest);
        }

        protected void CheckClosedOrFailed()
        {
            CheckClosed();
            if (transportFailed.Value)
            {
                throw new ConnectionFailedException(firstFailureError.Message);
            }
        }

        protected void CheckClosed()
        {
            if(closed.Value)
            {
                throw new ConnectionClosedException();
            }
        }
	}
}
