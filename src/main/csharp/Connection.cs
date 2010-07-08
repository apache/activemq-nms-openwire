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

        private bool userSpecifiedClientID;
        private readonly Uri brokerUri;
        private ITransport transport;
        private ConnectionInfo info;
        private TimeSpan requestTimeout;
        private BrokerInfo brokerInfo; // from broker
        private WireFormatInfo brokerWireFormatInfo; // from broker
        private readonly IList sessions = ArrayList.Synchronized(new ArrayList());
        private readonly IDictionary producers = Hashtable.Synchronized(new Hashtable());
        private readonly IDictionary dispatchers = Hashtable.Synchronized(new Hashtable());
        private readonly object myLock = new object();
        private bool connected = false;
        private bool closed = false;
        private bool closing = false;
        private int sessionCounter = 0;
        private int temporaryDestinationCounter = 0;
        private int localTransactionCounter;
        private readonly Atomic<bool> started = new Atomic<bool>(false);
        private ConnectionMetaData metaData = null;
        private bool disposed = false;
        private IRedeliveryPolicy redeliveryPolicy;
        private PrefetchPolicy prefetchPolicy = new PrefetchPolicy();
        private ICompressionPolicy compressionPolicy = new CompressionPolicy();
        private IdGenerator clientIdGenerator;
        private volatile CountDownLatch transportInterruptionProcessingComplete;

        public Connection(Uri connectionUri, ITransport transport, IdGenerator clientIdGenerator)
        {
            this.brokerUri = connectionUri;
            this.requestTimeout = transport.RequestTimeout;
            this.clientIdGenerator = clientIdGenerator;

            this.transport = transport;
            this.transport.Command = new CommandHandler(OnCommand);
            this.transport.Exception = new ExceptionHandler(OnException);
            this.transport.Interrupted = new InterruptedHandler(OnTransportInterrupted);
            this.transport.Resumed = new ResumedHandler(OnTransportResumed);

            ConnectionId id = new ConnectionId();
            id.Value = CONNECTION_ID_GENERATOR.GenerateId();

            this.info = new ConnectionInfo();
            this.info.ConnectionId = id;
            this.info.FaultTolerant = transport.IsFaultTolerant;
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

        public string ClientId
        {
            get { return info.ClientId; }
            set
            {
                if(this.connected)
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
            return CreateSession(acknowledgementMode);
        }

        /// <summary>
        /// Creates a new session to work on this connection
        /// </summary>
        public ISession CreateSession(AcknowledgementMode sessionAcknowledgementMode)
        {
            SessionInfo info = CreateSessionInfo(sessionAcknowledgementMode);
            SyncRequest(info, this.RequestTimeout);
            Session session = new Session(this, info, sessionAcknowledgementMode, this.dispatchAsync);

            // Set properties on session using parameters prefixed with "session."
            URISupport.CompositeData c = URISupport.parseComposite(this.brokerUri);
            URISupport.SetProperties(session, c.Parameters, "session.");

            if(IsStarted)
            {
                session.Start();
            }

            sessions.Add(session);
            return session;
        }

        internal void RemoveSession(Session session)
        {
            if(!this.closing)
            {
                sessions.Remove(session);
            }
        }

        internal void addDispatcher( ConsumerId id, IDispatcher dispatcher )
        {
            this.dispatchers.Add( id, dispatcher );
        }

        internal void removeDispatcher( ConsumerId id )
        {
            this.dispatchers.Remove( id );
        }

        internal void addProducer( ProducerId id, MessageProducer producer )
        {
            this.producers.Add( id, producer );
        }

        internal void removeProducer( ProducerId id )
        {
            this.producers.Remove( id );
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
                    Tracer.Info("Closing Connection.");
                    this.closing = true;
                    lock(sessions.SyncRoot)
                    {
                        foreach(Session session in sessions)
                        {
                            session.DoClose();
                        }
                    }
                    sessions.Clear();

                    if(connected)
                    {
                        DisposeOf(ConnectionId);
                        ShutdownInfo shutdowninfo = new ShutdownInfo();
                        transport.Oneway(shutdowninfo);
                    }

                    Tracer.Info("Disposing of the Transport.");
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

        // Implementation methods

        /// <summary>
        /// Performs a synchronous request-response with the broker
        /// </summary>
        ///

        public Response SyncRequest(Command command)
        {
            try
            {
                return SyncRequest(command, this.RequestTimeout);
            }
            catch(Exception ex)
            {
                throw NMSExceptionSupport.Create(ex);
            }
        }

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
                    if(connected)
                    {
                        transport.Oneway(command);
                    }
                    Tracer.Info("Oneway command sent to broker.");
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

        protected void CheckConnected()
        {
            if(closed)
            {
                throw new ConnectionClosedException();
            }

            if(!connected)
            {
                if(!this.userSpecifiedClientID)
                {
                    this.info.ClientId = this.clientIdGenerator.GenerateId();
                }

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
                WaitForTransportInterruptionProcessingToComplete();
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
            else if(command is ProducerAck)
            {
                ProducerAck ack = (ProducerAck) command;
                if(ack != null && ack.ProducerId != null) {
                    MessageProducer producer = (MessageProducer) producers[ack.ProducerId];
                    if( producer != null ) {
                        producer.OnProducerAck(ack);
                    }
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
            Tracer.Info("Keep alive message received.");

            try
            {
                if(connected)
                {
                    Tracer.Info("Returning KeepAliveInfo Response.");
                    info.ResponseRequired = false;
                    transport.Oneway(info);
                }
            }
            catch(Exception ex)
            {
                if(!closing && !closed)
                {
                    OnException(commandTransport, ex);
                }
            }
        }

        protected void OnException(ITransport sender, Exception exception)
        {
            if(ExceptionListener != null && !this.closing)
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

        protected void OnTransportInterrupted(ITransport sender)
        {
            Tracer.Debug("Connection: Transport has been Interrupted.");

            this.transportInterruptionProcessingComplete = new CountDownLatch(dispatchers.Count);
            if(Tracer.IsDebugEnabled)
            {
                Tracer.Debug("transport interrupted, dispatchers: " + dispatchers.Count);
            }

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

            if(this.ConnectionInterruptedListener != null && !this.closing )
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

            if(this.ConnectionResumedListener != null && !this.closing )
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

        private SessionInfo CreateSessionInfo(AcknowledgementMode sessionAcknowledgementMode)
        {
            SessionInfo answer = new SessionInfo();
            SessionId sessionId = new SessionId();
            sessionId.ConnectionId = info.ConnectionId.Value;
            sessionId.Value = Interlocked.Increment(ref sessionCounter);
            answer.SessionId = sessionId;
            return answer;
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

            return destination;
        }

        protected void CreateTemporaryDestination(ActiveMQDestination tempDestination)
        {
        }

        public void DeleteTemporaryDestination(IDestination destination)
        {
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
                if(!closed && cdl.Remaining > 0)
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

    }
}
