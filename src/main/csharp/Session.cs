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
using Apache.NMS.ActiveMQ.Util;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ
{
    /// <summary>
    /// Default provider of ISession
    /// </summary>
    public class Session : ISession, IDispatcher
    {
        /// <summary>
        /// Private object used for synchronization, instead of public "this"
        /// </summary>
        private readonly object myLock = new object();

        private readonly IDictionary consumers = Hashtable.Synchronized(new Hashtable());
        private readonly IDictionary producers = Hashtable.Synchronized(new Hashtable());

        private SessionExecutor executor;
        private TransactionContext transactionContext;
        private Connection connection;

        private bool dispatchAsync;
        private bool exclusive;
        private bool retroactive;
        private byte priority = 4;

        private readonly SessionInfo info;
        private int consumerCounter;
        private int producerCounter;
        private long nextDeliveryId;
        private long lastDeliveredSequenceId;
        private bool disposed = false;
        private bool closed = false;
        private bool closing = false;
        private TimeSpan disposeStopTimeout = TimeSpan.FromMilliseconds(30000);
        private TimeSpan closeStopTimeout = TimeSpan.FromMilliseconds(-1);
        private TimeSpan requestTimeout = Apache.NMS.NMSConstants.defaultRequestTimeout;
        private AcknowledgementMode acknowledgementMode;

        public Session(Connection connection, SessionInfo info, AcknowledgementMode acknowledgementMode, bool dispatchAsync)
        {
            this.connection = connection;
            this.info = info;
            this.acknowledgementMode = acknowledgementMode;
            this.requestTimeout = connection.RequestTimeout;
            this.dispatchAsync = dispatchAsync;

            if(acknowledgementMode == AcknowledgementMode.Transactional)
            {
                this.transactionContext = new TransactionContext(this);
            }

            this.executor = new SessionExecutor(this, this.consumers);
        }

        ~Session()
        {
            Dispose(false);
        }

        #region Property Accessors

        /// <summary>
        /// Sets the prefetch size, the maximum number of messages a broker will dispatch to consumers
        /// until acknowledgements are received.
        /// </summary>
        public int PrefetchSize
        {
            set{ this.connection.PrefetchPolicy.SetAll(value); }
        }

        /// <summary>
        /// Sets the maximum number of messages to keep around per consumer
        /// in addition to the prefetch window for non-durable topics until messages
        /// will start to be evicted for slow consumers.
        /// Must be > 0 to enable this feature
        /// </summary>
        public int MaximumPendingMessageLimit
        {
            set{ this.connection.PrefetchPolicy.MaximumPendingMessageLimit = value; }
        }

        /// <summary>
        /// Enables or disables whether asynchronous dispatch should be used by the broker
        /// </summary>
        public bool DispatchAsync
        {
            get{ return this.dispatchAsync; }
            set{ this.dispatchAsync = value; }
        }

        /// <summary>
        /// Enables or disables exclusive consumers when using queues. An exclusive consumer means
        /// only one instance of a consumer is allowed to process messages on a queue to preserve order
        /// </summary>
        public bool Exclusive
        {
            get{ return this.exclusive; }
            set{ this.exclusive = value; }
        }

        /// <summary>
        /// Enables or disables retroactive mode for consumers; i.e. do they go back in time or not?
        /// </summary>
        public bool Retroactive
        {
            get{ return this.retroactive; }
            set{ this.retroactive = value; }
        }

        /// <summary>
        /// Sets the default consumer priority for consumers
        /// </summary>
        public byte Priority
        {
            get{ return this.priority; }
            set{ this.priority = value; }
        }

        public Connection Connection
        {
            get { return this.connection; }
        }

        public SessionId SessionId
        {
            get { return info.SessionId; }
        }

        public TransactionContext TransactionContext
        {
            get { return this.transactionContext; }
        }

        public TimeSpan RequestTimeout
        {
            get { return this.requestTimeout; }
            set { this.requestTimeout = value; }
        }

        public bool Transacted
        {
            get { return this.AcknowledgementMode == AcknowledgementMode.Transactional; }
        }

        public AcknowledgementMode AcknowledgementMode
        {
            get { return this.acknowledgementMode; }
        }

        public bool IsClientAcknowledge
        {
            get { return this.acknowledgementMode == AcknowledgementMode.ClientAcknowledge; }
        }

        public bool IsAutoAcknowledge
        {
            get { return this.acknowledgementMode == AcknowledgementMode.AutoAcknowledge; }
        }

        public bool IsDupsOkAcknowledge
        {
            get { return this.acknowledgementMode == AcknowledgementMode.DupsOkAcknowledge; }
        }

        public bool IsIndividualAcknowledge
        {
            get { return this.acknowledgementMode == AcknowledgementMode.IndividualAcknowledge; }
        }

        public bool IsTransacted
        {
            get { return this.acknowledgementMode == AcknowledgementMode.Transactional; }
        }

        public SessionExecutor Executor
        {
            get { return this.executor; }
        }

        public long NextDeliveryId
        {
            get { return Interlocked.Increment(ref this.nextDeliveryId); }
        }

        public long DisposeStopTimeout
        {
            get { return (long) this.disposeStopTimeout.TotalMilliseconds; }
            set { this.disposeStopTimeout = TimeSpan.FromMilliseconds(value); }
        }

        public long CloseStopTimeout
        {
            get { return (long) this.closeStopTimeout.TotalMilliseconds; }
            set { this.closeStopTimeout = TimeSpan.FromMilliseconds(value); }
        }

        #endregion

        #region ISession Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected void Dispose(bool disposing)
        {
            if(this.disposed)
            {
                return;
            }

            if(disposing)
            {
                // Dispose managed code here.
            }

            try
            {
                // Force a Stop when we are Disposing vs a Normal Close.
                this.executor.Stop(this.disposeStopTimeout);

                Close();
            }
            catch
            {
                // Ignore network errors.
            }

            this.disposed = true;
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
                    Tracer.InfoFormat("Closing The Session with Id {0}", this.info.SessionId.ToString());
                    DoClose();
                    Tracer.InfoFormat("Closed The Session with Id {0}", this.info.SessionId.ToString());
                }
                catch(Exception ex)
                {
                    Tracer.ErrorFormat("Error during session close: {0}", ex);
                }
                finally
                {
                    // Make sure we attempt to inform the broker this Session is done.
                    RemoveInfo info = new RemoveInfo();
                    info.ObjectId = this.info.SessionId;
                    info.LastDeliveredSequenceId = this.lastDeliveredSequenceId;
                    this.connection.Oneway(info);
                    this.connection = null;
                    this.closed = true;
                    this.closing = false;
                }
            }
        }

        internal void DoClose()
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

                    // Stop all message deliveries from this Session
                    this.executor.Stop(this.closeStopTimeout);

                    lock(consumers.SyncRoot)
                    {
                        foreach(MessageConsumer consumer in consumers.Values)
                        {
                            consumer.DoClose();
                            this.lastDeliveredSequenceId =
                                Math.Min(this.lastDeliveredSequenceId, consumer.LastDeliveredSequenceId);
                        }
                    }
                    consumers.Clear();

                    lock(producers.SyncRoot)
                    {
                        foreach(MessageProducer producer in producers.Values)
                        {
                            producer.DoClose();
                        }
                    }
                    producers.Clear();

                    // If in a transaction roll it back
                    if(this.IsTransacted && this.transactionContext.InTransaction)
                    {
                        try
                        {
                            this.transactionContext.Rollback();
                        }
                        catch
                        {
                        }
                    }

                    Connection.RemoveSession(this);
                }
                catch(Exception ex)
                {
                    Tracer.ErrorFormat("Error during session close: {0}", ex);
                }
                finally
                {
                    this.closed = true;
                    this.closing = false;
                }
            }
        }

        public IMessageProducer CreateProducer()
        {
            return CreateProducer(null);
        }

        public IMessageProducer CreateProducer(IDestination destination)
        {
            ProducerInfo command = CreateProducerInfo(destination);
            ProducerId producerId = command.ProducerId;
            MessageProducer producer = null;

            try
            {
                producer = new MessageProducer(this, command);
                producers[producerId] = producer;
                this.connection.Oneway(command);
            }
            catch(Exception)
            {
                if(producer != null)
                {
                    producer.Close();
                }

                throw;
            }

            // Registered with Connection so it can process Producer Acks.
            connection.addProducer(producerId, producer);

            return producer;
        }

        public IMessageConsumer CreateConsumer(IDestination destination)
        {
            return CreateConsumer(destination, null, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector)
        {
            return CreateConsumer(destination, selector, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector, bool noLocal)
        {
            if (destination == null)
            {
                throw new InvalidDestinationException("Cannot create a Consumer with a Null destination");
            }

            ConsumerInfo command = CreateConsumerInfo(destination, selector);
            command.NoLocal = noLocal;
            ConsumerId consumerId = command.ConsumerId;
            MessageConsumer consumer = null;

            // Registered with Connection before we register at the broker.
            connection.addDispatcher(consumerId, this);

            try
            {
                consumer = new MessageConsumer(this, command);
                // lets register the consumer first in case we start dispatching messages immediately
                consumers[consumerId] = consumer;
                this.Connection.SyncRequest(command);

                if(this.Started)
                {
                    consumer.Start();
                }

                return consumer;
            }
            catch(Exception)
            {
                if(consumer != null)
                {
                    consumer.Close();
                }

                throw;
            }
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector, bool noLocal)
        {
            if (destination == null)
            {
                throw new InvalidDestinationException("Cannot create a Consumer with a Null destination");
            }

            ConsumerInfo command = CreateConsumerInfo(destination, selector);
            ConsumerId consumerId = command.ConsumerId;
            command.SubscriptionName = name;
            command.NoLocal = noLocal;
            command.PrefetchSize = this.connection.PrefetchPolicy.DurableTopicPrefetch;
            MessageConsumer consumer = null;

            // Registered with Connection before we register at the broker.
            connection.addDispatcher(consumerId, this);

            try
            {
                consumer = new MessageConsumer(this, command);
                // lets register the consumer first in case we start dispatching messages immediately
                consumers[consumerId] = consumer;

                if(this.Started)
                {
                    consumer.Start();
                }

                this.connection.SyncRequest(command);
            }
            catch(Exception)
            {
                if(consumer != null)
                {
                    consumer.Close();
                }

                throw;
            }

            return consumer;
        }

        public void DeleteDurableConsumer(string name)
        {
            RemoveSubscriptionInfo command = new RemoveSubscriptionInfo();
            command.ConnectionId = Connection.ConnectionId;
            command.ClientId = Connection.ClientId;
            command.SubcriptionName = name;
            this.connection.SyncRequest(command);
        }

        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            throw new NotSupportedException("Not Yet Implemented");
        }

        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            throw new NotSupportedException("Not Yet Implemented");
        }

        public IQueue GetQueue(string name)
        {
            return new ActiveMQQueue(name);
        }

        public ITopic GetTopic(string name)
        {
            return new ActiveMQTopic(name);
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            ActiveMQTempQueue answer = new ActiveMQTempQueue(Connection.CreateTemporaryDestinationName());
            CreateTemporaryDestination(answer);
            return answer;
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            ActiveMQTempTopic answer = new ActiveMQTempTopic(Connection.CreateTemporaryDestinationName());
            CreateTemporaryDestination(answer);
            return answer;
        }

        /// <summary>
        /// Delete a destination (Queue, Topic, Temp Queue, Temp Topic).
        /// </summary>
        public void DeleteDestination(IDestination destination)
        {
            DestinationInfo command = new DestinationInfo();
            command.ConnectionId = Connection.ConnectionId;
            command.OperationType = DestinationInfo.REMOVE_OPERATION_TYPE; // 1 is remove
            command.Destination = (ActiveMQDestination) destination;

            this.connection.Oneway(command);
        }

        public IMessage CreateMessage()
        {
            ActiveMQMessage answer = new ActiveMQMessage();
            return ConfigureMessage(answer) as IMessage;
        }

        public ITextMessage CreateTextMessage()
        {
            ActiveMQTextMessage answer = new ActiveMQTextMessage();
            return ConfigureMessage(answer) as ITextMessage;
        }

        public ITextMessage CreateTextMessage(string text)
        {
            ActiveMQTextMessage answer = new ActiveMQTextMessage(text);
            return ConfigureMessage(answer) as ITextMessage;
        }

        public IMapMessage CreateMapMessage()
        {
            return ConfigureMessage(new ActiveMQMapMessage()) as IMapMessage;
        }

        public IBytesMessage CreateBytesMessage()
        {
            return ConfigureMessage(new ActiveMQBytesMessage()) as IBytesMessage;
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            ActiveMQBytesMessage answer = new ActiveMQBytesMessage();
            answer.Content = body;
            return ConfigureMessage(answer) as IBytesMessage;
        }

        public IStreamMessage CreateStreamMessage()
        {
            return ConfigureMessage(new ActiveMQStreamMessage()) as IStreamMessage;
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            ActiveMQObjectMessage answer = new ActiveMQObjectMessage();
            answer.Body = body;
            return ConfigureMessage(answer) as IObjectMessage;
        }

        public void Commit()
        {
            if(!Transacted)
            {
                throw new InvalidOperationException(
                        "You cannot perform a Commit() on a non-transacted session. Acknowlegement mode is: "
                        + this.AcknowledgementMode);
            }

            this.TransactionContext.Commit();
        }

        public void Rollback()
        {
            if(!Transacted)
            {
                throw new InvalidOperationException(
                        "You cannot perform a Commit() on a non-transacted session. Acknowlegement mode is: "
                        + this.AcknowledgementMode);
            }

            this.TransactionContext.Rollback();
        }

        #endregion

        protected void CreateTemporaryDestination(ActiveMQDestination tempDestination)
        {
            DestinationInfo command = new DestinationInfo();
            command.ConnectionId = Connection.ConnectionId;
            command.OperationType = DestinationInfo.ADD_OPERATION_TYPE; // 0 is add
            command.Destination = tempDestination;

            this.connection.SyncRequest(command);
        }

        public void DoSend( ActiveMQMessage message, MessageProducer producer, MemoryUsage producerWindow, TimeSpan sendTimeout )
        {
            ActiveMQMessage msg = message;

            if(Transacted)
            {
                DoStartTransaction();
                msg.TransactionId = TransactionContext.TransactionId;
            }

            msg.RedeliveryCounter = 0;
            msg.BrokerPath = null;

            if(this.connection.CopyMessageOnSend)
            {
                msg = (ActiveMQMessage)msg.Clone();
            }

            msg.OnSend();
            msg.ProducerId = msg.MessageId.ProducerId;

            if(sendTimeout.TotalMilliseconds <= 0 && !msg.ResponseRequired && !connection.AlwaysSyncSend &&
               (!msg.Persistent || connection.AsyncSend || msg.TransactionId != null))
            {
                this.connection.Oneway(msg);

                if(producerWindow != null)
                {
                    // Since we defer lots of the marshaling till we hit the wire, this
                    // might not provide and accurate size. We may change over to doing
                    // more aggressive marshaling, to get more accurate sizes.. this is more
                    // important once users start using producer window flow control.
                    producerWindow.IncreaseUsage(msg.Size());
                }
            }
            else
            {
                if(sendTimeout.TotalMilliseconds > 0)
                {
                    this.connection.SyncRequest(msg, sendTimeout);
                }
                else
                {
                    this.connection.SyncRequest(msg);
                }
            }
        }

        /// <summary>
        /// Ensures that a transaction is started
        /// </summary>
        public void DoStartTransaction()
        {
            if(Transacted)
            {
                this.TransactionContext.Begin();
            }
        }

        public void DisposeOf(ConsumerId objectId, long lastDeliveredSequenceId)
        {
            connection.removeDispatcher(objectId);
            this.lastDeliveredSequenceId = Math.Min(this.lastDeliveredSequenceId, lastDeliveredSequenceId);

            if(!this.closing)
            {
                consumers.Remove(objectId);
            }
        }

        public void DisposeOf(ProducerId objectId)
        {
            connection.removeProducer(objectId);
            if(!this.closing)
            {
                producers.Remove(objectId);
            }
        }

        protected virtual ConsumerInfo CreateConsumerInfo(IDestination destination, string selector)
        {
            ConsumerInfo answer = new ConsumerInfo();
            ConsumerId id = new ConsumerId();
            id.ConnectionId = info.SessionId.ConnectionId;
            id.SessionId = info.SessionId.Value;
            id.Value = Interlocked.Increment(ref consumerCounter);
            answer.ConsumerId = id;
            answer.Destination = ActiveMQDestination.Transform(destination);
            answer.Selector = selector;
            answer.Priority = this.Priority;
            answer.Exclusive = this.Exclusive;
            answer.DispatchAsync = this.DispatchAsync;
            answer.Retroactive = this.Retroactive;
            answer.MaximumPendingMessageLimit = this.connection.PrefetchPolicy.MaximumPendingMessageLimit;

            if(destination is ITopic || destination is ITemporaryTopic)
            {
                answer.PrefetchSize = this.connection.PrefetchPolicy.TopicPrefetch;
            }
            else if(destination is IQueue || destination is ITemporaryQueue)
            {
                answer.PrefetchSize = this.connection.PrefetchPolicy.QueuePrefetch;
            }

            // If the destination contained a URI query, then use it to set public properties
            // on the ConsumerInfo
            ActiveMQDestination amqDestination = destination as ActiveMQDestination;
            if(amqDestination != null && amqDestination.Options != null)
            {
                URISupport.SetProperties(answer, amqDestination.Options, "consumer.");
            }

            return answer;
        }

        protected virtual ProducerInfo CreateProducerInfo(IDestination destination)
        {
            ProducerInfo answer = new ProducerInfo();
            ProducerId id = new ProducerId();
            id.ConnectionId = info.SessionId.ConnectionId;
            id.SessionId = info.SessionId.Value;
            id.Value = Interlocked.Increment(ref producerCounter);
            answer.ProducerId = id;
            answer.Destination = ActiveMQDestination.Transform(destination);
            answer.WindowSize = connection.ProducerWindowSize;

            // If the destination contained a URI query, then use it to set public
            // properties on the ProducerInfo
            ActiveMQDestination amqDestination = destination as ActiveMQDestination;
            if(amqDestination != null && amqDestination.Options != null)
            {
                URISupport.SetProperties(answer, amqDestination.Options, "producer.");
            }

            return answer;
        }

        public void Stop()
        {
            if(this.executor != null)
            {
                this.executor.Stop();
            }
        }

        public void Start()
        {
            foreach(MessageConsumer consumer in this.consumers.Values)
            {
                consumer.Start();
            }

            if(this.executor != null)
            {
                this.executor.Start();
            }
        }

        public bool Started
        {
            get
            {
                return this.executor != null ? this.executor.Running : false;
            }
        }

        internal void Redispatch(MessageDispatchChannel channel)
        {
            MessageDispatch[] messages = channel.RemoveAll();
            System.Array.Reverse(messages);

            foreach(MessageDispatch message in messages)
            {
                this.executor.ExecuteFirst(message);
            }
        }

        public void Dispatch(MessageDispatch dispatch)
        {
            if(this.executor != null)
            {
                this.executor.Execute(dispatch);
            }
        }

        internal void ClearMessagesInProgress()
        {
            if( this.executor != null ) {
                this.executor.ClearMessagesInProgress();
            }

            lock(this.consumers.SyncRoot)
            {
                foreach(MessageConsumer consumer in this.consumers.Values)
                {
                    consumer.ClearMessagesInProgress();
                }
            }
        }

        internal void Acknowledge()
        {
            lock(this.consumers.SyncRoot)
            {
                foreach(MessageConsumer consumer in this.consumers.Values)
                {
                    consumer.Acknowledge();
                }
            }
        }

        private ActiveMQMessage ConfigureMessage(ActiveMQMessage message)
        {
            message.Connection = this.connection;

            if(this.IsTransacted)
            {
                // Allows Acknowledge to be called in a transaction with no effect per JMS Spec.
                message.Acknowledger += new AcknowledgeHandler(DoNothingAcknowledge);
            }

            return message;
        }

        internal void SendAck(MessageAck ack)
        {
            this.SendAck(ack, false);
        }

        internal void SendAck(MessageAck ack, bool lazy)
        {
            if(lazy || connection.SendAcksAsync || this.IsTransacted )
            {
                this.connection.Oneway(ack);
            }
            else
            {
                this.connection.SyncRequest(ack);
            }
        }

        /// <summary>
        /// Prevents message from throwing an exception if a client calls Acknoweldge on
        /// a message that is part of a transaction either being produced or consumed.  The
        /// JMS Spec indicates that users should be able to call Acknowledge with no effect
        /// if the message is in a transaction.
        /// </summary>
        /// <param name="message">
        /// A <see cref="ActiveMQMessage"/>
        /// </param>
        private void DoNothingAcknowledge(ActiveMQMessage message)
        {
        }

    }
}
