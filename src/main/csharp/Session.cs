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
    public class Session : ISession
    {
        /// <summary>
        /// Private object used for synchronization, instead of public "this"
        /// </summary>
        private readonly object myLock = new object();
        private int consumerCounter;
        private readonly IDictionary consumers = Hashtable.Synchronized(new Hashtable());
        private readonly IDictionary producers = Hashtable.Synchronized(new Hashtable());
        private readonly DispatchingThread dispatchingThread;
        private DispatchingThread.ExceptionHandler dispatchingThread_ExceptionHandler;
        private readonly SessionInfo info;
        private int producerCounter;
        internal bool startedAsyncDelivery = false;
        private bool disposed = false;
        private bool closed = false;
        private bool closing = false;
        private TimeSpan MAX_THREAD_WAIT = TimeSpan.FromMilliseconds(30000);

        public Session(Connection connection, SessionInfo info, AcknowledgementMode acknowledgementMode)
        {
            this.connection = connection;
            this.info = info;
            this.acknowledgementMode = acknowledgementMode;
            this.AsyncSend = connection.AsyncSend;
            this.requestTimeout = connection.RequestTimeout;
            this.PrefetchSize = 1000;
            this.transactionContext = new TransactionContext(this);
            this.dispatchingThread = new DispatchingThread(new DispatchingThread.DispatchFunction(DispatchAsyncMessages));
            this.dispatchingThread_ExceptionHandler = new DispatchingThread.ExceptionHandler(dispatchingThread_ExceptionListener);
        }

        ~Session()
        {
            Dispose(false);
        }

        /// <summary>
        /// Sets the prefetch size, the maximum number of messages a broker will dispatch to consumers
        /// until acknowledgements are received.
        /// </summary>
        public int PrefetchSize;

        /// <summary>
        /// Sets the maximum number of messages to keep around per consumer
        /// in addition to the prefetch window for non-durable topics until messages
        /// will start to be evicted for slow consumers.
        /// Must be > 0 to enable this feature
        /// </summary>
        public int MaximumPendingMessageLimit;

        /// <summary>
        /// Enables or disables whether asynchronous dispatch should be used by the broker
        /// </summary>
        public bool DispatchAsync;

        /// <summary>
        /// Enables or disables exclusive consumers when using queues. An exclusive consumer means
        /// only one instance of a consumer is allowed to process messages on a queue to preserve order
        /// </summary>
        public bool Exclusive;

        /// <summary>
        /// Enables or disables retroactive mode for consumers; i.e. do they go back in time or not?
        /// </summary>
        public bool Retroactive;

        /// <summary>
        /// Sets the default consumer priority for consumers
        /// </summary>
        public byte Priority;

        /// <summary>
        /// This property indicates whether or not async send is enabled.
        /// </summary>
        public bool AsyncSend;

        private Connection connection;
        public Connection Connection
        {
            get { return this.connection; }
        }

        public SessionId SessionId
        {
            get { return info.SessionId; }
        }

        private TransactionContext transactionContext;
        public TransactionContext TransactionContext
        {
            get { return this.transactionContext; }
        }

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
                    this.closing = true;
                    StopAsyncDelivery();
                    lock(consumers.SyncRoot)
                    {
                        foreach(MessageConsumer consumer in consumers.Values)
                        {
                            consumer.Close();
                        }
                    }
                    consumers.Clear();

                    lock(producers.SyncRoot)
                    {
                        foreach(MessageProducer producer in producers.Values)
                        {
                            producer.Close();
                        }
                    }
                    producers.Clear();
                    Connection.RemoveSession(this);
                }
                catch(Exception ex)
                {
                    Tracer.ErrorFormat("Error during session close: {0}", ex);
                }
                finally
                {
                    this.connection = null;
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
                this.DoSend(command);
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

            try
            {
                consumer = new MessageConsumer(this, command, this.AcknowledgementMode);
                // lets register the consumer first in case we start dispatching messages immediately
                consumers[consumerId] = consumer;
                this.DoSend(command);
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
            MessageConsumer consumer = null;

            try
            {
                consumer = new MessageConsumer(this, command, this.AcknowledgementMode);
                // lets register the consumer first in case we start dispatching messages immediately
                consumers[consumerId] = consumer;
                this.DoSend(command);
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
            this.DoSend(command);
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

            this.DoSend(command);
        }

        public IMessage CreateMessage()
        {
            ActiveMQMessage answer = new ActiveMQMessage();
            Configure(answer);
            return answer;
        }

        public ITextMessage CreateTextMessage()
        {
            ActiveMQTextMessage answer = new ActiveMQTextMessage();
            Configure(answer);
            return answer;
        }

        public ITextMessage CreateTextMessage(string text)
        {
            ActiveMQTextMessage answer = new ActiveMQTextMessage(text);
            Configure(answer);
            return answer;
        }

        public IMapMessage CreateMapMessage()
        {
            return new ActiveMQMapMessage();
        }

        public IBytesMessage CreateBytesMessage()
        {
            return new ActiveMQBytesMessage();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            ActiveMQBytesMessage answer = new ActiveMQBytesMessage();
            answer.Content = body;
            return answer;
        }

		public IStreamMessage CreateStreamMessage()
		{
			return new ActiveMQStreamMessage();
		}
		
		public IObjectMessage CreateObjectMessage(object body)
        {
            ActiveMQObjectMessage answer = new ActiveMQObjectMessage();
            answer.Body = body;
            return answer;
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

            // lets ensure all the consumers redeliver any rolled back messages
            lock(consumers.SyncRoot)
            {
                foreach(MessageConsumer consumer in consumers.Values)
                {
                    consumer.RedeliverRolledBackMessages();
                }
            }
        }


        // Properties

        private TimeSpan requestTimeout = Apache.NMS.NMSConstants.defaultRequestTimeout;
        public TimeSpan RequestTimeout
        {
            get { return this.requestTimeout; }
            set { this.requestTimeout = value; }
        }

        public bool Transacted
        {
            get { return this.AcknowledgementMode == AcknowledgementMode.Transactional; }
        }

        private AcknowledgementMode acknowledgementMode;
        public AcknowledgementMode AcknowledgementMode
        {
            get { return this.acknowledgementMode; }
        }

        #endregion

        private void dispatchingThread_ExceptionListener(Exception exception)
        {
            if(null != Connection)
            {
                try
                {
                    Connection.OnSessionException(this, exception);
                }
                catch
                {
                }
            }
        }

        protected void CreateTemporaryDestination(ActiveMQDestination tempDestination)
        {
            DestinationInfo command = new DestinationInfo();
            command.ConnectionId = Connection.ConnectionId;
            command.OperationType = DestinationInfo.ADD_OPERATION_TYPE; // 0 is add
            command.Destination = tempDestination;

            this.DoSend(command);
        }

        private void DoSend(Command message)
        {
            this.DoSend(message, this.RequestTimeout);
        }

        private void DoSend(Command message, TimeSpan requestTimeout)
        {
            if(AsyncSend)
            {
                message.ResponseRequired = false;
                Connection.Oneway(message);
            }
            else
            {
                Connection.SyncRequest(message, requestTimeout);
            }
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

        public void DisposeOf(ConsumerId objectId)
        {
            Connection.DisposeOf(objectId);
            if(!this.closing)
            {
                consumers.Remove(objectId);
            }
        }

        public void DisposeOf(ProducerId objectId)
        {
            Connection.DisposeOf(objectId);
            connection.removeProducer(objectId);
            if(!this.closing)
            {
                producers.Remove(objectId);
            }
        }

        public bool DispatchMessage(ConsumerId consumerId, Message message)
        {
            bool dispatched = false;
            MessageConsumer consumer = (MessageConsumer) consumers[consumerId];

            if(consumer != null)
            {
                consumer.Dispatch((ActiveMQMessage) message);
                dispatched = true;
            }

            return dispatched;
        }

        /// <summary>
        /// Private method called by the dispatcher thread in order to perform
        /// asynchronous delivery of queued (inbound) messages.
        /// </summary>
        private void DispatchAsyncMessages()
        {
            // lets iterate through each consumer created by this session
            // ensuring that they have all pending messages dispatched
            lock(consumers.SyncRoot)
            {
                foreach(MessageConsumer consumer in consumers.Values)
                {
                    consumer.DispatchAsyncMessages();
                }
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
            answer.PrefetchSize = this.PrefetchSize;
            answer.Priority = this.Priority;
            answer.Exclusive = this.Exclusive;
            answer.DispatchAsync = this.DispatchAsync;
            answer.Retroactive = this.Retroactive;
            answer.MaximumPendingMessageLimit = this.MaximumPendingMessageLimit;

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

        /// <summary>
        /// Configures the message command
        /// </summary>
        protected void Configure(ActiveMQMessage message)
        {
        }

        internal void StopAsyncDelivery()
        {
            if(startedAsyncDelivery)
            {
                this.dispatchingThread.ExceptionListener -= this.dispatchingThread_ExceptionHandler;
                dispatchingThread.Stop((int) MAX_THREAD_WAIT.TotalMilliseconds);
                startedAsyncDelivery = false;
            }
        }

        internal void StartAsyncDelivery()
        {
            if(!startedAsyncDelivery)
            {
                this.dispatchingThread.ExceptionListener += this.dispatchingThread_ExceptionHandler;
                dispatchingThread.Start();
                startedAsyncDelivery = true;
            }
        }

        internal void RegisterConsumerDispatcher(Dispatcher dispatcher)
        {
            dispatcher.SetAsyncDelivery(this.dispatchingThread.EventHandle);
        }
    }
}
