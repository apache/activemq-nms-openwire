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
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Util;
using Apache.NMS.ActiveMQ.Threads;
using Apache.NMS.ActiveMQ.Util.Synchronization;
using Task = System.Threading.Tasks.Task;

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
        private readonly NmsSynchronizationMonitor myLock = new NmsSynchronizationMonitor();

        private readonly NmsSynchronizationMonitor consumersLock = new NmsSynchronizationMonitor();
        private readonly IDictionary consumers = Hashtable.Synchronized(new Hashtable());
        private readonly NmsSynchronizationMonitor producersLock = new NmsSynchronizationMonitor();
        private readonly IDictionary producers = Hashtable.Synchronized(new Hashtable());

        private readonly SessionExecutor executor;
        private readonly TransactionContext transactionContext;

        private readonly Connection connection;

        private bool dispatchAsync;
        private bool exclusive;
        private bool retroactive;
        private byte priority = 0;

        private readonly SessionInfo info;
        private int consumerCounter;
        private int producerCounter;
        private long nextDeliveryId;
        private long lastDeliveredSequenceId;
        protected bool disposed = false;
        protected bool closed = false;
        protected bool closing = false;
        protected Atomic<bool> clearInProgress = new Atomic<bool>();
        private TimeSpan disposeStopTimeout = TimeSpan.FromMilliseconds(30000);
        private TimeSpan closeStopTimeout = TimeSpan.FromMilliseconds(Timeout.Infinite);
        private TimeSpan requestTimeout;
        private readonly AcknowledgementMode acknowledgementMode;

        public Session(Connection connection, SessionId sessionId, AcknowledgementMode acknowledgementMode)
        {
            this.info = new SessionInfo();
            this.info.SessionId = sessionId;
            this.connection = connection;
            this.connection.Oneway(this.info);

            this.acknowledgementMode = acknowledgementMode;
            this.requestTimeout = connection.RequestTimeout;
            this.dispatchAsync = connection.DispatchAsync;
            this.transactionContext = CreateTransactionContext();
            this.exclusive = connection.ExclusiveConsumer;
            this.retroactive = connection.UseRetroactiveConsumer;

            Uri brokerUri = connection.BrokerUri;

            // Set propertieDs on session using parameters prefixed with "session."
            if(!String.IsNullOrEmpty(brokerUri.Query) && !brokerUri.OriginalString.EndsWith(")"))
            {
                string query = brokerUri.Query.Substring(brokerUri.Query.LastIndexOf(")") + 1);
                StringDictionary options = URISupport.ParseQuery(query);
                options = URISupport.GetProperties(options, "session.");
                URISupport.SetProperties(this, options);
            }

            this.ConsumerTransformer = connection.ConsumerTransformer;
            this.ProducerTransformer = connection.ProducerTransformer;

            this.executor = new SessionExecutor(this, this.consumers);

            if(connection.IsStarted)
            {
                this.Start();
            }

            connection.AddSession(this);
        }

        ~Session()
        {
            Dispose(false);
        }

        #region Property Accessors

        #region Session Transaction Events

        // We delegate the events to the TransactionContext since it knows
        // what the state is for both Local and DTC transactions.

        public event SessionTxEventDelegate TransactionStartedListener
        {
            add { this.transactionContext.TransactionStartedListener += value; }
            remove { this.transactionContext.TransactionStartedListener += value; }
        }

        public event SessionTxEventDelegate TransactionCommittedListener
        {
            add { this.transactionContext.TransactionCommittedListener += value; }
            remove { this.transactionContext.TransactionCommittedListener += value; }
        }

        public event SessionTxEventDelegate TransactionRolledBackListener
        {
            add { this.transactionContext.TransactionRolledBackListener += value; }
            remove { this.transactionContext.TransactionRolledBackListener += value; }
        }

        #endregion

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
            get { return this.IsTransacted; }
        }

        public virtual AcknowledgementMode AcknowledgementMode
        {
            get { return this.acknowledgementMode; }
        }

        public virtual bool IsClientAcknowledge
        {
            get { return this.acknowledgementMode == AcknowledgementMode.ClientAcknowledge; }
        }

        public virtual bool IsAutoAcknowledge
        {
            get { return this.acknowledgementMode == AcknowledgementMode.AutoAcknowledge; }
        }

        public virtual bool IsDupsOkAcknowledge
        {
            get { return this.acknowledgementMode == AcknowledgementMode.DupsOkAcknowledge; }
        }

        public virtual bool IsIndividualAcknowledge
        {
            get { return this.acknowledgementMode == AcknowledgementMode.IndividualAcknowledge; }
        }

        public virtual bool IsTransacted
        {
            get{ return this.acknowledgementMode == AcknowledgementMode.Transactional; }
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

        private ConsumerTransformerDelegate consumerTransformer;
        public Task RollbackAsync()
        {
            Rollback();
            return Task.CompletedTask;
        }

        /// <summary>
        /// A Delegate that is called each time a Message is dispatched to allow the client to do
        /// any necessary transformations on the received message before it is delivered.
        /// The Session instance sets the delegate on each Consumer it creates.
        /// </summary>
        public ConsumerTransformerDelegate ConsumerTransformer
        {
            get { return this.consumerTransformer; }
            set { this.consumerTransformer = value; }
        }

        private ProducerTransformerDelegate producerTransformer;
        /// <summary>
        /// A delegate that is called each time a Message is sent from this Producer which allows
        /// the application to perform any needed transformations on the Message before it is sent.
        /// The Session instance sets the delegate on each Producer it creates.
        /// </summary>
        public ProducerTransformerDelegate ProducerTransformer
        {
            get { return this.producerTransformer; }
            set { this.producerTransformer = value; }
        }

        internal Scheduler Scheduler
        {
            get { return this.connection.Scheduler; }
        }

        internal List<MessageConsumer> Consumers
        {
            get
            {
                List<MessageConsumer> copy = new List<MessageConsumer>();
                
                using(consumersLock.Lock())
                {
                    foreach(MessageConsumer consumer in consumers.Values)
                    {
                        copy.Add(consumer);
                    }
                }
                return copy;
            }
        }

        internal long LastDeliveredSequenceId
        {
            get { return this.lastDeliveredSequenceId; }
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

        public Task<IStreamMessage> CreateStreamMessageAsync()
        {
            return Task.FromResult(CreateStreamMessage());
        }

        public virtual void Close()
        {
            CloseAsync().GetAsyncResult();
        }
        
        public virtual async Task CloseAsync()
        {
            if (!this.closed)
            {
                try
                {
                    Tracer.InfoFormat("Closing The Session with Id {0}", this.info.SessionId);
                    await DoCloseAsync().Await();
                    Tracer.InfoFormat("Closed The Session with Id {0}", this.info.SessionId);
                }
                catch (Exception ex)
                {
                    Tracer.ErrorFormat("Error during session close: {0}", ex);
                }
            }
        }

        internal async Task DoCloseAsync()
        {
            await ShutdownAsync().Await();
            RemoveInfo removeInfo = new RemoveInfo();
            removeInfo.ObjectId = this.info.SessionId;
            removeInfo.LastDeliveredSequenceId = this.lastDeliveredSequenceId;
            this.connection.Oneway(removeInfo);
        }

        internal async Task ShutdownAsync()
        {
            Tracer.InfoFormat("Executing Shutdown on Session with Id {0}", this.info.SessionId);

            if(this.closed)
            {
                return;
            }

            using(await myLock.LockAsync().Await())
            {
                if(this.closed || this.closing)
                {
                    return;
                }

                try
                {
                    this.closing = true;

                    // Stop all message deliveries from this Session
                    this.executor.Stop(this.closeStopTimeout);

                    using(await consumersLock.LockAsync().Await())
                    {
                        foreach(MessageConsumer consumer in consumers.Values)
                        {
                            consumer.FailureError = this.connection.FirstFailureError;
                            consumer.ShutdownAsync().GetAsyncResult();
                            this.lastDeliveredSequenceId =
                                Math.Max(this.lastDeliveredSequenceId, consumer.LastDeliveredSequenceId);
                        }
                        // tried to move here
                        consumers.Clear();
                    }
                    

                    using(await producersLock.LockAsync().Await())
                    {
                        foreach(MessageProducer producer in producers.Values)
                        {
                            producer.Shutdown();
                        }
                        producers.Clear();
                    }
                    

                    // If in a local transaction we just roll back at this point.
                    if (this.IsTransacted && this.transactionContext.InLocalTransaction)
                    {
                        try
                        {
                            await this.transactionContext.RollbackAsync().Await();
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

        public Task<IMessageProducer> CreateProducerAsync()
        {
            return CreateProducerAsync(null);
        }

        public IMessageProducer CreateProducer(IDestination destination)
        {
            return CreateProducerAsync(destination).GetAsyncResult();
        }
        
        public async Task<IMessageProducer> CreateProducerAsync(IDestination destination)
        {
            MessageProducer producer = null;

            try
            {
                ActiveMQDestination dest = null;
                if(destination != null)
                {
                    dest = ActiveMQDestination.Transform(destination);
                }

                producer = DoCreateMessageProducer(GetNextProducerId(), dest);

                producer.ProducerTransformer = this.ProducerTransformer;

                this.AddProducer(producer);
                await this.Connection.SyncRequestAsync(producer.ProducerInfo).Await();
            }
            catch(Exception)
            {
                if(producer != null)
                {
                    this.RemoveProducer(producer.ProducerId);
                }

                throw;
            }

            return producer;
        }

        internal virtual MessageProducer DoCreateMessageProducer(ProducerId id, ActiveMQDestination destination)
        {
            return new MessageProducer(this, id, destination, this.RequestTimeout);
        }

        public IMessageConsumer CreateConsumer(IDestination destination)
        {
            return CreateConsumer(destination, null, false);
        }

        public Task<IMessageConsumer> CreateConsumerAsync(IDestination destination)
        {
            return CreateConsumerAsync(destination, null, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector)
        {
            return CreateConsumer(destination, selector, false);
        }

        public Task<IMessageConsumer> CreateConsumerAsync(IDestination destination, string selector)
        {
            return CreateConsumerAsync(destination, selector, false);
        }

        public IMessageConsumer CreateConsumer(IDestination destination, string selector, bool noLocal)
        {
            return CreateConsumerAsync(destination, selector, noLocal).GetAsyncResult();
        }
        public async Task<IMessageConsumer> CreateConsumerAsync(IDestination destination, string selector, bool noLocal)
        {
            if(destination == null)
            {
                throw new InvalidDestinationException("Cannot create a Consumer with a Null destination");
            }

            ActiveMQDestination dest = ActiveMQDestination.Transform(destination);
            int prefetchSize = this.Connection.PrefetchPolicy.DurableTopicPrefetch;

            if(dest.IsTopic)
            {
                prefetchSize = this.connection.PrefetchPolicy.TopicPrefetch;
            }
            else if(dest.IsQueue)
            {
                prefetchSize = this.connection.PrefetchPolicy.QueuePrefetch;
            }

            MessageConsumer consumer = null;

            try
            {
                consumer = DoCreateMessageConsumer(GetNextConsumerId(), dest, null, selector, prefetchSize,
                                                   this.connection.PrefetchPolicy.MaximumPendingMessageLimit,
                                                   noLocal);

                consumer.ConsumerTransformer = this.ConsumerTransformer;

                this.AddConsumer(consumer);
                await this.Connection.SyncRequestAsync(consumer.ConsumerInfo).Await();

                if(this.Connection.IsStarted)
                {
                    consumer.Start();
                }
            }
            catch(Exception)
            {
                if(consumer != null)
                {
                    this.RemoveConsumer(consumer);
                }

                throw;
            }

            return consumer;
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name)
        {
            return CreateDurableConsumerAsync(destination, name, null, false).GetAsyncResult();
        }

        public Task<IMessageConsumer> CreateDurableConsumerAsync(ITopic destination, string name)
        {
            return CreateDurableConsumerAsync(destination, name, null, false);
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector)
        {
            return CreateDurableConsumerAsync(destination, name, selector, false).GetAsyncResult();
        }

        public Task<IMessageConsumer> CreateDurableConsumerAsync(ITopic destination, string name, string selector)
        {
            return CreateDurableConsumerAsync(destination, name, selector, false);
        }

        public IMessageConsumer CreateDurableConsumer(ITopic destination, string name, string selector, bool noLocal)
        {
            return CreateDurableConsumerAsync(destination, name, selector, noLocal).GetAsyncResult();
        }
        
        public async Task<IMessageConsumer> CreateDurableConsumerAsync(ITopic destination, string name, string selector, bool noLocal)
        {
            if(destination == null)
            {
                throw new InvalidDestinationException("Cannot create a Consumer with a Null destination");
            }

            if (IsIndividualAcknowledge)
            {
                throw new NMSException("Cannot create a durable consumer for a session that is using " +
                                       "Individual Acknowledgement mode.");
            }

            ActiveMQDestination dest = ActiveMQDestination.Transform(destination);
            MessageConsumer consumer = null;

            try
            {
                consumer = DoCreateMessageConsumer(GetNextConsumerId(), dest, name, selector,
                                                   this.connection.PrefetchPolicy.DurableTopicPrefetch,
                                                   this.connection.PrefetchPolicy.MaximumPendingMessageLimit,
                                                   noLocal);

                consumer.ConsumerTransformer = this.ConsumerTransformer;

                this.AddConsumer(consumer);
                await this.Connection.SyncRequestAsync(consumer.ConsumerInfo).Await();

                if(this.Connection.IsStarted)
                {
                    consumer.Start();
                }
            }
            catch(Exception)
            {
                if(consumer != null)
                {
                    this.RemoveConsumer(consumer);
                }

                throw;
            }

            return consumer;
        }

        public IMessageConsumer CreateSharedConsumer(ITopic destination, string name)
        {
            throw new NotImplementedException();
        }

        public Task<IMessageConsumer> CreateSharedConsumerAsync(ITopic destination, string name)
        {
            throw new NotImplementedException();
        }

        public IMessageConsumer CreateSharedConsumer(ITopic destination, string name, string selector)
        {
            throw new NotImplementedException();
        }

        public Task<IMessageConsumer> CreateSharedConsumerAsync(ITopic destination, string name, string selector)
        {
            throw new NotImplementedException();
        }

        public IMessageConsumer CreateSharedDurableConsumer(ITopic destination, string name)
        {
            throw new NotImplementedException();
        }

        public Task<IMessageConsumer> CreateSharedDurableConsumerAsync(ITopic destination, string name)
        {
            throw new NotImplementedException();
        }

        public IMessageConsumer CreateSharedDurableConsumer(ITopic destination, string name, string selector)
        {
            throw new NotImplementedException();
        }

        public Task<IMessageConsumer> CreateSharedDurableConsumerAsync(ITopic destination, string name, string selector)
        {
            throw new NotImplementedException();
        }

        internal virtual MessageConsumer DoCreateMessageConsumer(
            ConsumerId id, ActiveMQDestination destination, string name, string selector,
            int prefetch, int maxPending, bool noLocal)
        {
            return new MessageConsumer(this, id, destination, name, selector, prefetch,
                                       maxPending, noLocal, false, this.DispatchAsync);
        }

        public void Unsubscribe(string name)
        {
            throw new NotImplementedException();
        }

        public Task UnsubscribeAsync(string name)
        {
            throw new NotImplementedException();
        }
        
        public void DeleteDurableConsumer(string name)
        {
            DeleteDurableConsumerAsync(name).GetAsyncResult();
        }

        public async Task DeleteDurableConsumerAsync(string name)
        {
            RemoveSubscriptionInfo command = new RemoveSubscriptionInfo();
            command.ConnectionId = Connection.ConnectionId;
            command.ClientId = Connection.ClientId;
            command.SubcriptionName = name;
            await this.connection.SyncRequestAsync(command).Await();
        }

        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            return this.CreateBrowser(queue, null);
        }

        public Task<IQueueBrowser> CreateBrowserAsync(IQueue queue)
        {
            return Task.FromResult(CreateBrowser(queue));
        }

        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            if(queue == null)
            {
                throw new InvalidDestinationException("Cannot create a Consumer with a Null destination");
            }

            ActiveMQDestination dest = ActiveMQDestination.Transform(queue);
            QueueBrowser browser = null;

            try
            {
                browser = new QueueBrowser(this, GetNextConsumerId(), dest, selector, this.DispatchAsync);
            }
            catch(Exception)
            {
                if(browser != null)
                {
                    browser.Close();
                }

                throw;
            }

            return browser;
        }

        public Task<IQueueBrowser> CreateBrowserAsync(IQueue queue, string selector)
        {
            return Task.FromResult(CreateBrowser(queue, selector));
        }

        public IQueue GetQueue(string name)
        {
            return new ActiveMQQueue(name);
        }

        public Task<IQueue> GetQueueAsync(string name)
        {
            return Task.FromResult(GetQueue(name));
        }

        public ITopic GetTopic(string name)
        {
            return new ActiveMQTopic(name);
        }

        public Task<ITopic> GetTopicAsync(string name)
        {
            return Task.FromResult(GetTopic(name));
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            return CreateTemporaryQueueAsync().GetAsyncResult();
        }
        
        public async Task<ITemporaryQueue> CreateTemporaryQueueAsync()
        {
            return (ITemporaryQueue) await this.connection.CreateTemporaryDestinationAsync(false).Await();
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            return CreateTemporaryTopicAsync().GetAsyncResult();
        }
        
        public async Task<ITemporaryTopic> CreateTemporaryTopicAsync()
        {
            return (ITemporaryTopic) await this.connection.CreateTemporaryDestinationAsync(true).Await();
        }

        /// <summary>
        /// Delete a destination (Queue, Topic, Temp Queue, Temp Topic).
        /// </summary>
        public void DeleteDestination(IDestination destination)
        {
            this.connection.DeleteDestination(destination);
        }

        public Task DeleteDestinationAsync(IDestination destination)
        {
            DeleteDestination(destination);
            return Task.CompletedTask;
        }

        public IMessage CreateMessage()
        {
            ActiveMQMessage answer = new ActiveMQMessage();
            return ConfigureMessage(answer) as IMessage;
        }

        public Task<IMessage> CreateMessageAsync()
        {
            return Task.FromResult(CreateMessage());
        }

        public ITextMessage CreateTextMessage()
        {
            ActiveMQTextMessage answer = new ActiveMQTextMessage();
            return ConfigureMessage(answer) as ITextMessage;
        }

        public Task<ITextMessage> CreateTextMessageAsync()
        {
            return Task.FromResult(CreateTextMessage());
        }

        public ITextMessage CreateTextMessage(string text)
        {
            ActiveMQTextMessage answer = new ActiveMQTextMessage(text);
            return ConfigureMessage(answer) as ITextMessage;
        }

        public Task<ITextMessage> CreateTextMessageAsync(string text)
        {
            return Task.FromResult(CreateTextMessage(text));
        }

        public IMapMessage CreateMapMessage()
        {
            return ConfigureMessage(new ActiveMQMapMessage()) as IMapMessage;
        }

        public Task<IMapMessage> CreateMapMessageAsync()
        {
            return Task.FromResult(CreateMapMessage());
        }

        public Task<IObjectMessage> CreateObjectMessageAsync(object body)
        {
            return Task.FromResult(CreateObjectMessage(body));
        }

        public IBytesMessage CreateBytesMessage()
        {
            return ConfigureMessage(new ActiveMQBytesMessage()) as IBytesMessage;
        }

        public Task<IBytesMessage> CreateBytesMessageAsync()
        {
            return Task.FromResult(CreateBytesMessage());
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            ActiveMQBytesMessage answer = new ActiveMQBytesMessage();
            answer.Content = body;
            return ConfigureMessage(answer) as IBytesMessage;
        }

        public Task<IBytesMessage> CreateBytesMessageAsync(byte[] body)
        {
            return Task.FromResult(CreateBytesMessage(body));
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

        public Task AcknowledgeAsync()
        {
            Acknowledge();
            return Task.CompletedTask;
        }

        public void Commit()
        {
            this.DoCommit();
        }

        public Task CommitAsync()
        {
            Commit();
            return Task.CompletedTask;
        }

        public void Rollback()
        {
            this.DoRollback();
        }

        public async Task RecoverAsync()
        {
            CheckClosed();

            if (IsTransacted)
            {
                throw new IllegalStateException("Cannot Recover a Transacted Session");
            }

            using(await this.consumersLock.LockAsync().Await())
            {
                foreach(MessageConsumer consumer in this.consumers.Values)
                {
                    await consumer.RollbackAsync().Await();
                }
            }
        }

        public void Recover()
        {
            RecoverAsync().GetAsyncResult();
        }

        void ISession.Acknowledge()
        {
            Acknowledge();
        }

        #endregion

        internal async Task DoSendAsync(ActiveMQDestination destination, ActiveMQMessage message,
                             MessageProducer producer, MemoryUsage producerWindow, TimeSpan sendTimeout)
        {
            ActiveMQMessage msg = message;

            if(destination.IsTemporary && !connection.IsTempDestinationActive(destination as ActiveMQTempDestination))
            {
                throw new InvalidDestinationException("Cannot publish to a deleted Destination: " + destination);
            }

            if(IsTransacted)
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
                    await this.connection.SyncRequestAsync(msg, sendTimeout).Await();
                }
                else
                {
                    await this.connection.SyncRequestAsync(msg).Await();
                }
            }
        }

        internal virtual void DoCommit()
        {
            if(!IsTransacted)
            {
                throw new InvalidOperationException(
                        "You cannot perform a Commit() on a non-transacted session. Acknowlegement mode is: "
                        + this.AcknowledgementMode);
            }

            this.TransactionContext.Commit();
        }

        internal virtual void DoRollback()
        {
            if(!IsTransacted)
            {
                throw new InvalidOperationException(
                        "You cannot perform a Commit() on a non-transacted session. Acknowlegement mode is: "
                        + this.AcknowledgementMode);
            }

            this.TransactionContext.Rollback();
        }

        /// <summary>
        /// Ensures that a transaction is started
        /// </summary>
        internal virtual void DoStartTransaction()
        {
            if(IsTransacted && !TransactionContext.InTransaction)
            {
                this.TransactionContext.Begin();
            }
        }

        public void AddConsumer(MessageConsumer consumer)
        {
            if(!this.closing)
            {
                ConsumerId id = consumer.ConsumerId;

                // Registered with Connection before we register at the broker.
                using (consumersLock.Lock())
                {
                    consumers[id] = consumer;
                }
                connection.AddDispatcher(id, this);
            }
        }

        public void RemoveConsumer(MessageConsumer consumer)
        {
            connection.RemoveDispatcher(consumer.ConsumerId);
            if(!this.closing)
            {
                using (consumersLock.Lock())
                {
                    consumers.Remove(consumer.ConsumerId);
                }
            }
            connection.RemoveDispatcher(consumer);
        }

        public void AddProducer(MessageProducer producer)
        {
            if(!this.closing)
            {
                ProducerId id = producer.ProducerId;

                using (producersLock.Lock())
                {
                    this.producers[id] = producer;
                }

                this.connection.AddProducer(id, producer);
            }
        }

        public void RemoveProducer(ProducerId objectId)
        {
            connection.RemoveProducer(objectId);
            if (!this.closing)
            {
                using (producersLock.Lock())
                {
                    producers.Remove(objectId);
                }
            }
        }

        public ConsumerId GetNextConsumerId()
        {
            ConsumerId id = new ConsumerId();
            id.ConnectionId = info.SessionId.ConnectionId;
            id.SessionId = info.SessionId.Value;
            id.Value = Interlocked.Increment(ref consumerCounter);

            return id;
        }

        public ProducerId GetNextProducerId()
        {
            ProducerId id = new ProducerId();
            id.ConnectionId = info.SessionId.ConnectionId;
            id.SessionId = info.SessionId.Value;
            id.Value = Interlocked.Increment(ref producerCounter);

            return id;
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
            using(this.consumersLock.Lock())
            {
                foreach(MessageConsumer consumer in this.consumers.Values)
                {
                    consumer.Start();
                }
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

        internal void Redispatch(IDispatcher dispatcher, MessageDispatchChannel channel)
        {
            MessageDispatch[] messages = channel.RemoveAll();
            foreach (MessageDispatch dispatch in messages)
            {
                this.connection.RollbackDuplicate(dispatcher, dispatch.Message);
            }
            System.Array.Reverse(messages);

            foreach(MessageDispatch message in messages)
            {
                this.executor.ExecuteFirst(message);
            }
        }

        public Task Dispatch_Async(MessageDispatch dispatch)
        {
            if(this.executor != null)
            {
                this.executor.Execute(dispatch);
            }

            return Task.CompletedTask;
        }

        internal void ClearMessagesInProgress(ref long transportInterruptionProcessingComplete)
        {
            if(this.executor != null)
            {
                this.executor.ClearMessagesInProgress();
            }

            if (this.consumers.Count == 0)
            {
                return;
            }

            // Because we are called from inside the Transport Reconnection logic
            // we spawn the Consumer clear to another Thread so that we can avoid
            // any lock contention that might exist between the consumer and the
            // connection that is reconnecting.  Use the Connection Scheduler so
            // that the clear calls are done one at a time to avoid further
            // contention on the Connection and Session resources.
            if (clearInProgress.CompareAndSet(false, true))
            {
                using(this.consumersLock.Lock())
                {
                    foreach(MessageConsumer consumer in this.consumers.Values)
                    {
                        consumer.InProgressClearRequired();
                        Interlocked.Increment(ref transportInterruptionProcessingComplete);
                        Scheduler.ExecuteAfterDelay(ClearMessages, consumer, 0);
                    }
                }

                // Clear after all consumer have had their ClearMessagesInProgress method called.
                Scheduler.ExecuteAfterDelay(ResetClearInProgressFlag, clearInProgress, 0);
            }
        }

        private static void ClearMessages(object value)
        {
            MessageConsumer consumer = value as MessageConsumer;

            if(Tracer.IsDebugEnabled)
            {
                Tracer.Debug("Performing Async Clear of In Progress Messages on Consumer: " + consumer.ConsumerId);
            }

            consumer.ClearMessagesInProgress();
        }

        private static void ResetClearInProgressFlag(object value)
        {
            Atomic<bool> clearInProgress = value as Atomic<bool>;
            clearInProgress.Value = false;
        }

        internal void Acknowledge()
        {
            using(this.consumersLock.Lock())
            {
                foreach(MessageConsumer consumer in this.consumers.Values)
                {
                    consumer.AcknowledgeAsync().GetAsyncResult();
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

        internal Task SendAckAsync(MessageAck ack)
        {
            return this.SendAckAsync(ack, false);
        }

        internal async Task SendAckAsync(MessageAck ack, bool lazy)
        {
            if(Tracer.IsDebugEnabled)
            {
                Tracer.Debug("Session sending Ack: " + ack);
            }

            if(lazy || connection.SendAcksAsync || this.IsTransacted )
            {
                this.connection.Oneway(ack);
            }
            else
            {
                await this.connection.SyncRequestAsync(ack).Await();
            }
        }

        protected virtual TransactionContext CreateTransactionContext()
        {
            return new TransactionContext(this);
        }

        private void CheckClosed()
        {
            if (closed)
            {
                throw new IllegalStateException("Session is Closed");
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
        private static void DoNothingAcknowledge(ActiveMQMessage message)
        {
        }

        class SessionCloseSynchronization : ISynchronization
        {
            private readonly Session session;

            public SessionCloseSynchronization(Session session)
            {
                this.session = session;
            }

            public Task BeforeEndAsync()
            {
                return Task.CompletedTask;
            }

            public async Task AfterCommitAsync()
            {
                Tracer.Debug("SessionCloseSynchronization AfterCommit called for Session: " + session.SessionId);
                await session.DoCloseAsync().Await();
            }

            public async Task AfterRollbackAsync()
            {
                Tracer.Debug("SessionCloseSynchronization AfterRollback called for Session: " + session.SessionId);
                await session.DoCloseAsync().Await();
            }
        }

        internal bool IsInUse(ActiveMQTempDestination dest)
        {
            using(this.consumersLock.Lock())
            {
                foreach(MessageConsumer consumer in this.consumers.Values)
                {
                    if(consumer.IsInUse(dest))
                    {
                        return true;
                    }
                }
            }

            return false;
        }
    }
}
