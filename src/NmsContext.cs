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
using System.Threading.Tasks;
using Apache.NMS.ActiveMQ.Util;
using Apache.NMS.ActiveMQ.Util.Synchronization;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ
{
    public class NmsContext : INMSContext
    {
        private readonly NmsSynchronizationMonitor syncRoot = new NmsSynchronizationMonitor();

        private readonly Connection connection;
        private readonly Atomic<long> connectionRefCount;
        private Session session;
        private MessageProducer sharedProducer;
        private bool autoStart = true;
        public AcknowledgementMode AcknowledgementMode { get; }

        public NmsContext(Connection connection, AcknowledgementMode acknowledgementMode)
        {
            this.connection = connection;
            this.AcknowledgementMode = acknowledgementMode;
            this.connectionRefCount = new Atomic<long>(1);
        }

        private NmsContext(Connection connection, AcknowledgementMode acknowledgementMode,
            Atomic<long> connectionRefCount)
        {
            this.connection = connection;
            this.AcknowledgementMode = acknowledgementMode;
            this.connectionRefCount = connectionRefCount;
        }

        public void Dispose()
        {
            connection.Dispose();
        }

        public void Start()
        {
            connection.Start();
        }

        public Task StartAsync()
        {
            return connection.StartAsync();
        }

        public bool IsStarted { get => connection.IsStarted; }
        
        public void Stop()
        {
            connection.Stop();
        }

        public Task StopAsync()
        {
            return connection.StopAsync();
        }

        public INMSContext CreateContext(AcknowledgementMode acknowledgementMode)
        {
            if (connectionRefCount.Value == 0) {
                throw new IllegalStateException("The Connection is closed");
            }

            connectionRefCount.IncrementAndGet();
            
            return new NmsContext(connection, acknowledgementMode, connectionRefCount);
        }

        public INMSProducer CreateProducer()
        {
            if (sharedProducer == null)
            {
                using(syncRoot.Lock())
                {
                    if (sharedProducer == null)
                    {
                        sharedProducer = (MessageProducer) GetSession().CreateProducer();
                    }
                }
            }
            return new NmsProducer(GetSession(), sharedProducer);
        }

        public async Task<INMSProducer> CreateProducerAsync()
        {
            if (sharedProducer == null)
            {
                using (await syncRoot.LockAsync().Await())
                {
                    if (sharedProducer == null)
                    {
                        sharedProducer = (MessageProducer) await (await GetSessionAsync().Await()).CreateProducerAsync().Await();
                    }
                }
            }
            return new NmsProducer(await GetSessionAsync().Await(), sharedProducer);
        }


        public INMSConsumer CreateConsumer(IDestination destination)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateConsumer(destination)));
        }

        public INMSConsumer CreateConsumer(IDestination destination, string selector)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateConsumer(destination, selector)));
        }

        public INMSConsumer CreateConsumer(IDestination destination, string selector, bool noLocal)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateConsumer(destination, selector, noLocal)));
        }

        public INMSConsumer CreateDurableConsumer(ITopic destination, string subscriptionName)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateDurableConsumer(destination, subscriptionName)));
        }

        public INMSConsumer CreateDurableConsumer(ITopic destination, string subscriptionName, string selector)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateDurableConsumer(destination, subscriptionName, selector)));
        }

        public INMSConsumer CreateDurableConsumer(ITopic destination, string subscriptionName, string selector, bool noLocal)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateDurableConsumer(destination, subscriptionName, selector, noLocal)));
        }

        public INMSConsumer CreateSharedConsumer(ITopic destination, string subscriptionName)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateSharedConsumer(destination, subscriptionName)));
        }

        public INMSConsumer CreateSharedConsumer(ITopic destination, string subscriptionName, string selector)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateSharedConsumer(destination, subscriptionName, selector)));
        }

        public INMSConsumer CreateSharedDurableConsumer(ITopic destination, string subscriptionName)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateSharedDurableConsumer(destination, subscriptionName)));
        }

        public INMSConsumer CreateSharedDurableConsumer(ITopic destination, string subscriptionName, string selector)
        {
            return PrepareConsumer(new NmsConsumer(GetSession(), (MessageConsumer) GetSession().CreateSharedDurableConsumer(destination, subscriptionName, selector)));
        }

        public async Task<INMSConsumer> CreateConsumerAsync(IDestination destination)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateConsumerAsync(destination).Await())).Await();
        }

        public async Task<INMSConsumer> CreateConsumerAsync(IDestination destination, string selector)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateConsumerAsync(destination, selector).Await())).Await();
        }

        public async Task<INMSConsumer> CreateConsumerAsync(IDestination destination, string selector, bool noLocal)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateConsumerAsync(destination, selector, noLocal).Await())).Await();
        }

        public async Task<INMSConsumer> CreateDurableConsumerAsync(ITopic destination, string subscriptionName)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateDurableConsumerAsync(destination, subscriptionName).Await())).Await();
        }

        public async Task<INMSConsumer> CreateDurableConsumerAsync(ITopic destination, string subscriptionName, string selector)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateDurableConsumerAsync(destination, subscriptionName, selector).Await())).Await();
        }

        public async Task<INMSConsumer> CreateDurableConsumerAsync(ITopic destination, string subscriptionName, string selector, bool noLocal)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateDurableConsumerAsync(destination, subscriptionName, selector, noLocal).Await())).Await();
        }

        public async Task<INMSConsumer> CreateSharedConsumerAsync(ITopic destination, string subscriptionName)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateSharedConsumerAsync(destination, subscriptionName).Await())).Await();
        }
        
        public async Task<INMSConsumer> CreateSharedConsumerAsync(ITopic destination, string subscriptionName, string selector)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateSharedConsumerAsync(destination, subscriptionName, selector).Await())).Await();
        }

        public async Task<INMSConsumer> CreateSharedDurableConsumerAsync(ITopic destination, string subscriptionName)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateSharedDurableConsumerAsync(destination, subscriptionName).Await())).Await();
        }

        public async Task<INMSConsumer> CreateSharedDurableConsumerAsync(ITopic destination, string subscriptionName, string selector)
        {
            return await PrepareConsumerAsync(new NmsConsumer(await GetSessionAsync().Await(), (MessageConsumer) await (await GetSessionAsync().Await()).CreateSharedDurableConsumerAsync(destination, subscriptionName, selector).Await())).Await();
        }

        public void Unsubscribe(string name)
        {
            GetSession().Unsubscribe(name);
        }

        public async Task UnsubscribeAsync(string name)
        {
            await (await GetSessionAsync().Await()).UnsubscribeAsync(name).Await();
        }

        public IQueueBrowser CreateBrowser(IQueue queue)
        {
            return GetSession().CreateBrowser(queue);
        }

        public async Task<IQueueBrowser> CreateBrowserAsync(IQueue queue)
        {
            return await (await GetSessionAsync().Await()).CreateBrowserAsync(queue).Await();
        }

        public IQueueBrowser CreateBrowser(IQueue queue, string selector)
        {
            return GetSession().CreateBrowser(queue, selector);
        }

        public async Task<IQueueBrowser> CreateBrowserAsync(IQueue queue, string selector)
        {
            return await (await GetSessionAsync().Await()).CreateBrowserAsync(queue, selector).Await();
        }

        public IQueue GetQueue(string name)
        {
            return GetSession().GetQueue(name);
        }

        public async Task<IQueue> GetQueueAsync(string name)
        {
            return await (await GetSessionAsync().Await()).GetQueueAsync(name).Await();
        }

        public ITopic GetTopic(string name)
        {
            return GetSession().GetTopic(name);
        }

        public async Task<ITopic> GetTopicAsync(string name)
        {
            return await (await GetSessionAsync().Await()).GetTopicAsync(name).Await();
        }

        public ITemporaryQueue CreateTemporaryQueue()
        {
            return GetSession().CreateTemporaryQueue();
        }

        public async Task<ITemporaryQueue> CreateTemporaryQueueAsync()
        {
            return await (await GetSessionAsync().Await()).CreateTemporaryQueueAsync().Await();
        }

        public ITemporaryTopic CreateTemporaryTopic()
        {
            return GetSession().CreateTemporaryTopic();
        }

        public async Task<ITemporaryTopic> CreateTemporaryTopicAsync()
        {
            return await (await GetSessionAsync().Await()).CreateTemporaryTopicAsync().Await();
        }

        public IMessage CreateMessage()
        {
            return GetSession().CreateMessage();
        }

        public async Task<IMessage> CreateMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateMessageAsync().Await();
        }

        public ITextMessage CreateTextMessage()
        {
            return GetSession().CreateTextMessage();
        }

        public async Task<ITextMessage> CreateTextMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateTextMessageAsync().Await();
        }

        public ITextMessage CreateTextMessage(string text)
        {
            return GetSession().CreateTextMessage(text);
        }

        public async Task<ITextMessage> CreateTextMessageAsync(string text)
        {
            return await (await GetSessionAsync().Await()).CreateTextMessageAsync(text).Await();
        }

        public IMapMessage CreateMapMessage()
        {
            return GetSession().CreateMapMessage();
        }

        public async Task<IMapMessage> CreateMapMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateMapMessageAsync().Await();
        }

        public IObjectMessage CreateObjectMessage(object body)
        {
            return GetSession().CreateObjectMessage(body);
        }

        public async Task<IObjectMessage> CreateObjectMessageAsync(object body)
        {
            return await (await GetSessionAsync().Await()).CreateObjectMessageAsync(body).Await();
        }

        public IBytesMessage CreateBytesMessage()
        {
            return GetSession().CreateBytesMessage();
        }

        public async Task<IBytesMessage> CreateBytesMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateBytesMessageAsync().Await();
        }

        public IBytesMessage CreateBytesMessage(byte[] body)
        {
            return GetSession().CreateBytesMessage(body);
        }

        public async Task<IBytesMessage> CreateBytesMessageAsync(byte[] body)
        {
            return await (await GetSessionAsync().Await()).CreateBytesMessageAsync(body).Await();
        }

        public IStreamMessage CreateStreamMessage()
        {
            return GetSession().CreateStreamMessage();
        }

        public async Task<IStreamMessage> CreateStreamMessageAsync()
        {
            return await (await GetSessionAsync().Await()).CreateStreamMessageAsync().Await();
        }

        public void Close()
        {
            CloseInternal(true).GetAsyncResult();
        }

        public Task CloseAsync()
        {
            return CloseInternal(false);
        }

        public async Task CloseInternal(bool sync)
        {
            NMSException failure = null;

            try
            {
                if (sync)
                    session?.Close();
                else
                    await (session?.CloseAsync() ?? Task.CompletedTask).Await();
            } catch (NMSException jmse)
            {
                failure = jmse;
            }

            if (connectionRefCount.DecrementAndGet() == 0) {
                try
                {
                    if (sync)
                        connection.Close();
                    else
                        await connection.CloseAsync().Await();
                } catch (NMSException jmse) {
                    if (failure == null)
                    {
                        failure = jmse;
                    }
                }
            }

            if (failure != null) {
                throw failure;
            }
        }
        
        
        public void Recover()
        {
            GetSession().Recover();
        }

        public async Task RecoverAsync()
        {
            await (await GetSessionAsync().Await()).RecoverAsync().Await();
        }

        public void Acknowledge()
        {
            GetSession().Acknowledge();
        }

        public async Task AcknowledgeAsync()
        {
            await (await GetSessionAsync().Await()).AcknowledgeAsync().Await();
        }

        public void Commit()
        {
            GetSession().Commit();
        }

        public async Task CommitAsync()
        {
            await (await GetSessionAsync().Await()).CommitAsync().Await();
        }

        public void Rollback()
        {
            GetSession().Rollback();
        }

        public async Task RollbackAsync()
        {
            await (await GetSessionAsync().Await()).RollbackAsync().Await();
        }

        public void PurgeTempDestinations()
        {
            connection.PurgeTempDestinations();
        }
        
        
        private Session GetSession()
        {
            return GetSessionAsync().GetAsyncResult();
        }

        private async Task<Session> GetSessionAsync()
        {
            if (session == null)
            {
                using(await syncRoot.LockAsync().Await())
                {
                    if (session == null)
                    {
                        session = (Session) await connection.CreateSessionAsync(AcknowledgementMode).Await();
                    }
                }
            }

            return session;
        }
        
        private NmsConsumer PrepareConsumer(NmsConsumer consumer)
        {
            return PrepareConsumerAsync(consumer).GetAsyncResult();
        }
        
        private async Task<NmsConsumer> PrepareConsumerAsync(NmsConsumer consumer) {
            if (autoStart) {
                await connection.StartAsync().Await();
            }
            return consumer;
        }
        

        public ConsumerTransformerDelegate ConsumerTransformer { get => session.ConsumerTransformer; set => session.ConsumerTransformer = value; }
        
        public ProducerTransformerDelegate ProducerTransformer { get => session.ProducerTransformer; set => session.ProducerTransformer = value; }
        
        public TimeSpan RequestTimeout { get => session.RequestTimeout; set => session.RequestTimeout = value; }
        
        public bool Transacted => session.Transacted;
        
        public string ClientId { get => connection.ClientId; set => connection.ClientId = value; }
        
        public bool AutoStart { get => autoStart; set => autoStart = value; }
        
        public event SessionTxEventDelegate TransactionStartedListener
        {
            add => GetSession().TransactionStartedListener += value;
            remove => GetSession().TransactionStartedListener -= value;
        }

        public event SessionTxEventDelegate TransactionCommittedListener
        {
            add => GetSession().TransactionCommittedListener += value;
            remove => GetSession().TransactionCommittedListener -= value;
        }

        public event SessionTxEventDelegate TransactionRolledBackListener
        {
            add => GetSession().TransactionRolledBackListener += value;
            remove => GetSession().TransactionRolledBackListener -= value;
        }

        public event ExceptionListener ExceptionListener
        {
            add => connection.ExceptionListener += value;
            remove => connection.ExceptionListener -= value;
        }
        
        public event ConnectionInterruptedListener ConnectionInterruptedListener
        {
            add => connection.ConnectionInterruptedListener += value;
            remove => connection.ConnectionInterruptedListener -= value;
        }
        
        public event ConnectionResumedListener ConnectionResumedListener
        {
            add => connection.ConnectionResumedListener += value;
            remove => connection.ConnectionResumedListener -= value;
        }
    }
}