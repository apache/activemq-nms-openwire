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
using System.Threading;
using System.Collections.Generic;
using System.Collections.Specialized;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Util;
using Apache.NMS.ActiveMQ.Threads;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ
{
	public enum AckType
	{
		DeliveredAck = 0, // Message delivered but not consumed
		PoisonAck = 1, // Message could not be processed due to poison pill but discard anyway
		ConsumedAck = 2, // Message consumed, discard
		RedeliveredAck = 3, // Message has been Redelivered and is not yet poisoned.
		IndividualAck = 4 // Only the given message is to be treated as consumed.
	}

	/// <summary>
	/// An object capable of receiving messages from some destination
	/// </summary>
	public class MessageConsumer : IMessageConsumer, IDispatcher
	{
        private readonly MessageTransformation messageTransformation;
        private readonly MessageDispatchChannel unconsumedMessages;
        private readonly LinkedList<MessageDispatch> dispatchedMessages = new LinkedList<MessageDispatch>();
        private readonly ConsumerInfo info;
        private readonly Session session;

		private MessageAck pendingAck = null;

		private readonly Atomic<bool> started = new Atomic<bool>();
		private readonly Atomic<bool> deliveringAcks = new Atomic<bool>();

		private int redeliveryTimeout = 500;
		protected bool disposed = false;
		private long lastDeliveredSequenceId = 0;
		private int ackCounter = 0;
		private int deliveredCounter = 0;
		private int additionalWindowSize = 0;
		private long redeliveryDelay = 0;
		private int dispatchedCount = 0;
		private volatile bool synchronizationRegistered = false;
		private bool clearDispatchList = false;
		private bool inProgressClearRequiredFlag;
		private bool optimizeAcknowledge;
		private DateTime optimizeAckTimestamp = DateTime.Now;
	    private long optimizeAcknowledgeTimeOut = 0;
	    private long optimizedAckScheduledAckInterval = 0;
	    private WaitCallback optimizedAckTask = null;
	    private long failoverRedeliveryWaitPeriod = 0;
	    private bool transactedIndividualAck = false;
	    private bool nonBlockingRedelivery = false;

        private Exception failureError;
		private ThreadPoolExecutor executor;

		private event MessageListener listener;

		private IRedeliveryPolicy redeliveryPolicy;
		private PreviouslyDeliveredMap previouslyDeliveredMessages;

		// Constructor internal to prevent clients from creating an instance.
		internal MessageConsumer(Session session, ConsumerId id, ActiveMQDestination destination,
								 String name, String selector, int prefetch, int maxPendingMessageCount,
								 bool noLocal, bool browser, bool dispatchAsync )
		{
			if(destination == null)
			{
				throw new InvalidDestinationException("Consumer cannot receive on Null Destinations.");
            }
            else if(destination.PhysicalName == null)
            {
                throw new InvalidDestinationException("The destination object was not given a physical name.");
            }
            else if (destination.IsTemporary)
            {
                String physicalName = destination.PhysicalName;

                if(String.IsNullOrEmpty(physicalName))
                {
                    throw new InvalidDestinationException("Physical name of Destination should be valid: " + destination);
                }
    
                String connectionID = session.Connection.ConnectionId.Value;

                if(physicalName.IndexOf(connectionID) < 0)
                {
                    throw new InvalidDestinationException("Cannot use a Temporary destination from another Connection");
                }
    
                if(!session.Connection.IsTempDestinationActive(destination as ActiveMQTempDestination))
                {
                    throw new InvalidDestinationException("Cannot use a Temporary destination that has been deleted");
                }
            }

			this.session = session;
			this.redeliveryPolicy = this.session.Connection.RedeliveryPolicy;
			this.messageTransformation = this.session.Connection.MessageTransformation;

			if(session.Connection.MessagePrioritySupported)
			{
				this.unconsumedMessages = new SimplePriorityMessageDispatchChannel();
			}
			else
			{
				this.unconsumedMessages = new FifoMessageDispatchChannel();
			}

			this.info = new ConsumerInfo();
			this.info.ConsumerId = id;
			this.info.Destination = destination;
			this.info.SubscriptionName = name;
			this.info.Selector = selector;
			this.info.PrefetchSize = prefetch;
			this.info.MaximumPendingMessageLimit = maxPendingMessageCount;
			this.info.NoLocal = noLocal;
			this.info.Browser = browser;
			this.info.DispatchAsync = dispatchAsync;
			this.info.Retroactive = session.Retroactive;
			this.info.Exclusive = session.Exclusive;
			this.info.Priority = session.Priority;
			this.info.ClientId = session.Connection.ClientId;

			// If the destination contained a URI query, then use it to set public properties
			// on the ConsumerInfo
			if(destination.Options != null)
			{
				// Get options prefixed with "consumer.*"
				StringDictionary options = URISupport.GetProperties(destination.Options, "consumer.");
				// Extract out custom extension options "consumer.nms.*"
				StringDictionary customConsumerOptions = URISupport.ExtractProperties(options, "nms.");

				URISupport.SetProperties(this.info, options);
				URISupport.SetProperties(this, customConsumerOptions, "nms.");
			}

	        this.optimizeAcknowledge = session.Connection.OptimizeAcknowledge && 
									   session.IsAutoAcknowledge && !this.info.Browser;
	        
			if (this.optimizeAcknowledge) {
	            this.optimizeAcknowledgeTimeOut = session.Connection.OptimizeAcknowledgeTimeOut;
	            OptimizedAckScheduledAckInterval = session.Connection.OptimizedAckScheduledAckInterval;
	        }

	        this.info.OptimizedAcknowledge = this.optimizeAcknowledge;
	        this.failoverRedeliveryWaitPeriod = session.Connection.ConsumerFailoverRedeliveryWaitPeriod;
	        this.nonBlockingRedelivery = session.Connection.NonBlockingRedelivery;
	        this.transactedIndividualAck = session.Connection.TransactedIndividualAck || this.nonBlockingRedelivery;
		}

		~MessageConsumer()
		{
			Dispose(false);
		}

		#region Property Accessors

		public long LastDeliveredSequenceId
		{
			get { return this.lastDeliveredSequenceId; }
		}

		public ConsumerId ConsumerId
		{
			get { return this.info.ConsumerId; }
		}

		public ConsumerInfo ConsumerInfo
		{
			get { return this.info; }
		}

		public int RedeliveryTimeout
		{
			get { return redeliveryTimeout; }
			set { redeliveryTimeout = value; }
		}

		public int PrefetchSize
		{
			get { return this.info.PrefetchSize; }
		}

		public IRedeliveryPolicy RedeliveryPolicy
		{
			get { return this.redeliveryPolicy; }
			set { this.redeliveryPolicy = value; }
		}

		public long UnconsumedMessageCount
		{
			get { return this.unconsumedMessages.Count; }
		}

		// Custom Options
		private bool ignoreExpiration = false;
		public bool IgnoreExpiration
		{
			get { return ignoreExpiration; }
			set { ignoreExpiration = value; }
		}

        public Exception FailureError
        {
            get { return this.failureError; }
            set { this.failureError = value; }
        }

		public bool OptimizeAcknowledge
		{
			get { return this.optimizeAcknowledge; }
			set 
			{
				if (optimizeAcknowledge && !value)
				{
					DeliverAcks();
				}
				this.optimizeAcknowledge = value;
			}
		}

		public long OptimizeAcknowledgeTimeOut
		{
			get { return this.optimizeAcknowledgeTimeOut; }
			set { this.optimizeAcknowledgeTimeOut = value; }
		}
	    
		public long OptimizedAckScheduledAckInterval
		{
			get { return this.optimizedAckScheduledAckInterval; }
			set 
			{ 
				this.optimizedAckScheduledAckInterval = value; 

		        if (this.optimizedAckTask != null) 
				{
					this.session.Scheduler.Cancel(this.optimizedAckTask);
					this.optimizedAckTask = null;
		        }

		        // Should we periodically send out all outstanding acks.
		        if (this.optimizeAcknowledge && this.optimizedAckScheduledAckInterval > 0)
				{
					this.optimizedAckTask = new WaitCallback(DoOptimizedAck);
					this.session.Scheduler.ExecutePeriodically(
						optimizedAckTask, null, TimeSpan.FromMilliseconds(optimizedAckScheduledAckInterval));
				}
			}
		}
	    
		public long FailoverRedeliveryWaitPeriod 
		{
			get { return this.failoverRedeliveryWaitPeriod; }
			set { this.failoverRedeliveryWaitPeriod = value; }
		}
	    
		public bool TransactedIndividualAck
		{
			get { return this.transactedIndividualAck; }
			set { this.transactedIndividualAck = value; }
		}
	    
		public bool NonBlockingRedelivery
		{
			get { return this.nonBlockingRedelivery; }
			set { this.nonBlockingRedelivery = value; }
		}

		#endregion

		#region IMessageConsumer Members

		private ConsumerTransformerDelegate consumerTransformer;
		/// <summary>
		/// A Delegate that is called each time a Message is dispatched to allow the client to do
		/// any necessary transformations on the received message before it is delivered.
		/// </summary>
		public ConsumerTransformerDelegate ConsumerTransformer
		{
			get { return this.consumerTransformer; }
			set { this.consumerTransformer = value; }
		}

		public event MessageListener Listener
		{
			add
			{
				CheckClosed();

				if(this.PrefetchSize == 0)
				{
					throw new NMSException("Cannot set Asynchronous Listener on a Consumer with a zero Prefetch size");
				}

				bool wasStarted = this.session.Started;

				if(wasStarted)
				{
					this.session.Stop();
				}

				listener += value;
				this.session.Redispatch(this.unconsumedMessages);

				if(wasStarted)
				{
					this.session.Start();
				}
			}
			remove { listener -= value; }
		}

		public IMessage Receive()
		{
			CheckClosed();
			CheckMessageListener();

			SendPullRequest(0);
			MessageDispatch dispatch = this.Dequeue(TimeSpan.FromMilliseconds(-1));

			if(dispatch == null)
			{
				return null;
			}

			BeforeMessageIsConsumed(dispatch);
			AfterMessageIsConsumed(dispatch, false);

			return CreateActiveMQMessage(dispatch);
		}

		public IMessage Receive(TimeSpan timeout)
		{
			CheckClosed();
			CheckMessageListener();

			MessageDispatch dispatch = null;
			SendPullRequest((long) timeout.TotalMilliseconds);

			if(this.PrefetchSize == 0)
			{
				dispatch = this.Dequeue(TimeSpan.FromMilliseconds(-1));
			}
			else
			{
				dispatch = this.Dequeue(timeout);
			}

			if(dispatch == null)
			{
				return null;
			}

			BeforeMessageIsConsumed(dispatch);
			AfterMessageIsConsumed(dispatch, false);

			return CreateActiveMQMessage(dispatch);
		}

		public IMessage ReceiveNoWait()
		{
			CheckClosed();
			CheckMessageListener();

			MessageDispatch dispatch = null;
			SendPullRequest(-1);

			if(this.PrefetchSize == 0)
			{
				dispatch = this.Dequeue(TimeSpan.FromMilliseconds(-1));
			}
			else
			{
				dispatch = this.Dequeue(TimeSpan.Zero);
			}

			if(dispatch == null)
			{
				return null;
			}

			BeforeMessageIsConsumed(dispatch);
			AfterMessageIsConsumed(dispatch, false);

			return CreateActiveMQMessage(dispatch);
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

		public virtual void Close()
		{
			if(!this.unconsumedMessages.Closed)
			{
				if(this.session.IsTransacted && this.session.TransactionContext.InTransaction)
				{
                    Tracer.DebugFormat("Consumer {0} Registering new ConsumerCloseSynchronization",
                                       this.info.ConsumerId);
                    this.session.TransactionContext.AddSynchronization(new ConsumerCloseSynchronization(this));
				}
				else
				{
                    Tracer.DebugFormat("Consumer {0} No Active TX closing normally.",
                                       this.info.ConsumerId);
                    this.DoClose();
				}
			}
		}

		internal void DoClose()
		{
	        Shutdown();
			RemoveInfo removeCommand = new RemoveInfo();
			removeCommand.ObjectId = this.ConsumerId;
	        if (Tracer.IsDebugEnabled) 
			{
				Tracer.DebugFormat("Remove of Consumer[{0}] sent last delivered Id[{1}].", 
				                   this.ConsumerId, this.lastDeliveredSequenceId);
	        }
	        removeCommand.LastDeliveredSequenceId = lastDeliveredSequenceId;
	        this.session.Connection.Oneway(removeCommand);
		}
		
		/// <summary>
		/// Called from the parent Session of this Consumer to indicate that its
		/// parent session is closing and this Consumer should close down but not
		/// send any message to the Broker as the parent close will take care of
		/// removing its child resources at the broker.
		/// </summary>
		internal void Shutdown()
		{
			if(!this.unconsumedMessages.Closed)
			{
				if(Tracer.IsDebugEnabled)
				{
					Tracer.DebugFormat("Shutdown of Consumer[{0}] started.", ConsumerId);
				}

				// Do we have any acks we need to send out before closing?
				// Ack any delivered messages now.
				if(!this.session.IsTransacted)
				{
					DeliverAcks();
					if(this.IsAutoAcknowledgeBatch)
					{
						Acknowledge();
					}
				}

	            if (this.executor != null) 
				{
					this.executor.Shutdown();
					this.executor.AwaitTermination(TimeSpan.FromMinutes(1));
					this.executor = null;
	            }
				if (this.optimizedAckTask != null)
				{
					this.session.Scheduler.Cancel(this.optimizedAckTask);
				}

	            if (this.session.IsClientAcknowledge)
				{
	                if (!this.info.Browser) 
					{
	                    // rollback duplicates that aren't acknowledged
	                    LinkedList<MessageDispatch> temp = null;
					    lock(this.dispatchedMessages)
						{
	                        temp = new LinkedList<MessageDispatch>(this.dispatchedMessages);
	                    }
	                    foreach (MessageDispatch old in temp) 
						{
	                        this.session.Connection.RollbackDuplicate(this, old.Message);
	                    }
	                    temp.Clear();
	                }
	            }

				if(!this.session.IsTransacted)
				{
					lock(this.dispatchedMessages)
					{
						dispatchedMessages.Clear();
					}
				}

				this.session.RemoveConsumer(this);
				this.unconsumedMessages.Close();

	            MessageDispatch[] unconsumed = unconsumedMessages.RemoveAll();
	            if (!this.info.Browser) 
				{
	                foreach (MessageDispatch old in unconsumed) 
					{
	                    // ensure we don't filter this as a duplicate
	                    session.Connection.RollbackDuplicate(this, old.Message);
	                }
	            }
				if(Tracer.IsDebugEnabled)
				{
					Tracer.DebugFormat("Shutdown of Consumer[{0}] completed.", ConsumerId);
				}
			}			
		}

		#endregion

		protected void SendPullRequest(long timeout)
		{
			if(this.info.PrefetchSize == 0 && this.unconsumedMessages.Empty)
			{
				MessagePull messagePull = new MessagePull();
				messagePull.ConsumerId = this.info.ConsumerId;
				messagePull.Destination = this.info.Destination;
				messagePull.Timeout = timeout;
				messagePull.ResponseRequired = false;

				if(Tracer.IsDebugEnabled)
				{
					Tracer.Debug("Sending MessagePull: " + messagePull);
				}

				session.Connection.Oneway(messagePull);
			}
		}

		protected void DoIndividualAcknowledge(ActiveMQMessage message)
		{
			MessageDispatch dispatch = null;

			lock(this.dispatchedMessages)
			{
				foreach(MessageDispatch originalDispatch in this.dispatchedMessages)
				{
					if(originalDispatch.Message.MessageId.Equals(message.MessageId))
					{
						dispatch = originalDispatch;
						this.dispatchedMessages.Remove(originalDispatch);
						break;
					}
				}
			}

			if(dispatch == null)
			{
				Tracer.DebugFormat("Attempt to Ack MessageId[{0}] failed because the original dispatch is not in the Dispatch List", message.MessageId);
				return;
			}

			MessageAck ack = new MessageAck(dispatch, (byte) AckType.IndividualAck, 1);
			Tracer.Debug("Sending Individual Ack for MessageId: " + ack.LastMessageId.ToString());
			this.session.SendAck(ack);
		}

		protected void DoNothingAcknowledge(ActiveMQMessage message)
		{
		}

		protected void DoClientAcknowledge(ActiveMQMessage message)
		{
			this.CheckClosed();
			Tracer.Debug("Sending Client Ack:");
			this.session.Acknowledge();
		}

		public void Start()
		{
			if(this.unconsumedMessages.Closed)
			{
				return;
			}

			this.started.Value = true;
			this.unconsumedMessages.Start();
			this.session.Executor.Wakeup();
		}

		public void Stop()
		{
			this.started.Value = false;
			this.unconsumedMessages.Stop();
		}

		public void DeliverAcks()
		{
			MessageAck ack = null;

			if(this.deliveringAcks.CompareAndSet(false, true))
			{
				if(this.IsAutoAcknowledgeEach)
				{
					lock(this.dispatchedMessages)
					{
						ack = MakeAckForAllDeliveredMessages(AckType.ConsumedAck);
						if(ack != null)
						{
                            Tracer.Debug("Consumer - DeliverAcks clearing the Dispatch list");
							this.dispatchedMessages.Clear();
							this.ackCounter = 0;
						}
						else
						{
							ack = this.pendingAck;
							this.pendingAck = null;
						}
					}
				}
				else if(pendingAck != null && pendingAck.AckType == (byte) AckType.ConsumedAck)
				{
					ack = pendingAck;
					pendingAck = null;
				}

				if(ack != null)
				{
	                if (this.executor == null) 
					{
						this.executor = new ThreadPoolExecutor();
	                }

					this.executor.QueueUserWorkItem(AsyncDeliverAck, ack);
				}
				else
				{
					this.deliveringAcks.Value = false;
				}
			}
		}

		private void AsyncDeliverAck(object ack)
		{
			MessageAck pending = ack as MessageAck;
			try
			{
				this.session.SendAck(pending, true);
			}
			catch
			{
				Tracer.ErrorFormat("Consumer {0} Failed to deliver async Ack {1}",
                                   this.info.ConsumerId, pending);
			}
			finally
			{
				this.deliveringAcks.Value = false;
			}
		}

		internal void InProgressClearRequired()
		{
			inProgressClearRequiredFlag = true;
			// deal with delivered messages async to avoid lock contention with in progress acks
			clearDispatchList = true;
		}

		internal void ClearMessagesInProgress()
		{
			if(inProgressClearRequiredFlag)
			{
				// Called from a thread in the ThreadPool, so we wait until we can
				// get a lock on the unconsumed list then we clear it.
				lock(this.unconsumedMessages.SyncRoot)
				{
					if(inProgressClearRequiredFlag)
					{
						if(Tracer.IsDebugEnabled)
						{
							Tracer.Debug(this.ConsumerId + " clearing dispatched list (" +
										 this.unconsumedMessages.Count + ") on transport interrupt");
						}

    	                // ensure unconsumed are rolledback up front as they may get redelivered to another consumer
	                    MessageDispatch[] list = this.unconsumedMessages.RemoveAll();
	                    if (!this.info.Browser) 
						{
	                        foreach (MessageDispatch old in list) 
							{
	                            session.Connection.RollbackDuplicate(this, old.Message);
	                        }
	                    }

						// allow dispatch on this connection to resume
						this.session.Connection.TransportInterruptionProcessingComplete();
						this.inProgressClearRequiredFlag = false;
					}
				}
			}
		}

	    private void ClearDispatchList() 
		{
	        if (this.clearDispatchList) 
			{
				lock(this.dispatchedMessages)
				{
	                if (this.clearDispatchList) 
					{
	                    if (dispatchedMessages.Count != 0) 
						{
	                        if (session.IsTransacted) 
							{
	                            if (Tracer.IsDebugEnabled) 
								{
									Tracer.DebugFormat("Consumer[{0}]: tracking existing transacted delivered list {1} on transport interrupt",
									                   this.info.ConsumerId, dispatchedMessages.Count);
	                            }
	                            if (previouslyDeliveredMessages == null) 
								{
	                                previouslyDeliveredMessages = new PreviouslyDeliveredMap(session.TransactionContext.TransactionId);
	                            }
	                            foreach (MessageDispatch delivered in dispatchedMessages) 
								{
	                                this.previouslyDeliveredMessages.Add(delivered.Message.MessageId, false);
	                            }
	                        } 
							else 
							{
	                            if (Tracer.IsDebugEnabled) 
								{
									Tracer.DebugFormat("Consumer[{0}]: clearing delivered list {1} on transport interrupt",
									                   this.info.ConsumerId, dispatchedMessages.Count);
	                            }
								this.dispatchedMessages.Clear();
	                            this.pendingAck = null;
	                        }
	                    }
	                    this.clearDispatchList = false;
	                }
	            }
	        }
	    }

		public virtual void Dispatch(MessageDispatch dispatch)
		{
			MessageListener listener = this.listener;
			bool dispatchMessage = false;

			try
			{
				ClearMessagesInProgress();
				ClearDispatchList();

				lock(this.unconsumedMessages.SyncRoot)
				{
					if(!this.unconsumedMessages.Closed)
					{
	                    if(this.info.Browser || !session.Connection.IsDuplicate(this, dispatch.Message)) 
						{
							if(listener != null && this.unconsumedMessages.Running)
							{
								dispatchMessage = true;
							}
							else
							{
	                            if (!this.unconsumedMessages.Running) 
								{
	                                // delayed redelivery, ensure it can be re delivered
	                                session.Connection.RollbackDuplicate(this, dispatch.Message);
	                            }
								this.unconsumedMessages.Enqueue(dispatch);
							}
						}
						else 
						{
	                        if (!this.session.IsTransacted) 
							{
	                            Tracer.Warn("Duplicate dispatch on connection: " + session.Connection.ConnectionId +
	                                        " to consumer: " + ConsumerId + ", ignoring (auto acking) duplicate: " + dispatch);
	                            MessageAck ack = new MessageAck(dispatch, (byte) AckType.IndividualAck, 1);
	                            session.SendAck(ack);
	                        } 
							else
							{
	                            if (Tracer.IsDebugEnabled)
								{
									Tracer.DebugFormat("Consumer[{0}]: tracking transacted redelivery of duplicate: {1}",
									                   this.info.ConsumerId, dispatch.Message);
	                            }
	                            bool needsPoisonAck = false;
	                            lock(this.dispatchedMessages)
								{
	                                if (previouslyDeliveredMessages != null) 
									{
	                                    previouslyDeliveredMessages.Add(dispatch.Message.MessageId, true);
	                                } 
									else 
									{
	                                    // delivery while pending redelivery to another consumer on the same connection
	                                    // not waiting for redelivery will help here
	                                    needsPoisonAck = true;
	                                }
	                            }
	                            if (needsPoisonAck) 
								{
	                                MessageAck poisonAck = new MessageAck(dispatch, (byte) AckType.PoisonAck, 1);
	                                poisonAck.FirstMessageId = dispatch.Message.MessageId;
									BrokerError cause = new BrokerError();
									cause.ExceptionClass = "javax.jms.JMSException";
									cause.Message = "Duplicate dispatch with transacted redeliver pending on another consumer, connection: " + 
													session.Connection.ConnectionId;
	                                Tracer.Warn("Acking duplicate delivery as poison, redelivery must be pending to another" +
	                                            " consumer on this connection, failoverRedeliveryWaitPeriod=" +
	                                            failoverRedeliveryWaitPeriod + ". Message: " + dispatch + ", poisonAck: " + poisonAck);
	                                this.session.SendAck(poisonAck);
	                            } 
								else 
								{
	                                if (transactedIndividualAck) 
									{
	                                    ImmediateIndividualTransactedAck(dispatch);
	                                } 
									else 
									{
	                                    this.session.SendAck(new MessageAck(dispatch, (byte) AckType.DeliveredAck, 1));
	                                }
	                            }
	                        }
						}
					}
				}

				if(dispatchMessage)
				{
					ActiveMQMessage message = CreateActiveMQMessage(dispatch);

					this.BeforeMessageIsConsumed(dispatch);

					try
					{
						bool expired = (!IgnoreExpiration && message.IsExpired());

						if(!expired)
						{
							listener(message);
						}

						this.AfterMessageIsConsumed(dispatch, expired);
					}
					catch(Exception e)
					{
						if(IsAutoAcknowledgeBatch || IsAutoAcknowledgeEach || IsIndividualAcknowledge)
						{
                            // Schedule redelivery and possible dlq processing
                            dispatch.RollbackCause = e;
                            Rollback();
						}
						else
						{
							// Transacted or Client ack: Deliver the next message.
							this.AfterMessageIsConsumed(dispatch, false);
						}

						Tracer.Error(this.info.ConsumerId + " Exception while processing message: " + e);

						// If aborted we stop the abort here and let normal processing resume.
						// This allows the session to shutdown normally and ack all messages
						// that have outstanding acks in this consumer.
						if((Thread.CurrentThread.ThreadState & ThreadState.AbortRequested) == ThreadState.AbortRequested)
						{
							Thread.ResetAbort();
						}
					}
				}

				if(++dispatchedCount % 1000 == 0)
				{
					dispatchedCount = 0;
					Thread.Sleep(1);
				}
			}
			catch(Exception e)
			{
				this.session.Connection.OnSessionException(this.session, e);
			}
		}

		public bool Iterate()
		{
			if(this.listener != null)
			{
				MessageDispatch dispatch = this.unconsumedMessages.DequeueNoWait();
				if(dispatch != null)
				{
					this.Dispatch(dispatch);
					return true;
				}
			}

			return false;
		}

		/// <summary>
		/// Used to get an enqueued message from the unconsumedMessages list. The
		/// amount of time this method blocks is based on the timeout value.  if
		/// timeout == Timeout.Infinite then it blocks until a message is received.
		/// if timeout == 0 then it it tries to not block at all, it returns a
		/// message if it is available if timeout > 0 then it blocks up to timeout
		/// amount of time.  Expired messages will consumed by this method.
		/// </summary>
		/// <param name="timeout">
		/// A <see cref="System.TimeSpan"/>
		/// </param>
		/// <returns>
		/// A <see cref="MessageDispatch"/>
		/// </returns>
		private MessageDispatch Dequeue(TimeSpan timeout)
		{
			DateTime deadline = DateTime.Now;

			if(timeout > TimeSpan.Zero)
			{
				deadline += timeout;
			}

			while(true)
			{
				MessageDispatch dispatch = this.unconsumedMessages.Dequeue(timeout);

				// Grab a single date/time for calculations to avoid timing errors.
				DateTime dispatchTime = DateTime.Now;

				if(dispatch == null)
				{
					if(timeout > TimeSpan.Zero && !this.unconsumedMessages.Closed)
					{
						if(dispatchTime > deadline)
						{
							// Out of time.
							timeout = TimeSpan.Zero;
						}
						else
						{
							// Adjust the timeout to the remaining time.
							timeout = deadline - dispatchTime;
						}
					}
					else
					{
                        // Informs the caller of an error in the event that an async exception
                        // took down the parent connection.
                        if(this.failureError != null)
                        {
                            throw NMSExceptionSupport.Create(this.failureError);
                        }

						return null;
					}
				}
				else if(dispatch.Message == null)
				{
					return null;
				}
				else if(!IgnoreExpiration && dispatch.Message.IsExpired())
				{
					Tracer.DebugFormat("{0} received expired message: {1}", info.ConsumerId, dispatch.Message.MessageId);

					BeforeMessageIsConsumed(dispatch);
					AfterMessageIsConsumed(dispatch, true);
					// Refresh the dispatch time
					dispatchTime = DateTime.Now;

					if(timeout > TimeSpan.Zero && !this.unconsumedMessages.Closed)
					{
						if(dispatchTime > deadline)
						{
							// Out of time.
							timeout = TimeSpan.Zero;
						}
						else
						{
							// Adjust the timeout to the remaining time.
							timeout = deadline - dispatchTime;
						}
					}
				}
				else
				{
					return dispatch;
				}
			}
		}

		public virtual void BeforeMessageIsConsumed(MessageDispatch dispatch)
		{
			this.lastDeliveredSequenceId = dispatch.Message.MessageId.BrokerSequenceId;

			if (!IsAutoAcknowledgeBatch)
			{
			    lock(this.dispatchedMessages)
				{
					this.dispatchedMessages.AddFirst(dispatch);
				}

				if (this.session.IsTransacted)
				{
                	if (this.transactedIndividualAck) 
					{
                    	ImmediateIndividualTransactedAck(dispatch);
                	} 
					else 
					{
						this.AckLater(dispatch, AckType.DeliveredAck);
                	}
				}
			}
		}

		private bool IsOptimizedAckTime()
		{
            // evaluate both expired and normal msgs as otherwise consumer may get stalled
            if (ackCounter + deliveredCounter >= (this.info.PrefetchSize * .65))
			{
				return true;
			}

			if (optimizeAcknowledgeTimeOut > 0)
			{
				DateTime deadline = optimizeAckTimestamp + 
					TimeSpan.FromMilliseconds(optimizeAcknowledgeTimeOut);

				if (DateTime.Now >= deadline)
				{
					return true;
				}
			}

			return false;
		}

		public virtual void AfterMessageIsConsumed(MessageDispatch dispatch, bool expired)
		{
			if(this.unconsumedMessages.Closed)
			{
				return;
			}

			if(expired)
			{
				lock(this.dispatchedMessages)
				{
					this.dispatchedMessages.Remove(dispatch);
				}

				Acknowledge(dispatch, AckType.DeliveredAck);
			}
			else
			{
				if(this.session.IsTransacted)
				{
					// Do nothing.
				}
				else if(this.IsAutoAcknowledgeEach)
				{
					if(this.deliveringAcks.CompareAndSet(false, true))
					{
						lock(this.dispatchedMessages)
						{
							if(this.dispatchedMessages.Count != 0)
							{
	                            if (this.optimizeAcknowledge) 
								{
	                                this.ackCounter++;

	                                if (IsOptimizedAckTime())
									{
	                                    MessageAck ack = MakeAckForAllDeliveredMessages(AckType.ConsumedAck);
	                                    if (ack != null) 
										{
	                                        this.dispatchedMessages.Clear();
	                                        this.ackCounter = 0;
	                                        this.session.SendAck(ack);
	                                        this.optimizeAckTimestamp = DateTime.Now;
	                                    }
	                                    // as further optimization send ack for expired msgs wehn
	                                    // there are any.  This resets the deliveredCounter to 0 so 
										// that we won't sent standard acks with every msg just
	                                    // because the deliveredCounter just below 0.5 * prefetch 
										// as used in ackLater()
	                                    if (this.pendingAck != null && this.deliveredCounter > 0) 
										{
	                                        this.session.SendAck(pendingAck);
	                                        this.pendingAck = null;
	                                        this.deliveredCounter = 0;
	                                    }
	                                }
	                            }
								else 
								{
	                                MessageAck ack = MakeAckForAllDeliveredMessages(AckType.ConsumedAck);
	                                if (ack != null) 
									{
	                                    this.dispatchedMessages.Clear();
	                                    this.session.SendAck(ack);
	                                }
	                            }
							}
						}
						this.deliveringAcks.Value = false;
					}
				}
				else if(this.IsAutoAcknowledgeBatch)
				{
					AckLater(dispatch, AckType.ConsumedAck);
				}
				else if(IsClientAcknowledge || IsIndividualAcknowledge)
				{
					bool messageAckedByConsumer = false;

					lock(this.dispatchedMessages)
					{
						messageAckedByConsumer = this.dispatchedMessages.Contains(dispatch);
					}

					if(messageAckedByConsumer)
					{
						AckLater(dispatch, AckType.DeliveredAck);
					}
				}
				else
				{
					throw new NMSException("Invalid session state.");
				}
			}
		}

		private MessageAck MakeAckForAllDeliveredMessages(AckType type)
		{
			lock(this.dispatchedMessages)
			{
				if(this.dispatchedMessages.Count == 0)
				{
					return null;
				}

				MessageDispatch dispatch = this.dispatchedMessages.First.Value;
				MessageAck ack = new MessageAck(dispatch, (byte) type, this.dispatchedMessages.Count);
				ack.FirstMessageId = this.dispatchedMessages.Last.Value.Message.MessageId;
				return ack;
			}
		}

		private void AckLater(MessageDispatch dispatch, AckType type)
		{
			// Don't acknowledge now, but we may need to let the broker know the
			// consumer got the message to expand the pre-fetch window
			if(this.session.IsTransacted)
			{
				RegisterSync();
			}

			this.deliveredCounter++;

			MessageAck oldPendingAck = pendingAck;

        	pendingAck = new MessageAck(dispatch, (byte) type, deliveredCounter);

			if(this.session.IsTransacted && this.session.TransactionContext.InTransaction)
			{
				pendingAck.TransactionId = this.session.TransactionContext.TransactionId;
			}

			if(oldPendingAck == null)
			{
				pendingAck.FirstMessageId = pendingAck.LastMessageId;
			}
			else if(oldPendingAck.AckType == pendingAck.AckType)
			{
				pendingAck.FirstMessageId = oldPendingAck.FirstMessageId;
			}
			else
			{
				// old pending ack being superseded by ack of another type, if is is not a delivered
				// ack and hence important, send it now so it is not lost.
				if(oldPendingAck.AckType != (byte) AckType.DeliveredAck)
				{
					if(Tracer.IsDebugEnabled)
					{
						Tracer.Debug("Sending old pending ack " + oldPendingAck + ", new pending: " + pendingAck);
					}

					this.session.SendAck(oldPendingAck);
				}
				else
				{
					if(Tracer.IsDebugEnabled)
					{
						Tracer.Debug("dropping old pending ack " + oldPendingAck + ", new pending: " + pendingAck);
					}
				}
			}

	        // evaluate both expired and normal msgs as otherwise consumer may get stalled
			if((0.5 * this.info.PrefetchSize) <= (this.deliveredCounter + this.ackCounter - this.additionalWindowSize))
			{
				this.session.SendAck(pendingAck);
				this.pendingAck = null;
				this.deliveredCounter = 0;
				this.additionalWindowSize = 0;
			}
		}

	    private void ImmediateIndividualTransactedAck(MessageDispatch dispatch)
		{
	        // acks accumulate on the broker pending transaction completion to indicate
	        // delivery status
	        RegisterSync();
	        MessageAck ack = new MessageAck(dispatch, (byte) AckType.IndividualAck, 1);
			ack.TransactionId = session.TransactionContext.TransactionId;
	        this.session.Connection.SyncRequest(ack);
	    }

	    private void RegisterSync()
		{
			// Don't acknowledge now, but we may need to let the broker know the
			// consumer got the message to expand the pre-fetch window
			if(this.session.IsTransacted)
			{
				this.session.DoStartTransaction();

				if(!synchronizationRegistered)
				{
                    Tracer.DebugFormat("Consumer {0} Registering new MessageConsumerSynchronization",
                                       this.info.ConsumerId);
					this.synchronizationRegistered = true;
					this.session.TransactionContext.AddSynchronization(new MessageConsumerSynchronization(this));
				}
			}
		}

	    private void Acknowledge(MessageDispatch dispatch, AckType ackType)
		{
	        MessageAck ack = new MessageAck(dispatch, (byte) ackType, 1);
	        this.session.SendAck(ack);
	        lock(this.dispatchedMessages)
			{
	            dispatchedMessages.Remove(dispatch);
	        }
	    }

		internal void Acknowledge()
		{
        	ClearDispatchList();
        	WaitForRedeliveries();

			lock(this.dispatchedMessages)
			{
				// Acknowledge all messages so far.
				MessageAck ack = MakeAckForAllDeliveredMessages(AckType.ConsumedAck);

				if(ack == null)
				{
					return; // no msgs
				}

				if(this.session.IsTransacted)
				{
                	RollbackOnFailedRecoveryRedelivery();
                    if (!this.session.TransactionContext.InTransaction)
                    {
                        this.session.DoStartTransaction();
                    }
				    ack.TransactionId = this.session.TransactionContext.TransactionId;
				}

				this.session.SendAck(ack);
				this.pendingAck = null;

				// Adjust the counters
				this.deliveredCounter = Math.Max(0, this.deliveredCounter - this.dispatchedMessages.Count);
				this.additionalWindowSize = Math.Max(0, this.additionalWindowSize - this.dispatchedMessages.Count);

				if(!this.session.IsTransacted)
				{
					this.dispatchedMessages.Clear();
				}
			}
		}

		internal void Commit()
		{
			lock(this.dispatchedMessages)
			{
				this.dispatchedMessages.Clear();
				ClearPreviouslyDelivered();
			}

			this.redeliveryDelay = 0;
		}

		internal void Rollback()
		{
			lock(this.unconsumedMessages.SyncRoot)
			{
	            if (this.optimizeAcknowledge) 
				{
	                // remove messages read but not acked at the broker yet through optimizeAcknowledge
	                if (!this.info.Browser) 
					{
	                    lock(this.dispatchedMessages)
						{
	                        for (int i = 0; (i < this.dispatchedMessages.Count) && (i < ackCounter); i++)
							{
	                            // ensure we don't filter this as a duplicate
								MessageDispatch dispatch = this.dispatchedMessages.Last.Value;
								this.dispatchedMessages.RemoveLast();
	                            session.Connection.RollbackDuplicate(this, dispatch.Message);
	                        }
	                    }
	                }
	            }
				lock(this.dispatchedMessages)
				{
                	RollbackPreviouslyDeliveredAndNotRedelivered();
					if(this.dispatchedMessages.Count == 0)
					{
                        Tracer.DebugFormat("Consumer {0} Rolled Back, no dispatched Messages",
                                           this.info.ConsumerId);
						return;
					}

					// Only increase the redelivery delay after the first redelivery..
					MessageDispatch lastMd = this.dispatchedMessages.First.Value;
					int currentRedeliveryCount = lastMd.Message.RedeliveryCounter;

					redeliveryDelay = this.redeliveryPolicy.RedeliveryDelay(currentRedeliveryCount);

					MessageId firstMsgId = this.dispatchedMessages.Last.Value.Message.MessageId;

					foreach(MessageDispatch dispatch in this.dispatchedMessages)
					{
						// Allow the message to update its internal to reflect a Rollback.
						dispatch.Message.OnMessageRollback();
                    	// ensure we don't filter this as a duplicate
                    	session.Connection.RollbackDuplicate(this, dispatch.Message);
					}

					if(this.redeliveryPolicy.MaximumRedeliveries >= 0 &&
					   lastMd.Message.RedeliveryCounter > this.redeliveryPolicy.MaximumRedeliveries)
					{
						// We need to NACK the messages so that they get sent to the DLQ.
                    	MessageAck ack = new MessageAck(lastMd, (byte) AckType.PoisonAck, dispatchedMessages.Count);
                    	
                        if(Tracer.IsDebugEnabled)
                        {
							Tracer.DebugFormat("Consumer {0} Poison Ack of {1} messages aft max redeliveries: {2}",
                                               this.info.ConsumerId, this.dispatchedMessages.Count, this.redeliveryPolicy.MaximumRedeliveries);
                        }

						if (lastMd.RollbackCause != null)
						{
							BrokerError cause = new BrokerError();
							cause.ExceptionClass = "javax.jms.JMSException";
							cause.Message = lastMd.RollbackCause.Message;
							ack.PoisonCause = cause;
						}
                    	ack.FirstMessageId = firstMsgId;

						this.session.SendAck(ack);

						// Adjust the window size.
						additionalWindowSize = Math.Max(0, this.additionalWindowSize - this.dispatchedMessages.Count);

						this.redeliveryDelay = 0;
                    	this.deliveredCounter -= this.dispatchedMessages.Count;
                    	this.dispatchedMessages.Clear();
					}
					else
					{
						// We only send a RedeliveryAck after the first redelivery
						if(currentRedeliveryCount > 0)
						{
                        	MessageAck ack = new MessageAck(lastMd, (byte) AckType.RedeliveredAck, dispatchedMessages.Count);
							ack.FirstMessageId = firstMsgId;
							this.session.SendAck(ack);
						}

						if (this.nonBlockingRedelivery)
						{
							if(redeliveryDelay == 0)
							{
								redeliveryDelay = RedeliveryPolicy.InitialRedeliveryDelay;
							}

	                        if(Tracer.IsDebugEnabled)
	                        {
								Tracer.DebugFormat("Consumer {0} Rolled Back, Re-enque {1} messages in Non-Blocking mode, delay: {2}",
	                                               this.info.ConsumerId, this.dispatchedMessages.Count, redeliveryDelay);
	                        }

                            List<MessageDispatch> pendingRedeliveries =
                                new List<MessageDispatch>(this.dispatchedMessages);
							pendingRedeliveries.Reverse();

							this.deliveredCounter -= this.dispatchedMessages.Count;
							this.dispatchedMessages.Clear();

							this.session.Scheduler.ExecuteAfterDelay(
								NonBlockingRedeliveryCallback, 
								pendingRedeliveries, 
								TimeSpan.FromMilliseconds(redeliveryDelay));
						}
						else 
						{
							// stop the delivery of messages.
							this.unconsumedMessages.Stop();

	                        if(Tracer.IsDebugEnabled)
	                        {
	                            Tracer.DebugFormat("Consumer {0} Rolled Back, Re-enque {1} messages",
	                                               this.info.ConsumerId, this.dispatchedMessages.Count);
	                        }

							foreach(MessageDispatch dispatch in this.dispatchedMessages)
							{
	                            this.unconsumedMessages.EnqueueFirst(dispatch);
							}

							this.deliveredCounter -= this.dispatchedMessages.Count;
							this.dispatchedMessages.Clear();

							if(redeliveryDelay > 0 && !this.unconsumedMessages.Closed)
							{
								DateTime deadline = DateTime.Now.AddMilliseconds(redeliveryDelay);
								ThreadPool.QueueUserWorkItem(this.RollbackHelper, deadline);
							}
							else
							{
								Start();
							}
						}
					}
				}
			}

			// Only redispatch if there's an async listener otherwise a synchronous
			// consumer will pull them from the local queue.
			if(this.listener != null)
			{
				this.session.Redispatch(this.unconsumedMessages);
			}
		}

		private void RollbackHelper(Object arg)
		{
			try
			{
				TimeSpan waitTime = (DateTime) arg - DateTime.Now;

				if(waitTime.CompareTo(TimeSpan.Zero) > 0)
				{
					Thread.Sleep(waitTime);
				}

				this.Start();
			}
			catch(Exception e)
			{
				if(!this.unconsumedMessages.Closed)
				{
					this.session.Connection.OnSessionException(this.session, e);
				}
			}
		}

        private void NonBlockingRedeliveryCallback(object arg) 
		{
            try 
			{
                if (!this.unconsumedMessages.Closed) 
				{
					List<MessageDispatch> pendingRedeliveries = arg as List<MessageDispatch>;

                    foreach (MessageDispatch dispatch in pendingRedeliveries) 
					{
                        session.Dispatch(dispatch);
                    }
                }
            } 
			catch (Exception e) 
			{
				session.Connection.OnAsyncException(e);
            }
        }

		private ActiveMQMessage CreateActiveMQMessage(MessageDispatch dispatch)
		{
			ActiveMQMessage message = dispatch.Message.Clone() as ActiveMQMessage;

			if(this.ConsumerTransformer != null)
			{
				IMessage newMessage = ConsumerTransformer(this.session, this, message);
				if(newMessage != null)
				{
					message = this.messageTransformation.TransformMessage<ActiveMQMessage>(newMessage);
				}
			}

			message.Connection = this.session.Connection;

			if(IsClientAcknowledge)
			{
				message.Acknowledger += new AcknowledgeHandler(DoClientAcknowledge);
			}
			else if(IsIndividualAcknowledge)
			{
				message.Acknowledger += new AcknowledgeHandler(DoIndividualAcknowledge);
			}
			else
			{
				message.Acknowledger += new AcknowledgeHandler(DoNothingAcknowledge);
			}

			return message;
		}

		private void CheckClosed()
		{
			if(this.unconsumedMessages.Closed)
			{
				throw new NMSException("The Consumer has been Closed");
			}
		}

		private void CheckMessageListener()
		{
			if(this.listener != null)
			{
				throw new NMSException("Cannot set Async listeners on Consumers with a prefetch limit of zero");
			}
		}

		protected bool IsAutoAcknowledgeEach
		{
			get
			{
				return this.session.IsAutoAcknowledge ||
					   (this.session.IsDupsOkAcknowledge && this.info.Destination.IsQueue);
			}
		}

	    protected bool IsAutoAcknowledgeBatch
		{
			get { return this.session.IsDupsOkAcknowledge && !this.info.Destination.IsQueue; }
		}

        protected bool IsIndividualAcknowledge
		{
			get { return this.session.IsIndividualAcknowledge; }
		}

        protected bool IsClientAcknowledge
		{
			get { return this.session.IsClientAcknowledge; }
		}

        internal bool IsInUse(ActiveMQTempDestination dest)
        {
            return this.info.Destination.Equals(dest);
        }

	    internal bool Closed
	    {
            get { return this.unconsumedMessages.Closed; }
	    }

	    private void DoOptimizedAck(object state)
		{
			if (this.optimizeAcknowledge && !this.unconsumedMessages.Closed)
			{
				DeliverAcks();
			}
		}
	    
	    private void WaitForRedeliveries() 
		{
	        if (failoverRedeliveryWaitPeriod > 0 && previouslyDeliveredMessages != null) 
			{
				DateTime expiry = DateTime.Now + TimeSpan.FromMilliseconds(failoverRedeliveryWaitPeriod);
	            int numberNotReplayed;
	            do 
				{
	                numberNotReplayed = 0;
	                lock(this.dispatchedMessages)
					{
	                    if (previouslyDeliveredMessages != null) 
						{
							foreach(KeyValuePair<MessageId, bool> entry in previouslyDeliveredMessages)
							{
								if (!entry.Value)
								{
									numberNotReplayed++;
								}
							}
	                    }
	                }
	                if (numberNotReplayed > 0) 
					{
	                    Tracer.Info("waiting for redelivery of " + numberNotReplayed + " in transaction: " +
	                                previouslyDeliveredMessages.TransactionId +  ", to consumer :" + 
						            this.info.ConsumerId);
	                    Thread.Sleep((int) Math.Max(500, failoverRedeliveryWaitPeriod/4));
	                }
	            } 
				while (numberNotReplayed > 0 && expiry < DateTime.Now);
	        }
	    }

     	// called with deliveredMessages locked
	    private void RollbackOnFailedRecoveryRedelivery() 
		{
	        if (previouslyDeliveredMessages != null) 
			{
	            // if any previously delivered messages was not re-delivered, transaction is 
				// invalid and must rollback as messages have been dispatched else where.
	            int numberNotReplayed = 0;
				foreach(KeyValuePair<MessageId, bool> entry in previouslyDeliveredMessages)
				{
					if (!entry.Value)
					{
						numberNotReplayed++;
	                    if (Tracer.IsDebugEnabled) 
						{
	                        Tracer.DebugFormat("previously delivered message has not been replayed in transaction: " +
	                            previouslyDeliveredMessages.TransactionId + " , messageId: " + entry.Key);
	                    }
					}
				}

	            if (numberNotReplayed > 0) 
				{
	                String message = "rolling back transaction (" +
	                     previouslyDeliveredMessages.TransactionId + ") post failover recovery. " + numberNotReplayed +
	                     " previously delivered message(s) not replayed to consumer: " + this.info.ConsumerId;
	                Tracer.Warn(message);
	                throw new TransactionRolledBackException(message);
	            }
	        }
	    }

	     // called with unconsumedMessages && dispatchedMessages locked
	     // remove any message not re-delivered as they can't be replayed to this
	     // consumer on rollback
	    private void RollbackPreviouslyDeliveredAndNotRedelivered() 
		{
	        if (previouslyDeliveredMessages != null) 
			{
				foreach(KeyValuePair<MessageId, bool> entry in previouslyDeliveredMessages)
				{
	                if (!entry.Value) 
					{
	                    RemoveFromDeliveredMessages(entry.Key);
	                }
	            }

	            ClearPreviouslyDelivered();
	        }
	    }

		// Must be called with dispatchedMessages locked
	    private void RemoveFromDeliveredMessages(MessageId key) 
		{
			MessageDispatch toRemove = null;
			foreach(MessageDispatch candidate in this.dispatchedMessages)
			{
				if (candidate.Message.MessageId.Equals(key))
				{
                	session.Connection.RollbackDuplicate(this, candidate.Message);
					toRemove = candidate;
					break;
				}
			}

			if (toRemove != null)
			{
				this.dispatchedMessages.Remove(toRemove);
			}
	    }

	    // called with deliveredMessages locked
	    private void ClearPreviouslyDelivered() 
		{
	        if (previouslyDeliveredMessages != null) 
			{
	            previouslyDeliveredMessages.Clear();
	            previouslyDeliveredMessages = null;
	        }
	    }

		#region Transaction Redelivery Tracker

		class PreviouslyDeliveredMap : Dictionary<MessageId, bool>
		{
			private TransactionId transactionId;
			public TransactionId TransactionId
			{
				get { return this.transactionId; }
			}

			public PreviouslyDeliveredMap(TransactionId transactionId) : base()
			{
				this.transactionId = transactionId;
			}
		}

		#endregion

		#region Nested ISyncronization Types

		class MessageConsumerSynchronization : ISynchronization
		{
			private readonly MessageConsumer consumer;

			public MessageConsumerSynchronization(MessageConsumer consumer)
			{
				this.consumer = consumer;
			}

			public void BeforeEnd()
			{
                Tracer.DebugFormat("MessageConsumerSynchronization - BeforeEnd Called for Consumer {0}.",
                                   this.consumer.ConsumerId);

                if (this.consumer.TransactedIndividualAck) 
				{
                    this.consumer.ClearDispatchList();
                    this.consumer.WaitForRedeliveries();
                    lock(this.consumer.dispatchedMessages)
					{
                        this.consumer.RollbackOnFailedRecoveryRedelivery();
                    }
                } 
				else 
				{
					this.consumer.Acknowledge();
                }

				this.consumer.synchronizationRegistered = false;
			}

			public void AfterCommit()
			{
                Tracer.DebugFormat("MessageConsumerSynchronization - AfterCommit Called for Consumer {0}.",
                                   this.consumer.ConsumerId);
				this.consumer.Commit();
				this.consumer.synchronizationRegistered = false;
			}

			public void AfterRollback()
			{
                Tracer.DebugFormat("MessageConsumerSynchronization - AfterRollback Called for Consumer {0}.",
                                   this.consumer.ConsumerId);
				this.consumer.Rollback();
				this.consumer.synchronizationRegistered = false;
			}
		}

		protected class ConsumerCloseSynchronization : ISynchronization
		{
			private readonly MessageConsumer consumer;

			public ConsumerCloseSynchronization(MessageConsumer consumer)
			{
				this.consumer = consumer;
			}

			public void BeforeEnd()
			{
			}

			public void AfterCommit()
			{
                Tracer.DebugFormat("ConsumerCloseSynchronization - AfterCommit Called for Consumer {0}.",
                                   this.consumer.ConsumerId);
                this.consumer.DoClose();
			}

			public void AfterRollback()
			{
                Tracer.DebugFormat("ConsumerCloseSynchronization - AfterRollback Called for Consumer {0}.",
                                   this.consumer.ConsumerId);
                this.consumer.DoClose();
			}
		}

		#endregion
	}
}
