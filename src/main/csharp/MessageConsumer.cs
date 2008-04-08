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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS;

namespace Apache.NMS.ActiveMQ
{
	public enum AckType
	{
		DeliveredAck = 0, // Message delivered but not consumed
		PoisonAck = 1, // Message could not be processed due to poison pill but discard anyway
		ConsumedAck = 2 // Message consumed, discard
	}


	/// <summary>
	/// An object capable of receiving messages from some destination
	/// </summary>
	public class MessageConsumer : IMessageConsumer
	{
		private readonly AcknowledgementMode acknowledgementMode;
		private bool closed = false;
		private readonly Dispatcher dispatcher = new Dispatcher();
		private readonly ConsumerInfo info;
		private int maximumRedeliveryCount = 10;
		private int redeliveryTimeout = 500;
		private Session session;
		private Session ackSession;
		protected bool disposed = false;

		// Constructor internal to prevent clients from creating an instance.
		internal MessageConsumer(Session session, ConsumerInfo info,
								 AcknowledgementMode acknowledgementMode)
		{
			this.session = session;
			this.info = info;
			this.acknowledgementMode = acknowledgementMode;
			if(AcknowledgementMode.AutoAcknowledge == acknowledgementMode)
			{
				this.ackSession = (Session) session.Connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
			}
		}

		~MessageConsumer()
		{
			Dispose(false);
		}

		internal Dispatcher Dispatcher
		{
			get { return this.dispatcher; }
		}

		public ConsumerId ConsumerId
		{
			get { return info.ConsumerId; }
		}

		public int MaximumRedeliveryCount
		{
			get { return maximumRedeliveryCount; }
			set { maximumRedeliveryCount = value; }
		}

		public int RedeliveryTimeout
		{
			get { return redeliveryTimeout; }
			set { redeliveryTimeout = value; }
		}

		#region IMessageConsumer Members

		public event MessageListener Listener
		{
			add
			{
				listener += value;
				session.StartAsyncDelivery(dispatcher);
			}
			remove { listener -= value; }
		}


		public IMessage Receive()
		{
			CheckClosed();
			return SetupAcknowledge(dispatcher.Dequeue());
		}

		public IMessage Receive(System.TimeSpan timeout)
		{
			CheckClosed();
			return SetupAcknowledge(dispatcher.Dequeue(timeout));
		}

		public IMessage ReceiveNoWait()
		{
			CheckClosed();
			return SetupAcknowledge(dispatcher.DequeueNoWait());
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
					// wake up any pending dequeue() call on the dispatcher
					dispatcher.Close();
					session.DisposeOf(info.ConsumerId);

					if(ackSession != null)
					{
						ackSession.Close();
					}
				}
				catch(Exception ex)
				{
					Tracer.ErrorFormat("Error during consumer close: {0}", ex);
				}

				session = null;
				ackSession = null;
				closed = true;
			}
		}

		#endregion

		private event MessageListener listener;

		public void RedeliverRolledBackMessages()
		{
			dispatcher.RedeliverRolledBackMessages();
		}

		/// <summary>
		/// Method Dispatch
		/// </summary>
		/// <param name="message">An ActiveMQMessage</param>
		public void Dispatch(ActiveMQMessage message)
		{
			lock(this)
			{
				if(ackSession != null)
				{
					message.Acknowledger += DoNothingAcknowledge;
					MessageAck ack = CreateMessageAck(message);
					Tracer.Debug("Sending AutoAck: " + ack);
					ackSession.Connection.OneWay(ack);
				}
			}

			dispatcher.Enqueue(message);
		}

		/// <summary>
		/// Dispatch any pending messages to the asynchronous listener
		/// </summary>
		internal void DispatchAsyncMessages()
		{
			while(listener != null)
			{
				IMessage message = dispatcher.DequeueNoWait();
				if(message == null)
				{
					break;
				}

				message = SetupAcknowledge(message);
				// invoke listener. Exceptions caught by the dispatcher thread
				listener(message);
			}
		}

		protected void CheckClosed()
		{
			lock(this)
			{
				if(closed)
				{
					throw new ConnectionClosedException();
				}
			}
		}

		protected IMessage SetupAcknowledge(IMessage message)
		{
			if(null == message)
			{
				return null;
			}

			if(message is ActiveMQMessage)
			{
				ActiveMQMessage activeMessage = (ActiveMQMessage)message;

				if(AcknowledgementMode.ClientAcknowledge == acknowledgementMode)
				{
					activeMessage.Acknowledger += DoClientAcknowledge;
				}
				else if(AcknowledgementMode.AutoAcknowledge != acknowledgementMode)
				{
					activeMessage.Acknowledger += DoNothingAcknowledge;
					DoClientAcknowledge(activeMessage);
				}
			}

			return message;
		}

		protected void DoNothingAcknowledge(ActiveMQMessage message)
		{
		}

		protected void DoClientAcknowledge(ActiveMQMessage message)
		{
			MessageAck ack = CreateMessageAck(message);
			Tracer.Debug("Sending Ack: " + ack);
			session.Connection.OneWay(ack);
		}

		protected virtual MessageAck CreateMessageAck(Message message)
		{
			MessageAck ack = new MessageAck();
			ack.AckType = (int) AckType.ConsumedAck;
			ack.ConsumerId = info.ConsumerId;
			ack.Destination = message.Destination;
			ack.FirstMessageId = message.MessageId;
			ack.LastMessageId = message.MessageId;
			ack.MessageCount = 1;

			if(session.Transacted)
			{
				session.DoStartTransaction();
				ack.TransactionId = session.TransactionContext.TransactionId;
				session.TransactionContext.AddSynchronization(
						new MessageConsumerSynchronization(this, message));
			}
			return ack;
		}

		public void AfterRollback(ActiveMQMessage message)
		{
			// lets redeliver the message again
			message.RedeliveryCounter += 1;
			if(message.RedeliveryCounter > MaximumRedeliveryCount)
			{
				// lets send back a poisoned pill
				MessageAck ack = new MessageAck();
				ack.AckType = (int) AckType.PoisonAck;
				ack.ConsumerId = info.ConsumerId;
				ack.Destination = message.Destination;
				ack.FirstMessageId = message.MessageId;
				ack.LastMessageId = message.MessageId;
				ack.MessageCount = 1;
				session.Connection.OneWay(ack);
			}
			else
			{
				dispatcher.Redeliver(message);
			}
		}
	}


	// TODO maybe there's a cleaner way of creating stateful delegates to make this code neater
	internal class MessageConsumerSynchronization : ISynchronization
	{
		private readonly MessageConsumer consumer;
		private readonly Message message;

		public MessageConsumerSynchronization(MessageConsumer consumer, Message message)
		{
			this.message = message;
			this.consumer = consumer;
		}

		#region ISynchronization Members

		public void BeforeCommit()
		{
		}

		public void AfterCommit()
		{
		}

		public void AfterRollback()
		{
			consumer.AfterRollback((ActiveMQMessage) message);
		}

		#endregion
	}
}