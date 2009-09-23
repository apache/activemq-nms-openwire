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
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ
{
	/// <summary>
	/// An object capable of sending messages to some destination
	/// </summary>
	public class MessageProducer : IMessageProducer
	{
		private Session session;
        private MemoryUsage usage = null;
		private bool closed = false;
        private object closedLock = new object();
		private readonly ProducerInfo info;
        private int producerSequenceId = 0;

		private MsgDeliveryMode msgDeliveryMode = NMSConstants.defaultDeliveryMode;
		private TimeSpan requestTimeout = NMSConstants.defaultRequestTimeout;
		private TimeSpan msgTimeToLive = NMSConstants.defaultTimeToLive;
		private MsgPriority msgPriority = NMSConstants.defaultPriority;
		private bool disableMessageID = false;
		private bool disableMessageTimestamp = false;
		protected bool disposed = false;

		public MessageProducer(Session session, ProducerInfo info)
		{
			this.session = session;
			this.info = info;
			this.RequestTimeout = session.RequestTimeout;
		}

		~MessageProducer()
		{
			Dispose(false);
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
			lock(closedLock)
			{
				if(closed)
				{
					return;
				}

				try
				{
					session.DisposeOf(info.ProducerId);
				}
				catch(Exception ex)
				{
					Tracer.ErrorFormat("Error during producer close: {0}", ex);
				}

                if(this.usage != null)
                {
                    this.usage.Stop();
                }
                
				session = null;
				closed = true;
			}
		}

		public void Send(IMessage message)
		{
			Send(info.Destination, message, this.msgDeliveryMode, this.msgPriority, this.msgTimeToLive, false);
		}

		public void Send(IDestination destination, IMessage message)
		{
			Send(destination, message, this.msgDeliveryMode, this.msgPriority, this.msgTimeToLive, false);
		}

		public void Send(IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority, TimeSpan timeToLive)
		{
			Send(info.Destination, message, deliveryMode, priority, timeToLive, true);
		}

		public void Send(IDestination destination, IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority, TimeSpan timeToLive)
		{
			Send(destination, message, deliveryMode, priority, timeToLive, true);
		}

		protected void Send(IDestination destination, IMessage message, MsgDeliveryMode deliveryMode, MsgPriority priority, TimeSpan timeToLive, bool specifiedTimeToLive)
		{
			if(null == destination)
			{
				// See if this producer was created without a destination.
				if(null == info.Destination)
				{
					throw new NotSupportedException();
				}

				// The producer was created with a destination, but an invalid destination
				// was specified.
				throw new Apache.NMS.InvalidDestinationException();
			}

			ActiveMQMessage activeMessage = (ActiveMQMessage) message;

			activeMessage.ProducerId = info.ProducerId;
			activeMessage.FromDestination = destination;
			activeMessage.NMSDeliveryMode = deliveryMode;
			activeMessage.NMSPriority = priority;

            // Always set the message Id regardless of the disable flag.
            MessageId id = new MessageId();
            id.ProducerId = info.ProducerId;
            id.ProducerSequenceId = Interlocked.Increment(ref this.producerSequenceId);
            activeMessage.MessageId = id;
            
			if(!disableMessageTimestamp)
			{
				activeMessage.NMSTimestamp = DateTime.UtcNow;
			}

			if(specifiedTimeToLive)
			{
				activeMessage.NMSTimeToLive = timeToLive;
			}

            // Ensure there's room left to send this message            
            if(this.usage != null)
            {
                usage.WaitForSpace();
            }
            
			lock(closedLock)
			{
				if(closed)
				{
					throw new ConnectionClosedException();
				}

				session.DoSend(activeMessage, this, this.usage, this.RequestTimeout);
			}
		}

        public ProducerId ProducerId
        {
            get { return info.ProducerId; }
        }
        
		public MsgDeliveryMode DeliveryMode
		{
			get { return msgDeliveryMode; }
			set { this.msgDeliveryMode = value; }
		}

		public TimeSpan TimeToLive
		{
			get { return msgTimeToLive; }
			set { this.msgTimeToLive = value; }
		}

		public TimeSpan RequestTimeout
		{
			get { return requestTimeout; }
			set { this.requestTimeout = value; }
		}

		public MsgPriority Priority
		{
			get { return msgPriority; }
			set { this.msgPriority = value; }
		}

		public bool DisableMessageID
		{
			get { return disableMessageID; }
			set { this.disableMessageID = value; }
		}

		public bool DisableMessageTimestamp
		{
			get { return disableMessageTimestamp; }
			set { this.disableMessageTimestamp = value; }
		}

		public IMessage CreateMessage()
		{
			return session.CreateMessage();
		}

		public ITextMessage CreateTextMessage()
		{
			return session.CreateTextMessage();
		}

		public ITextMessage CreateTextMessage(string text)
		{
			return session.CreateTextMessage(text);
		}

		public IMapMessage CreateMapMessage()
		{
			return session.CreateMapMessage();
		}

		public IObjectMessage CreateObjectMessage(object body)
		{
			return session.CreateObjectMessage(body);
		}

		public IBytesMessage CreateBytesMessage()
		{
			return session.CreateBytesMessage();
		}

		public IBytesMessage CreateBytesMessage(byte[] body)
		{
			return session.CreateBytesMessage(body);
		}
        
        public void OnProducerAck(ProducerAck ack)
        {
            if(this.usage != null)
            {
                this.usage.DecreaseUsage( ack.Size );
            }
        }
	}
}
