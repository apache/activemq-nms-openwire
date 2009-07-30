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

using Apache.NMS.ActiveMQ.OpenWire;
using Apache.NMS.ActiveMQ.State;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.Commands
{
    public delegate void AcknowledgeHandler(ActiveMQMessage message);

    public class ActiveMQMessage : Message, IMessage, MarshallAware
    {
        public const byte ID_ACTIVEMQMESSAGE = 23;

        private MessagePropertyHelper propertyHelper;
        private PrimitiveMap properties;

        public event AcknowledgeHandler Acknowledger;

        public static ActiveMQMessage Transform(IMessage message)
        {
            return (ActiveMQMessage) message;
        }

        // TODO generate Equals method
        // TODO generate GetHashCode method

        public ActiveMQMessage()
                : base()
        {
            Timestamp = DateUtils.ToJavaTimeUtc(DateTime.UtcNow);
        }

        public override byte GetDataStructureType()
        {
            return ID_ACTIVEMQMESSAGE;
        }

        public void Acknowledge()
        {
            if(null == Acknowledger)
            {
                throw new NMSException("No Acknowledger has been associated with this message: " + this);
            }
            else
            {
                Acknowledger(this);
            }
        }

        #region Properties

        public IPrimitiveMap Properties
        {
            get
            {
                if(null == properties)
                {
                    properties = PrimitiveMap.Unmarshal(MarshalledProperties);
                    propertyHelper = new MessagePropertyHelper(this, properties);
                }

                return propertyHelper;
            }
        }

        public IDestination FromDestination
        {
            get { return Destination; }
            set { this.Destination = ActiveMQDestination.Transform(value); }
        }

        /// <summary>
        /// The correlation ID used to correlate messages with conversations or long running business processes
        /// </summary>
        public string NMSCorrelationID
        {
            get { return CorrelationId; }
            set { CorrelationId = value; }
        }

        /// <summary>
        /// The destination of the message
        /// </summary>
        public IDestination NMSDestination
        {
            get { return Destination; }
        }

        private TimeSpan timeToLive = TimeSpan.FromMilliseconds(0);
        /// <summary>
        /// The time in milliseconds that this message should expire in
        /// </summary>
        public TimeSpan NMSTimeToLive
        {
            get { return timeToLive; }

            set
            {
                timeToLive = value;
                if(timeToLive.TotalMilliseconds > 0)
                {
                    Expiration = Timestamp + (long) timeToLive.TotalMilliseconds;
                }
                else
                {
                    Expiration = 0;
                }
            }
        }

        /// <summary>
        /// The message ID which is set by the provider
        /// </summary>
        public string NMSMessageId
        {
            get
            {
                if(null != MessageId)
                {
                    return BaseDataStreamMarshaller.ToString(MessageId);
                }

                return String.Empty;
            }
        }

        /// <summary>
        /// Whether or not this message is persistent
        /// </summary>
        public MsgDeliveryMode NMSDeliveryMode
        {
            get { return (Persistent ? MsgDeliveryMode.Persistent : MsgDeliveryMode.NonPersistent); }
            set { Persistent = (MsgDeliveryMode.Persistent == value); }
        }

        /// <summary>
        /// The Priority on this message
        /// </summary>
        public MsgPriority NMSPriority
        {
            get { return (MsgPriority) Priority; }
            set { Priority = (byte) value; }
        }

        /// <summary>
        /// Returns true if this message has been redelivered to this or another consumer before being acknowledged successfully.
        /// </summary>
        public bool NMSRedelivered
        {
            get { return (RedeliveryCounter > 0); }
        }

        /// <summary>
        /// The destination that the consumer of this message should send replies to
        /// </summary>
        public IDestination NMSReplyTo
        {
            get { return ReplyTo; }
            set { ReplyTo = ActiveMQDestination.Transform(value); }
        }

        /// <summary>
        /// The timestamp the broker added to the message
        /// </summary>
        public DateTime NMSTimestamp
        {
            get { return DateUtils.ToDateTime(Timestamp); }
            set
            {
                Timestamp = DateUtils.ToJavaTimeUtc(value);
                if(timeToLive.TotalMilliseconds > 0)
                {
                    Expiration = Timestamp + (long) timeToLive.TotalMilliseconds;
                }
            }
        }

        /// <summary>
        /// The type name of this message
        /// </summary>
        public string NMSType
        {
            get { return Type; }
            set { Type = value; }
        }

        #endregion

        #region NMS Extension headers

        /// <summary>
        /// Returns the number of times this message has been redelivered to other consumers without being acknowledged successfully.
        /// </summary>
        public int NMSXDeliveryCount
        {
            get { return RedeliveryCounter + 1; }
        }

        /// <summary>
        /// The Message Group ID used to group messages together to the same consumer for the same group ID value
        /// </summary>
        public string NMSXGroupID
        {
            get { return GroupID; }
            set { GroupID = value; }
        }
        /// <summary>
        /// The Message Group Sequence counter to indicate the position in a group
        /// </summary>
        public int NMSXGroupSeq
        {
            get { return GroupSequence; }
            set { GroupSequence = value; }
        }

        /// <summary>
        /// Returns the ID of the producers transaction
        /// </summary>
        public string NMSXProducerTXID
        {
            get
            {
                TransactionId txnId = OriginalTransactionId;
                if(null == txnId)
                {
                    txnId = TransactionId;
                }

                if(null != txnId)
                {
                    return BaseDataStreamMarshaller.ToString(txnId);
                }

                return String.Empty;
            }
        }

        #endregion

        public object GetObjectProperty(string name)
        {
            return Properties[name];
        }

        public void SetObjectProperty(string name, object value)
        {
            Properties[name] = value;
        }

        // MarshallAware interface
        public override bool IsMarshallAware()
        {
            return true;
        }

        public override void BeforeMarshall(OpenWireFormat wireFormat)
        {
            MarshalledProperties = null;
            if(properties != null)
            {
                MarshalledProperties = properties.Marshal();
            }
        }

        public override Response visit(ICommandVisitor visitor)
        {
            return visitor.processMessage(this);
        }
    }
}

