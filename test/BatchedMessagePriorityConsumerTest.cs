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
using NUnit.Framework;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS;
using Apache.NMS.Util;
using Apache.NMS.Test;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class BatchedMessagePriorityConsumerTest : NMSTestSupport
    {
        protected static string DESTINATION_NAME = "queue://TEST.BatchedMessagePriorityConsumerTest";
        protected static string TEST_CLIENT_ID = "BatchedMessagePriorityConsumerTestID";

        [Test]
        public void TestBatchWithLowPriorityFirstAndClientSupport() 
        {
            DoTestBatchWithLowPriorityFirst(true);
        }

        [Test]
        public void testBatchWithLowPriorityFirstAndClientSupportOff() 
        {
            DoTestBatchWithLowPriorityFirst(false);
        }

        protected void DoTestBatchWithLowPriorityFirst(bool clientPrioritySupport)
        {
            using (Connection connection = (Connection) CreateConnection(TEST_CLIENT_ID))
            {
                connection.Start();
                connection.MessagePrioritySupported = clientPrioritySupport;

                using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                using (IQueue queue = (IQueue) SessionUtil.GetDestination(session, DESTINATION_NAME))
                {
                    using (IMessageProducer producer = session.CreateProducer(queue))
                    {
                        producer.Priority = MsgPriority.Lowest;
                        producer.Send(session.CreateTextMessage("1"));
                        producer.Send(session.CreateTextMessage("2"));
                    }

                    using (IMessageProducer producer = session.CreateProducer(queue))
                    {
                        producer.Priority = MsgPriority.Highest;
                        producer.Send(session.CreateTextMessage("3"));
                        producer.Send(session.CreateTextMessage("4"));
                        producer.Send(session.CreateTextMessage("5"));
                    }
                }

                using (ISession session = connection.CreateSession(AcknowledgementMode.Transactional))
                using (IQueue queue = (IQueue) SessionUtil.GetDestination(session, DESTINATION_NAME))
                using (IMessageConsumer consumer = session.CreateConsumer(queue))
                {
                    for (int i = 0; i < 5; i++) 
                    {
                        IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(4000));
                        Tracer.InfoFormat("MessageID: {0}", message.NMSMessageId);
                    }

                    session.Commit();
                }

                using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                using (IQueue queue = SessionUtil.GetDestination(session, DESTINATION_NAME) as IQueue)
                using (IMessageConsumer consumer = session.CreateConsumer(queue))
                {
                    // should be nothing left
                    IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
                    Assert.IsNull(message, "Should be no messages in the Queue");
                }    
            }
        }
    }
}

