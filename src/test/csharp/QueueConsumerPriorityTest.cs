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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class QueueConsumerPriorityTest : NMSTestSupport
    {
        protected static string DESTINATION_NAME = "TEST.QueueConsumerPriority";
        protected static string TEST_CLIENT_ID = "QueueConsumerPriorityTestClientId";
        protected static int MSG_COUNT = 50;
        
        private IConnection createConnection(bool start) 
        {
            IConnection conn = CreateConnection(TEST_CLIENT_ID);
            if(start) 
            {
                conn.Start();
            }
            
            return conn;
        }
        
        private void PurgeQueue(IConnection conn, IDestination queue)
        {
            ISession session = conn.CreateSession();
            IMessageConsumer consumer = session.CreateConsumer(queue);
            while(consumer.Receive(TimeSpan.FromMilliseconds(500)) != null)
            {
            }

            consumer.Close();
            session.Close();
        }

        class Producer
        {
            private readonly ISession session;
            private readonly IDestination dest;
            private readonly int count;
            private readonly MsgPriority priority;

            private Thread theThread;

            public Producer(ISession session, IDestination dest, int count, MsgPriority priority)
            {
                this.session = session;
                this.dest = dest;
                this.count = count;
                this.priority = priority;
            }

            public void Start()
            {
                theThread = new Thread(Run);
                theThread.Start();
            }

            public void Join()
            {
                if(theThread != null)
                {
                    theThread.Join();
                }
            }

            public void Run()
            {
                IMessageProducer producer = session.CreateProducer(dest);
                producer.Priority = this.priority;
                for(int i = 0; i < this.count; ++i)
                {
                    ITextMessage message = session.CreateTextMessage("Message Priority = " + (byte) priority);
                    producer.Send(message);
                }
            }
        }

        [Test]
        public void TestPriorityConsumption()
        {
            IConnection conn = createConnection(true);

            Connection connection = conn as Connection;
            Assert.IsNotNull(connection);
            connection.MessagePrioritySupported = true;

            ISession receiverSession = conn.CreateSession();
            ISession senderSession = conn.CreateSession();

            IDestination queue = receiverSession.GetQueue(DESTINATION_NAME);

            PurgeQueue(conn, queue);

            IMessageConsumer consumer = receiverSession.CreateConsumer(queue);

            Producer producer1 = new Producer(senderSession, queue, MSG_COUNT, MsgPriority.High);
            Producer producer2 = new Producer(senderSession, queue, MSG_COUNT, MsgPriority.Low);

            producer1.Start();
            producer2.Start();

            producer1.Join();
            producer2.Join();

            for(int i = 0; i < MSG_COUNT * 2; i++)
            {
                IMessage msg = consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(msg, "Message {0} was null", i);
                Assert.AreEqual(i < MSG_COUNT ? MsgPriority.High : MsgPriority.Low, msg.NMSPriority,
                                "Message {0} priority was wrong", i);
            }
        }
    }
}

