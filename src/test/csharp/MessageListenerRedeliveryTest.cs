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

using System.Threading;
using Apache.NMS.Test;
using Apache.NMS.Policies;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class MessageListenerRedeliveryTest : NMSTestSupport
    {
        private Connection connection;
        private int counter;
        private ISession session;

        [SetUp]
        public override void SetUp()
        {
            this.connection = (Connection) CreateConnection();
            this.connection.RedeliveryPolicy = GetRedeliveryPolicy();

            this.counter = 0;
        }

        [TearDown]
        public override void TearDown()
        {
            this.session = null;

            if(this.connection != null)
            {
                this.connection.Close();
                this.connection = null;
            }
            
            base.TearDown();
        }
    
        protected IRedeliveryPolicy GetRedeliveryPolicy() 
        {
            RedeliveryPolicy redeliveryPolicy = new RedeliveryPolicy();
            redeliveryPolicy.InitialRedeliveryDelay = 1000;
            redeliveryPolicy.MaximumRedeliveries = 3;
            redeliveryPolicy.BackOffMultiplier = (short)2;
            redeliveryPolicy.UseExponentialBackOff = true;
            return redeliveryPolicy;
        }
    
        private void OnMessageListener(IMessage message)
        {        
            counter++;
            if(this.counter <= 4) 
            {
                session.Rollback();
            }
            else
            {
                message.Acknowledge();
                session.Commit();
            }
        }

        [Test]
        public void TestQueueRollbackConsumerListener() 
        {
            connection.Start();
    
            this.session = connection.CreateSession(AcknowledgementMode.Transactional);
            ITemporaryQueue queue = session.CreateTemporaryQueue();
            IMessageProducer producer = session.CreateProducer(queue);
            IMessage message = session.CreateTextMessage("Test Message");
            producer.Send(message);
            session.Commit();
    
            IMessageConsumer consumer = session.CreateConsumer(queue);

            consumer.Listener += new MessageListener(OnMessageListener);
    
            Thread.Sleep(500);

            // first try.. should get 2 since there is no delay on the
            // first redeliver..
            Assert.AreEqual(2, counter);
    
            Thread.Sleep(1000);
    
            // 2nd redeliver (redelivery after 1 sec)
            Assert.AreEqual(3, counter);
    
            Thread.Sleep(2000);
    
            // 3rd redeliver (redelivery after 2 seconds) - it should give up after
            // that
            Assert.AreEqual(4, counter);
    
            // create new message
            producer.Send(session.CreateTextMessage("Test Message Again"));
            session.Commit();
    
            Thread.Sleep(500);
            
            // it should be committed, so no redelivery
            Assert.AreEqual(5, counter);
    
            Thread.Sleep(1500);

            // no redelivery, counter should still be 5
            Assert.AreEqual(5, counter);
    
            session.Close();
        }
       
    }
}
