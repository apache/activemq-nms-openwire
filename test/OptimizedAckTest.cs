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
using Apache.NMS.Test;
using NUnit.Framework;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
	public class OptimizedAckTest : NMSTestSupport
	{
        private Connection connection;
		private int counter;

        [SetUp]
        public override void SetUp()
        {
        	connection = (Connection) CreateConnection();
        	connection.OptimizeAcknowledge = true;
        	connection.OptimizeAcknowledgeTimeOut = 0;
			connection.PrefetchPolicy.All = 100;

			counter = 0;
        }

        [TearDown]
        public override void TearDown()
        {
            if(this.connection != null)
            {
                this.connection.Close();
                this.connection = null;
            }
            
            base.TearDown();
        }

		[Test]
	    public void TestOptimizedAckWithExpiredMsgs()
	    {
	        ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
	        IDestination destination = session.GetQueue("TestOptimizedAckWithExpiredMsgs");
	        IMessageConsumer consumer = session.CreateConsumer(destination);
	        IMessageProducer producer = session.CreateProducer(destination);
	        producer.DeliveryMode = MsgDeliveryMode.NonPersistent;

	        ITextMessage message;

	        // Produce msgs that will expire quickly
	        for (int i = 0; i < 45; i++) 
			{
	            message = session.CreateTextMessage();
	            producer.Send(message, 
				              MsgDeliveryMode.NonPersistent, 
				              MsgPriority.Normal, 
				              TimeSpan.FromMilliseconds(200));
	        }
	        
			// Produce msgs that don't expire
	        for (int i=0; i < 60; i++) 
			{
	            message = session.CreateTextMessage();
	            producer.Send(message, 
				              MsgDeliveryMode.NonPersistent, 
				              MsgPriority.Normal, 
				              TimeSpan.FromMilliseconds(60000));
	        }

	        Thread.Sleep(1000);  // let the batch of 45 expire.

            consumer.Listener += OnMessage;
	        connection.Start();

			for (int i = 0; i < 60; ++i) 
			{
				if (counter == 60)
				{
					break;
				}
				Thread.Sleep(1000);
			}

			Assert.AreEqual(60, counter, "Failed to receive all expected messages");

	        // Cleanup
	        producer.Close();
	        consumer.Close();
	        session.Close();
	        connection.Close();
	    }

	    [Test]
	    public void TestOptimizedAckWithExpiredMsgsSync()
	    {
	        ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
	        IDestination destination = session.GetQueue("TestOptimizedAckWithExpiredMsgs");
	        IMessageConsumer consumer = session.CreateConsumer(destination);
	        IMessageProducer producer = session.CreateProducer(destination);
	        producer.DeliveryMode = MsgDeliveryMode.NonPersistent;

	        ITextMessage message;

	        // Produce msgs that will expire quickly
	        for (int i = 0; i < 45; i++) 
			{
	            message = session.CreateTextMessage();
	            producer.Send(message, 
				              MsgDeliveryMode.NonPersistent, 
				              MsgPriority.Normal, 
				              TimeSpan.FromMilliseconds(200));
	        }
	        
			// Produce msgs that don't expire
	        for (int i=0; i < 60; i++) 
			{
	            message = session.CreateTextMessage();
	            producer.Send(message, 
				              MsgDeliveryMode.NonPersistent, 
				              MsgPriority.Normal, 
				              TimeSpan.FromMilliseconds(60000));
	        }

			Thread.Sleep(1000);
			connection.Start();

	        int counter = 0;
	        for (; counter < 60; ++counter) 
			{
	            Assert.IsNotNull(consumer.Receive(TimeSpan.FromMilliseconds(5000)));
	        }

			Assert.AreEqual(60, counter, "Failed to receive all expected messages");
			Assert.IsNull(consumer.Receive(TimeSpan.FromMilliseconds(2000)));

	        // Cleanup
	        producer.Close();
	        consumer.Close();
	        session.Close();
	        connection.Close();
	    }

	    [Test]
	    public void testOptimizedAckWithExpiredMsgsSync2()
	    {
	        ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
	        IDestination destination = session.GetQueue("TestOptimizedAckWithExpiredMsgs");
	        IMessageConsumer consumer = session.CreateConsumer(destination);
	        IMessageProducer producer = session.CreateProducer(destination);
	        producer.DeliveryMode = MsgDeliveryMode.NonPersistent;

	        ITextMessage message;

	        // Produce msgs that don't expire
	        for (int i = 0; i < 56; i++) 
			{
	            message = session.CreateTextMessage();
	            producer.Send(message, 
				              MsgDeliveryMode.NonPersistent, 
				              MsgPriority.Normal, 
				              TimeSpan.FromMilliseconds(60000));
	        }
	        
	        // Produce msgs that will expire quickly
	        for (int i=0; i<44; i++) 
		    {
	            message = session.CreateTextMessage();
	            producer.Send(message, 
				              MsgDeliveryMode.NonPersistent, 
				              MsgPriority.Normal, 
				              TimeSpan.FromMilliseconds(200));
	        }

	        // Produce some moremsgs that don't expire
	        for (int i=0; i<4; i++) 
			{
	            message = session.CreateTextMessage();
	            producer.Send(message, 
				              MsgDeliveryMode.NonPersistent, 
				              MsgPriority.Normal, 
				              TimeSpan.FromMilliseconds(60000));
	        }

			Thread.Sleep(1000);
			connection.Start();

	        int counter = 0;
	        for (; counter < 60; ++counter) 
			{
	            Assert.IsNotNull(consumer.Receive(TimeSpan.FromMilliseconds(5000)));
	        }

			Assert.AreEqual(60, counter, "Failed to receive all expected messages");
			Assert.IsNull(consumer.Receive(TimeSpan.FromMilliseconds(2000)));

	        // Cleanup
	        producer.Close();
	        consumer.Close();
	        session.Close();
	        connection.Close();
	    }

        private void OnMessage(IMessage msg)
        {
            counter++;
		}
	}
}

