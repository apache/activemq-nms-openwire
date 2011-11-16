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
using System.Collections.Generic;
using System.Threading;
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    public abstract class AMQTransactionTestSupport : NMSTestSupport
    {
        private const int MESSAGE_COUNT = 5;
        private const string MESSAGE_TEXT = "message";

        private IConnectionFactory connectionFactory;
        private IConnection connection;
        private ISession session;
        private IMessageConsumer consumer;
        private IMessageProducer producer;
        private IDestination destination;

        private int batchCount = 10;
        private int batchSize = 20;

        // for message listener test
        private LinkedList<IMessage> unackMessages = new LinkedList<IMessage>();
        private LinkedList<IMessage> ackMessages = new LinkedList<IMessage>();
        private bool resendPhase;

        [SetUp]
        public override void SetUp()
        {
            base.SetUp();

            this.connectionFactory = new ConnectionFactory();
            this.resendPhase = false;

            Reconnect();
        }

        [TearDown]
        public override void TearDown()
        {
            this.session.Close();
            this.session = null;
            this.connection.Close();
            this.connection = null;

            this.unackMessages.Clear();
            this.ackMessages.Clear();

            base.TearDown();
        }

        protected abstract bool Topic
        {
            get;
        }

        protected abstract String TestClientId
        {
            get;
        }

        protected abstract String Subscription
        {
            get;
        }

        protected abstract String DestinationName
        {
            get;
        }

        public override IConnection CreateConnection()
        {
            return this.connectionFactory.CreateConnection();
        }

        protected void BeginTx()
        {
        }

        protected void CommitTx()
        {
            session.Commit();
        }

        protected void RollbackTx()
        {
            session.Rollback();
        }

        [Test]
        public void TestSessionCommitedWithoutReceivingMessage()
        {
            Assert.IsTrue(session.Transacted);

            IMessage message = consumer.Receive(new TimeSpan(0, 0, 0, 0, 100));
            Assert.IsNull(message);
            session.Commit();

            Assert.Pass("When getting here. It is ok");
        }

        [Test]
        public void TestSendReceiveTransactedBatches()
        {
            ITextMessage message = session.CreateTextMessage("Batch IMessage");

            for(int j = 0; j < batchCount; j++)
            {
                BeginTx();

                for(int i = 0; i < batchSize; i++)
                {
                    producer.Send(message);
                }

                CommitTx();

                BeginTx();
                for(int i = 0; i < batchSize; i++)
                {
                    message = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(5000));
                    Assert.IsNotNull(message, "Received only " + i + " messages in batch " + j);
                    Assert.AreEqual("Batch IMessage", message.Text);
                }

                CommitTx();
            }
        }

        [Test]
        public void TestSendRollback()
        {
            IMessage[] outbound = new IMessage[]
            {session.CreateTextMessage("First IMessage"), session.CreateTextMessage("Second IMessage")};

            // sends a message
            BeginTx();
            producer.Send(outbound[0]);
            CommitTx();

            // sends a message that gets rollbacked
            BeginTx();
            producer.Send(session.CreateTextMessage("I'm going to get rolled back."));
            RollbackTx();

            // sends a message
            BeginTx();
            producer.Send(outbound[1]);
            CommitTx();

            // receives the first message
            BeginTx();
            LinkedList<IMessage> messages = new LinkedList<IMessage>();
            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            messages.AddLast(message);

            // receives the second message
            message = consumer.Receive(TimeSpan.FromMilliseconds(5000));
            messages.AddLast(message);

            // validates that the rollbacked was not consumed
            CommitTx();
            IMessage[] inbound = new IMessage[messages.Count];
            messages.CopyTo(inbound, 0);
            AssertTextMessagesEqual(outbound, inbound, "Rollback did not work.");
        }

        [Test]
        public void TestSendSessionClose()
        {
            IMessage[] outbound = new IMessage[] {
                session.CreateTextMessage("First IMessage"),
                session.CreateTextMessage("Second IMessage")};

            // sends a message
            BeginTx();
            producer.Send(outbound[0]);
            CommitTx();

            // sends a message that gets rollbacked
            BeginTx();
            producer.Send(session.CreateTextMessage("I'm going to get rolled back."));
            consumer.Close();

            ReconnectSession();

            // sends a message
            producer.Send(outbound[1]);
            CommitTx();

            // receives the first message
            LinkedList<IMessage> messages = new LinkedList<IMessage>();
            BeginTx();
            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(message);
            messages.AddLast(message);

            // receives the second message
            message = consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.IsNotNull(message);
            messages.AddLast(message);

            // validates that the rollbacked was not consumed
            CommitTx();
            IMessage[] inbound = new IMessage[messages.Count];
            messages.CopyTo(inbound, 0);
            AssertTextMessagesEqual(outbound, inbound, "Rollback did not work.");
        }

        [Test]
        public void TestSendSessionAndConnectionClose()
        {
            IMessage[] outbound = new IMessage[] {
                session.CreateTextMessage("First IMessage"),
                session.CreateTextMessage("Second IMessage")};

            // sends a message
            BeginTx();
            producer.Send(outbound[0]);
            CommitTx();

            // sends a message that gets rollbacked
            BeginTx();
            producer.Send(session.CreateTextMessage("I'm going to get rolled back."));
            consumer.Close();
            session.Close();

            Reconnect();

            // sends a message
            BeginTx();
            producer.Send(outbound[1]);
            CommitTx();

            // receives the first message
            LinkedList<IMessage> messages = new LinkedList<IMessage>();
            BeginTx();
            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(message);
            messages.AddLast(message);

            // receives the second message
            message = consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.IsNotNull(message);
            messages.AddLast(message);

            // validates that the rollbacked was not consumed
            CommitTx();
            IMessage[] inbound = new IMessage[messages.Count];
            messages.CopyTo(inbound, 0);
            AssertTextMessagesEqual(outbound, inbound, "Rollback did not work.");
        }

        [Test]
        public void TestReceiveRollback()
        {
            IMessage[] outbound = new IMessage[] {
                session.CreateTextMessage("First IMessage"),
                session.CreateTextMessage("Second IMessage")};

            // lets consume any outstanding messages from prev test runs
            BeginTx();
            bool needCommit = false;
            while(consumer.ReceiveNoWait() != null)
            {
                needCommit = true;
            }

            if(needCommit)
            {
                CommitTx();
            }

            // sent both messages
            BeginTx();
            producer.Send(outbound[0]);
            producer.Send(outbound[1]);
            CommitTx();

            LinkedList<IMessage> messages = new LinkedList<IMessage>();
            BeginTx();
            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(message);
            messages.AddLast(message);
            AssertEquals(outbound[0], message);
            CommitTx();

            // Rollback so we can get that last message again.
            BeginTx();
            message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(message);
            AssertEquals(outbound[1], message);
            RollbackTx();

            // Consume again.. the prev message should
            // get redelivered.
            BeginTx();
            message = consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.IsNotNull(message, "Should have re-received the message again!");
            messages.AddLast(message);
            CommitTx();

            IMessage[] inbound = new IMessage[messages.Count];
            messages.CopyTo(inbound, 0);
            AssertTextMessagesEqual(outbound, inbound, "Rollback did not work.");
        }

        [Test]
        public void TestReceiveTwoThenRollback()
        {
            IMessage[] outbound = new IMessage[] {
                session.CreateTextMessage("First IMessage"),
                session.CreateTextMessage("Second IMessage")};

            // lets consume any outstanding messages from prev test runs
            BeginTx();
            bool needCommit = false;
            while(consumer.ReceiveNoWait() != null)
            {
                needCommit = true;
            }

            if(needCommit)
            {
                CommitTx();
            }

            BeginTx();
            producer.Send(outbound[0]);
            producer.Send(outbound[1]);
            CommitTx();

            LinkedList<IMessage> messages = new LinkedList<IMessage>();
            BeginTx();
            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            AssertEquals(outbound[0], message);

            message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(message);
            AssertEquals(outbound[1], message);
            RollbackTx();

            // Consume again.. the prev message should
            // get redelivered.
            BeginTx();
            message = consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.IsNotNull(message, "Should have re-received the first message again!");
            messages.AddLast(message);
            AssertEquals(outbound[0], message);
            message = consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.IsNotNull(message, "Should have re-received the first message again!");
            messages.AddLast(message);
            AssertEquals(outbound[1], message);

            Assert.IsNull(consumer.ReceiveNoWait());
            CommitTx();

            IMessage[] inbound = new IMessage[messages.Count];
            messages.CopyTo(inbound, 0);
            AssertTextMessagesEqual(outbound, inbound, "Rollback did not work.");
        }

        [Test]
        public void TestSendReceiveWithPrefetchOne() {
            SetPrefetchToOne();
            ReconnectSession();

            IMessage[] outbound = new IMessage[] {
                session.CreateTextMessage("First IMessage"),
                session.CreateTextMessage("Second IMessage"),
                session.CreateTextMessage("Third IMessage"),
                session.CreateTextMessage("Fourth IMessage")};

            BeginTx();
            for(int i = 0; i < outbound.Length; i++)
            {
                // sends a message
                producer.Send(outbound[i]);
            }
            CommitTx();

            // receives the first message
            BeginTx();

            for(int i = 0; i < outbound.Length; i++)
            {
                IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
                Assert.IsNotNull(message);
            }

            // validates that the rollbacked was not consumed
            CommitTx();
        }

        [Test]
        public void TestReceiveTwoThenRollbackManyTimes()
        {
            for(int i = 0; i < 5; i++)
            {
                TestReceiveTwoThenRollback();
            }
        }

        [Test]
        public void TestSendRollbackWithPrefetchOfOne()
        {
            SetPrefetchToOne();
            TestSendRollback();
        }

        [Test]
        public void TestReceiveRollbackWithPrefetchOfOne()
        {
            SetPrefetchToOne();
            TestReceiveRollback();
        }

        [Test]
        public void TestCloseConsumerBeforeCommit()
        {
            ITextMessage[] outbound = new ITextMessage[] {
                session.CreateTextMessage("First IMessage"),
                session.CreateTextMessage("Second IMessage")};

            // lets consume any outstanding messages from prev test runs
            BeginTx();
            bool needCommit = false;
            while(consumer.ReceiveNoWait() != null)
            {
                needCommit = true;
            }

            if(needCommit)
            {
                CommitTx();
            }

            // sends the messages
            BeginTx();
            producer.Send(outbound[0]);
            producer.Send(outbound[1]);
            CommitTx();

            BeginTx();
            ITextMessage message = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.AreEqual(outbound[0].Text, message.Text);
            // Close the consumer before the Commit. This should not cause the
            // received message to Rollback.
            consumer.Close();
            CommitTx();

            // Create a new consumer
            consumer = CreateMessageConsumer();

            BeginTx();
            message = (ITextMessage)consumer.Receive(TimeSpan.FromMilliseconds(1000));
            Assert.IsNotNull(message);
            Assert.AreEqual(outbound[1].Text, message.Text);
            CommitTx();
        }

        protected void Reconnect()
        {
            if(this.connection != null)
            {
                // Close the prev connection.
                this.connection.Close();
                this.connection = null;
            }

            this.session = null;
            this.connection = CreateConnection(this.TestClientId);

            ReconnectSession();
            this.connection.Start();
        }

        protected void ReconnectSession()
        {
            if(this.session != null)
            {
                this.session.Close();
            }

            this.session = this.connection.CreateSession(AcknowledgementMode.Transactional);

            if( this.Topic == true )
            {
                this.destination = this.session.GetTopic(this.DestinationName);
            }
            else
            {
                this.destination = this.session.GetQueue(this.DestinationName);
            }

            this.producer = this.session.CreateProducer(destination);
            this.consumer = CreateMessageConsumer();
        }

        protected IMessageConsumer CreateMessageConsumer()
        {
            if(this.Subscription != null)
            {
                return this.session.CreateDurableConsumer((ITopic) destination, Subscription, null, false);
            }
            else
            {
                return this.session.CreateConsumer(destination);
            }
        }

        protected void SetPrefetchToOne()
        {
            GetPrefetchPolicy().SetAll(1);
        }

        protected PrefetchPolicy GetPrefetchPolicy()
        {
            return ((Connection) connection).PrefetchPolicy;
        }

        [Test]
        public void TestTransactionEventsFired()
        {
            IMessage[] outbound = new IMessage[]
            {session.CreateTextMessage("First IMessage"), session.CreateTextMessage("Second IMessage")};

            session.TransactionStartedListener += TransactionStarted;
            session.TransactionCommittedListener += TransactionCommitted;
            session.TransactionRolledBackListener += TransactionRolledBack;

            // sends a message
            BeginTx();
            producer.Send(outbound[0]);
            Assert.IsTrue(this.transactionStarted);
            CommitTx();
            Assert.IsFalse(this.transactionStarted);
            Assert.IsTrue(this.transactionCommitted);

            // sends a message that gets rollbacked
            BeginTx();
            producer.Send(session.CreateTextMessage("I'm going to get rolled back."));
            Assert.IsTrue(this.transactionStarted);
            RollbackTx();
            Assert.IsFalse(this.transactionStarted);
            Assert.IsTrue(this.transactionRolledBack);

            // sends a message
            BeginTx();
            producer.Send(outbound[1]);
            Assert.IsTrue(this.transactionStarted);
            CommitTx();
            Assert.IsFalse(this.transactionStarted);
            Assert.IsTrue(this.transactionCommitted);

            // receives the first message
            BeginTx();
            LinkedList<IMessage> messages = new LinkedList<IMessage>();
            IMessage message = consumer.Receive(TimeSpan.FromMilliseconds(1000));
            messages.AddLast(message);

            // receives the second message
            message = consumer.Receive(TimeSpan.FromMilliseconds(5000));
            Assert.IsTrue(this.transactionStarted);
            messages.AddLast(message);

            // validates that the rollbacked was not consumed
            CommitTx();
            Assert.IsFalse(this.transactionStarted);
            Assert.IsTrue(this.transactionCommitted);

            IMessage[] inbound = new IMessage[messages.Count];
            messages.CopyTo(inbound, 0);
            AssertTextMessagesEqual(outbound, inbound, "Rollback did not work.");
        }

        [Test]
        public void TestMessageListenerGeneratesTxEvents()
        {
            messageReceived = false;

            session.TransactionStartedListener += TransactionStarted;
            session.TransactionCommittedListener += TransactionCommitted;
            session.TransactionRolledBackListener += TransactionRolledBack;

            // Send messages
            for(int i = 0; i < MESSAGE_COUNT; i++)
            {
                producer.Send(session.CreateTextMessage(MESSAGE_TEXT + i));
            }

            Assert.IsTrue(this.transactionStarted);
            CommitTx();
            Assert.IsFalse(this.transactionStarted);
            Assert.IsTrue(this.transactionCommitted);

            consumer.Listener += new MessageListener(OnAsyncTxMessage);

            // wait receive
            WaitForMessageToBeReceived();
            Assert.IsTrue(this.transactionStarted);

            CommitTx();
            Assert.IsFalse(this.transactionStarted);
            Assert.IsTrue(this.transactionCommitted);
        }

        private bool transactionStarted = false;
        private bool transactionCommitted = false;
        private bool transactionRolledBack = false;
        private bool messageReceived = false;

        public void OnAsyncTxMessage(IMessage message)
        {
            messageReceived = true;
        }

        private void WaitForMessageToBeReceived()
        {
            for(int i = 0; i < 100 && !messageReceived; i++)
            {
                Thread.Sleep(100);
            }

            Assert.IsTrue(messageReceived);
        }

        private void TransactionStarted(ISession session)
        {
            transactionStarted = true;
            transactionCommitted = false;
            transactionRolledBack = false;
        }

        private void TransactionCommitted(ISession session)
        {
            transactionStarted = false;
            transactionCommitted = true;
            transactionRolledBack = false;
        }

        private void TransactionRolledBack(ISession session)
        {
            transactionStarted = false;
            transactionCommitted = false;
            transactionRolledBack = true;
        }

        [Test]
        public void TestMessageListener()
        {
            // Send messages
            for(int i = 0; i < MESSAGE_COUNT; i++)
            {
                producer.Send(session.CreateTextMessage(MESSAGE_TEXT + i));
            }

            CommitTx();
            consumer.Listener += new MessageListener(OnMessage);

            // wait receive
            WaitReceiveUnack();
            Assert.AreEqual(unackMessages.Count, MESSAGE_COUNT);

            // resend phase
            WaitReceiveAck();
            Assert.AreEqual(ackMessages.Count, MESSAGE_COUNT);

            // should no longer re-receive
            consumer.Listener -= new MessageListener(OnMessage);
            Assert.IsNull(consumer.Receive(TimeSpan.FromMilliseconds(500)));
            Reconnect();
        }

        public void OnMessage(IMessage message)
        {
            if(!resendPhase)
            {
                unackMessages.AddLast(message);
                if(unackMessages.Count == MESSAGE_COUNT)
                {
                    try
                    {
                        RollbackTx();
                        resendPhase = true;
                    }
                    catch
                    {
                    }
                }
            }
            else
            {
                ackMessages.AddLast(message);
                if(ackMessages.Count == MESSAGE_COUNT)
                {
                    try
                    {
                        CommitTx();
                    }
                    catch
                    {
                    }
                }
            }
        }

        private void WaitReceiveUnack()
        {
            for(int i = 0; i < 100 && !resendPhase; i++)
            {
                Thread.Sleep(100);
            }

            Assert.IsTrue(resendPhase);
        }

        private void WaitReceiveAck()
        {
            for(int i = 0; i < 100 && ackMessages.Count < MESSAGE_COUNT; i++)
            {
                Thread.Sleep(100);
            }

            Assert.IsFalse(ackMessages.Count < MESSAGE_COUNT);
        }
    }
}
