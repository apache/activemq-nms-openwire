/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.State
{
	public class CommandVisitorAdapter : ICommandVisitor
	{

		public virtual Response processAddConnection(ConnectionInfo info)
		{
			return null;
		}

		public virtual Response processAddConsumer(ConsumerInfo info)
		{
			return null;
		}

		public virtual Response processAddDestination(DestinationInfo info)
		{
			return null;
		}

		public virtual Response processAddProducer(ProducerInfo info)
		{
			return null;
		}

		public virtual Response processAddSession(SessionInfo info)
		{
			return null;
		}

		public virtual Response processBeginTransaction(TransactionInfo info)
		{
			return null;
		}

		public virtual Response processBrokerInfo(BrokerInfo info)
		{
			return null;
		}

		public virtual Response processCommitTransactionOnePhase(TransactionInfo info)
		{
			return null;
		}

		public virtual Response processCommitTransactionTwoPhase(TransactionInfo info)
		{
			return null;
		}

		public virtual Response processEndTransaction(TransactionInfo info)
		{
			return null;
		}

		public virtual Response processFlush(FlushCommand command)
		{
			return null;
		}

		public virtual Response processForgetTransaction(TransactionInfo info)
		{
			return null;
		}

		public virtual Response processKeepAlive(KeepAliveInfo info)
		{
			return null;
		}

		public virtual Response processMessage(Message send)
		{
			return null;
		}

		public virtual Response processMessageAck(MessageAck ack)
		{
			return null;
		}

		public virtual Response processMessageDispatchNotification(MessageDispatchNotification notification)
		{
			return null;
		}

		public virtual Response processMessagePull(MessagePull pull)
		{
			return null;
		}

		public virtual Response processPrepareTransaction(TransactionInfo info)
		{
			return null;
		}

		public virtual Response processProducerAck(ProducerAck ack)
		{
			return null;
		}

		public virtual Response processRecoverTransactions(TransactionInfo info)
		{
			return null;
		}

		public virtual Response processRemoveConnection(ConnectionId id)
		{
			return null;
		}

		public virtual Response processRemoveConsumer(ConsumerId id)
		{
			return null;
		}

		public virtual Response processRemoveDestination(DestinationInfo info)
		{
			return null;
		}

		public virtual Response processRemoveProducer(ProducerId id)
		{
			return null;
		}

		public virtual Response processRemoveSession(SessionId id)
		{
			return null;
		}

		public virtual Response processRemoveSubscription(RemoveSubscriptionInfo info)
		{
			return null;
		}

		public virtual Response processRollbackTransaction(TransactionInfo info)
		{
			return null;
		}

		public virtual Response processShutdown(ShutdownInfo info)
		{
			return null;
		}

		public virtual Response processWireFormat(WireFormatInfo info)
		{
			return null;
		}

		public virtual Response processMessageDispatch(MessageDispatch dispatch)
		{
			return null;
		}

		public virtual Response processControlCommand(ControlCommand command)
		{
			return null;
		}

		public virtual Response processConnectionControl(ConnectionControl control)
		{
			return null;
		}

		public virtual Response processConnectionError(ConnectionError error)
		{
			return null;
		}

		public virtual Response processConsumerControl(ConsumerControl control)
		{
			return null;
		}

	}
}
