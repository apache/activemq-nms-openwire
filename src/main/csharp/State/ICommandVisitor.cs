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
    public interface ICommandVisitor
    {

        Response processAddConnection(ConnectionInfo info);

        Response processAddSession(SessionInfo info);

        Response processAddProducer(ProducerInfo info);

        Response processAddConsumer(ConsumerInfo info);

        Response processRemoveConnection(ConnectionId id);

        Response processRemoveSession(SessionId id);

        Response processRemoveProducer(ProducerId id);

        Response processRemoveConsumer(ConsumerId id);

        Response processAddDestination(DestinationInfo info);

        Response processRemoveDestination(DestinationInfo info);

        Response processRemoveSubscriptionInfo(RemoveSubscriptionInfo info);

        Response processMessage(Message send);

        Response processMessageAck(MessageAck ack);

        Response processMessagePull(MessagePull pull);

        Response processBeginTransaction(TransactionInfo info);

        Response processPrepareTransaction(TransactionInfo info);

        Response processCommitTransactionOnePhase(TransactionInfo info);

        Response processCommitTransactionTwoPhase(TransactionInfo info);

        Response processRollbackTransaction(TransactionInfo info);

        Response processWireFormat(WireFormatInfo info);

        Response processKeepAliveInfo(KeepAliveInfo info);

        Response processShutdownInfo(ShutdownInfo info);

        Response processFlushCommand(FlushCommand command);

        Response processBrokerInfo(BrokerInfo info);

        Response processRecoverTransactions(TransactionInfo info);

        Response processForgetTransaction(TransactionInfo info);

        Response processEndTransaction(TransactionInfo info);

        Response processMessageDispatchNotification(MessageDispatchNotification notification);

        Response processProducerAck(ProducerAck ack);

        Response processMessageDispatch(MessageDispatch dispatch);

        Response processControlCommand(ControlCommand command);

        Response processConnectionError(ConnectionError error);

        Response processConnectionControl(ConnectionControl control);

        Response processConsumerControl(ConsumerControl control);

        Response processResponse(Response response);

        Response processReplayCommand(ReplayCommand replayCommand);

    }
}
