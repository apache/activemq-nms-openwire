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

using System;
using System.Collections.Generic;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using System.Collections;

namespace Apache.NMS.ActiveMQ.State
{
    /// <summary>
    /// Tracks the state of a connection so a newly established transport can be
    /// re-initialized to the state that was tracked.
    /// </summary>
    public class ConnectionStateTracker : CommandVisitorAdapter
    {
        private static readonly Tracked TRACKED_RESPONSE_MARKER = new Tracked(null);

        protected Dictionary<ConnectionId, ConnectionState> connectionStates = new Dictionary<ConnectionId, ConnectionState>();

        private bool _trackTransactions;
        private bool _trackTransactionProducers = true;
        private bool _restoreSessions = true;
        private bool _restoreConsumers = true;
        private bool _restoreProducers = true;
        private bool _restoreTransaction = true;
        private bool _trackMessages = true;
        private int _maxCacheSize = 256;
        private int currentCacheSize;
        private readonly Dictionary<MessageId, Message> messageCache = new Dictionary<MessageId, Message>();
        private readonly Queue<MessageId> messageCacheFIFO = new Queue<MessageId>();

        protected void RemoveEldestInCache()
        {
            System.Collections.ICollection ic = messageCacheFIFO;
            lock(ic.SyncRoot)
            {
                while(messageCacheFIFO.Count > MaxCacheSize)
                {
                    messageCache.Remove(messageCacheFIFO.Dequeue());
                    currentCacheSize = currentCacheSize - 1;
                }
            }
        }

        private class RemoveTransactionAction : ThreadSimulator
        {
            private readonly TransactionInfo info;
            private readonly ConnectionStateTracker cst;

            public RemoveTransactionAction(TransactionInfo info, ConnectionStateTracker aCst)
            {
                this.info = info;
                this.cst = aCst;
            }

            public override void Run()
            {
                ConnectionState cs;

				if(cst.connectionStates.TryGetValue(info.ConnectionId, out cs))
				{
					cs.removeTransactionState(info.TransactionId);
				}
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="command"></param>
        /// <returns>null if the command is not state tracked.</returns>
        public Tracked Track(Command command)
        {
            try
            {
                return (Tracked) command.visit(this);
            }
            catch(IOException e)
            {
                throw e;
            }
            catch(Exception e)
            {
                throw new IOException(e.Message);
            }
        }

        public void TrackBack(Command command)
        {
            if(TrackMessages && command != null && command.IsMessage)
            {
                Message message = (Message) command;
                if(message.TransactionId == null)
                {
                    currentCacheSize = currentCacheSize + 1;
                }
            }
        }

        public void DoRestore(ITransport transport)
        {
            // Restore the connections.
            foreach(ConnectionState connectionState in connectionStates.Values)
            {
                ConnectionInfo info = connectionState.Info;
                info.FailoverReconnect = true;
                transport.Oneway(info);

                DoRestoreTempDestinations(transport, connectionState);

                if(RestoreSessions)
                {
                    DoRestoreSessions(transport, connectionState);
                }

                if(RestoreTransaction)
                {
                    DoRestoreTransactions(transport, connectionState);
                }
            }
            //now flush messages
            foreach(Message msg in messageCache.Values)
            {
                transport.Oneway(msg);
            }
        }

        private void DoRestoreTransactions(ITransport transport, ConnectionState connectionState)
        {
            AtomicCollection<TransactionState> transactionStates = connectionState.TransactionStates;

            foreach(TransactionState transactionState in transactionStates)
            {
                // replay the add and remove of short lived producers that may have been
                // involved in the transaction
                foreach (ProducerState producerState in transactionState.ProducerStates)
                {
                    if (Tracer.IsDebugEnabled)
                    {
                        Tracer.Debug("tx replay producer :" + producerState.Info);
                    }
                    transport.Oneway(producerState.Info);
                }
    
                foreach (Command command in transactionState.Commands)
                {
                    if (Tracer.IsDebugEnabled)
                    {
                        Tracer.Debug("tx replay: " + command);
                    }
                    transport.Oneway(command);
                }
    
                foreach (ProducerState producerState in transactionState.ProducerStates)
                {
                    if (Tracer.IsDebugEnabled)
                    {
                        Tracer.Debug("tx remove replayed producer :" + producerState.Info);
                    }

                    RemoveInfo producerRemove = new RemoveInfo();
                    producerRemove.ObjectId = producerState.Info.ProducerId;
                    transport.Oneway(producerRemove);
                }
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="transport"></param>
        /// <param name="connectionState"></param>
        protected void DoRestoreSessions(ITransport transport, ConnectionState connectionState)
        {
            // Restore the connection's sessions
            foreach(SessionState sessionState in connectionState.SessionStates)
            {
                transport.Oneway(sessionState.Info);

                if(RestoreProducers)
                {
                    DoRestoreProducers(transport, sessionState);
                }

                if(RestoreConsumers)
                {
                    DoRestoreConsumers(transport, sessionState);
                }
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="transport"></param>
        /// <param name="sessionState"></param>
        protected void DoRestoreConsumers(ITransport transport, SessionState sessionState)
        {
            // Restore the session's consumers but possibly in pull only (prefetch 0 state) till
            // recovery completes.

			ConnectionState connectionState = null;
			bool connectionInterruptionProcessingComplete = false;

			if(connectionStates.TryGetValue(sessionState.Info.SessionId.ParentId, out connectionState))
			{
				connectionInterruptionProcessingComplete = connectionState.ConnectionInterruptProcessingComplete;
			}
			
			// Restore the session's consumers
            foreach(ConsumerState consumerState in sessionState.ConsumerStates)
            {
                ConsumerInfo infoToSend = consumerState.Info;

                if(!connectionInterruptionProcessingComplete && infoToSend.PrefetchSize > 0 && transport.WireFormat.Version > 5)
                {
                    infoToSend = consumerState.Info.Clone() as ConsumerInfo;
					lock(((ICollection) connectionState.RecoveringPullConsumers).SyncRoot)
					{
						if(!connectionState.RecoveringPullConsumers.ContainsKey(infoToSend.ConsumerId))
						{
							connectionState.RecoveringPullConsumers.Add(infoToSend.ConsumerId, consumerState.Info);
						}
					}
                    infoToSend.PrefetchSize = 0;
                    if(Tracer.IsDebugEnabled)
                    {
                        Tracer.Debug("restore consumer: " + infoToSend.ConsumerId +
                                     " in pull mode pending recovery, overriding prefetch: " +
                                     consumerState.Info.PrefetchSize);
                    }
                }

                if(Tracer.IsDebugEnabled)
                {
                    Tracer.Debug("restore consumer: " + infoToSend.ConsumerId);
                }

                transport.Oneway(infoToSend);
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="transport"></param>
        /// <param name="sessionState"></param>
        protected void DoRestoreProducers(ITransport transport, SessionState sessionState)
        {
            // Restore the session's producers
            foreach(ProducerState producerState in sessionState.ProducerStates)
            {
                transport.Oneway(producerState.Info);
            }
        }

        /// <summary>
        /// </summary>
        /// <param name="transport"></param>
        /// <param name="connectionState"></param>
        protected void DoRestoreTempDestinations(ITransport transport, ConnectionState connectionState)
        {
            // Restore the connection's temp destinations.
            foreach(DestinationInfo destinationInfo in connectionState.TempDestinations)
            {
                transport.Oneway(destinationInfo);
            }
        }

        public override Response processAddDestination(DestinationInfo info)
        {
            if(info != null && info.Destination.IsTemporary)
            {
                ConnectionState cs;

				if(connectionStates.TryGetValue(info.ConnectionId, out cs))
				{
					cs.addTempDestination(info);
				}
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processRemoveDestination(DestinationInfo info)
        {
            if(info != null && info.Destination.IsTemporary)
            {
                ConnectionState cs;
				if(connectionStates.TryGetValue(info.ConnectionId, out cs))
				{
                    cs.removeTempDestination(info.Destination);
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processAddProducer(ProducerInfo info)
        {
            if(info != null && info.ProducerId != null)
            {
                SessionId sessionId = info.ProducerId.ParentId;
                if(sessionId != null)
                {
                    ConnectionId connectionId = sessionId.ParentId;
                    if(connectionId != null)
                    {
                        ConnectionState cs;
						
						if(connectionStates.TryGetValue(connectionId, out cs))
                        {
                            SessionState ss = cs[sessionId];
                            if(ss != null)
                            {
                                ss.addProducer(info);
                            }
                        }
                    }
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processRemoveProducer(ProducerId id)
        {
            if(id != null)
            {
                SessionId sessionId = id.ParentId;
                if(sessionId != null)
                {
                    ConnectionId connectionId = sessionId.ParentId;
                    if(connectionId != null)
                    {
						ConnectionState cs = null;
						
						if(connectionStates.TryGetValue(connectionId, out cs))
                        {
                            SessionState ss = cs[sessionId];
                            if(ss != null)
                            {
                                ss.removeProducer(id);
                            }
                        }
                    }
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processAddConsumer(ConsumerInfo info)
        {
            if(info != null)
            {
                SessionId sessionId = info.ConsumerId.ParentId;
                if(sessionId != null)
                {
                    ConnectionId connectionId = sessionId.ParentId;
                    if(connectionId != null)
                    {
						ConnectionState cs = null;

						if(connectionStates.TryGetValue(connectionId, out cs))
                        {
                            SessionState ss = cs[sessionId];
                            if(ss != null)
                            {
                                ss.addConsumer(info);
                            }
                        }
                    }
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processRemoveConsumer(ConsumerId id)
        {
            if(id != null)
            {
                SessionId sessionId = id.ParentId;
                if(sessionId != null)
                {
                    ConnectionId connectionId = sessionId.ParentId;
                    if(connectionId != null)
                    {
						ConnectionState cs = null;

						if(connectionStates.TryGetValue(connectionId, out cs))
                        {
                            SessionState ss = cs[sessionId];
                            if(ss != null)
                            {
                                ss.removeConsumer(id);
                            }
                        }
                    }
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processAddSession(SessionInfo info)
        {
            if(info != null)
            {
                ConnectionId connectionId = info.SessionId.ParentId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

					if(connectionStates.TryGetValue(connectionId, out cs))
                    {
                        cs.addSession(info);
                    }
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processRemoveSession(SessionId id)
        {
            if(id != null)
            {
                ConnectionId connectionId = id.ParentId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

					if(connectionStates.TryGetValue(connectionId, out cs))
                    {
                        cs.removeSession(id);
                    }
                }
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processAddConnection(ConnectionInfo info)
        {
            if(info != null)
            {
                connectionStates.Add(info.ConnectionId, new ConnectionState(info));
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processRemoveConnection(ConnectionId id)
        {
            if(id != null)
            {
                connectionStates.Remove(id);
            }
            return TRACKED_RESPONSE_MARKER;
        }

        public override Response processMessage(Message send)
        {
            if(send != null)
            {
                if(TrackTransactions && send.TransactionId != null)
                {
                    ProducerId producerId = send.ProducerId;
                    ConnectionId connectionId = producerId.ParentId.ParentId;
                    if(connectionId != null)
                    {
						ConnectionState cs = null;

						if(connectionStates.TryGetValue(connectionId, out cs))
                        {
                            TransactionState transactionState = cs[send.TransactionId];
                            if(transactionState != null)
                            {
                                transactionState.addCommand(send);

                                if (_trackTransactionProducers)
                                {
                                    SessionState ss = cs[producerId.ParentId];
                                    ProducerState producerState = ss[producerId];
                                    producerState.TransactionState = transactionState;
                                }
                            }
                        }
                    }
                    return TRACKED_RESPONSE_MARKER;
                }
                else if(TrackMessages)
                {
                    messageCache.Add(send.MessageId, (Message) send.Clone());
                    RemoveEldestInCache();
                }
            }
            return null;
        }

        public override Response processMessageAck(MessageAck ack)
        {
            if(TrackTransactions && ack != null && ack.TransactionId != null)
            {
                ConnectionId connectionId = ack.ConsumerId.ParentId.ParentId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

					if(connectionStates.TryGetValue(connectionId, out cs))
                    {
                        TransactionState transactionState = cs[ack.TransactionId];
                        if(transactionState != null)
                        {
                            transactionState.addCommand(ack);
                        }
                    }
                }
                return TRACKED_RESPONSE_MARKER;
            }
            return null;
        }

        public override Response processBeginTransaction(TransactionInfo info)
        {
            if(TrackTransactions && info != null && info.TransactionId != null)
            {
                ConnectionId connectionId = info.ConnectionId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

					if(connectionStates.TryGetValue(connectionId, out cs))
                    {
                        cs.addTransactionState(info.TransactionId);
                        TransactionState state = cs[info.TransactionId];
                        state.addCommand(info);
                    }
                }
                return TRACKED_RESPONSE_MARKER;
            }
            return null;
        }

        public override Response processPrepareTransaction(TransactionInfo info)
        {
            if(TrackTransactions && info != null)
            {
                ConnectionId connectionId = info.ConnectionId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

					if(connectionStates.TryGetValue(connectionId, out cs))
                    {
                        TransactionState transactionState = cs[info.TransactionId];
                        if(transactionState != null)
                        {
                            transactionState.addCommand(info);
                        }
                    }
                }
                return TRACKED_RESPONSE_MARKER;
            }
            return null;
        }

        public override Response processCommitTransactionOnePhase(TransactionInfo info)
        {
            if(TrackTransactions && info != null)
            {
                ConnectionId connectionId = info.ConnectionId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

					if(connectionStates.TryGetValue(connectionId, out cs))
                    {
                        TransactionState transactionState = cs[info.TransactionId];
                        if(transactionState != null)
                        {
                            transactionState.addCommand(info);
                            return new Tracked(new RemoveTransactionAction(info, this));
                        }
                    }
                }
            }
            return null;
        }

        public override Response processCommitTransactionTwoPhase(TransactionInfo info)
        {
            if(TrackTransactions && info != null)
            {
                ConnectionId connectionId = info.ConnectionId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

                    if(cs != null)
                    {
                        TransactionState transactionState = cs[info.TransactionId];
                        if(transactionState != null)
                        {
                            transactionState.addCommand(info);
                            return new Tracked(new RemoveTransactionAction(info, this));
                        }
                    }
                }
            }
            return null;
        }

        public override Response processRollbackTransaction(TransactionInfo info)
        {
            if(TrackTransactions && info != null)
            {
                ConnectionId connectionId = info.ConnectionId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

					if(connectionStates.TryGetValue(connectionId, out cs))
                    {
                        TransactionState transactionState = cs[info.TransactionId];
                        if(transactionState != null)
                        {
                            transactionState.addCommand(info);
                            return new Tracked(new RemoveTransactionAction(info, this));
                        }
                    }
                }
            }
            return null;
        }

        public override Response processEndTransaction(TransactionInfo info)
        {
            if(TrackTransactions && info != null)
            {
                ConnectionId connectionId = info.ConnectionId;
                if(connectionId != null)
                {
					ConnectionState cs = null;

					if(connectionStates.TryGetValue(connectionId, out cs))
                    {
                        TransactionState transactionState = cs[info.TransactionId];
                        if(transactionState != null)
                        {
                            transactionState.addCommand(info);
                        }
                    }
                }
                return TRACKED_RESPONSE_MARKER;
            }
            return null;
        }

        public bool RestoreConsumers
        {
            get { return _restoreConsumers; }
            set { _restoreConsumers = value; }
        }

        public bool RestoreProducers
        {
            get { return _restoreProducers; }
            set { _restoreProducers = value; }
        }

        public bool RestoreSessions
        {
            get { return _restoreSessions; }
            set { _restoreSessions = value; }
        }

        public bool TrackTransactions
        {
            get { return _trackTransactions; }
            set { _trackTransactions = value; }
        }

        public bool TrackTransactionProducers
        {
            get { return _trackTransactionProducers; }
            set { _trackTransactionProducers = value; }
        }

        public bool RestoreTransaction
        {
            get { return _restoreTransaction; }
            set { _restoreTransaction = value; }
        }

        public bool TrackMessages
        {
            get { return _trackMessages; }
            set { _trackMessages = value; }
        }

        public int MaxCacheSize
        {
            get { return _maxCacheSize; }
            set { _maxCacheSize = value; }
        }

        public void ConnectionInterruptProcessingComplete(ITransport transport, ConnectionId connectionId)
        {
			ConnectionState connectionState = null;

			if(connectionStates.TryGetValue(connectionId, out connectionState))
            {
                connectionState.ConnectionInterruptProcessingComplete = true;

				lock(((ICollection) connectionState.RecoveringPullConsumers).SyncRoot)
				{
					foreach(KeyValuePair<ConsumerId, ConsumerInfo> entry in connectionState.RecoveringPullConsumers)
					{
						ConsumerControl control = new ConsumerControl();
						control.ConsumerId = entry.Key;
						control.Prefetch = entry.Value.PrefetchSize;
						control.Destination = entry.Value.Destination;
						try
						{
							if(Tracer.IsDebugEnabled)
							{
								Tracer.Debug("restored recovering consumer: " + control.ConsumerId +
											 " with: " + control.Prefetch);
							}
							transport.Oneway(control);
						}
						catch(Exception ex)
						{
							if(Tracer.IsDebugEnabled)
							{
								Tracer.Debug("Failed to submit control for consumer: " + control.ConsumerId +
											 " with: " + control.Prefetch + "Error: " + ex.Message);
							}
						}
					}
					connectionState.RecoveringPullConsumers.Clear();
				}
            }
        }

        public void TransportInterrupted(ConnectionId id)
        {
			ConnectionState connection = null;

			if(connectionStates.TryGetValue(id, out connection))
            {
                connection.ConnectionInterruptProcessingComplete = false;
            }
        }
    }
}
