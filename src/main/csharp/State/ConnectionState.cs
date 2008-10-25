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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.State
{
	public class ConnectionState
	{

		ConnectionInfo info;
		private SynchronizedDictionary<TransactionId, TransactionState> transactions = new SynchronizedDictionary<TransactionId, TransactionState>();
		private SynchronizedDictionary<SessionId, SessionState> sessions = new SynchronizedDictionary<SessionId, SessionState>();
		private SynchronizedCollection<DestinationInfo> tempDestinations = new SynchronizedCollection<DestinationInfo>();
		private AtomicBoolean _shutdown = new AtomicBoolean(false);

		public ConnectionState(ConnectionInfo info)
		{
			this.info = info;
			// Add the default session id.
			addSession(new SessionInfo(info, -1));
		}

		public override String ToString()
		{
			return info.ToString();
		}

		public void reset(ConnectionInfo info)
		{
			this.info = info;
			transactions.Clear();
			sessions.Clear();
			tempDestinations.Clear();
			_shutdown.Value = false;
		}

		public void addTempDestination(DestinationInfo info)
		{
			checkShutdown();
			tempDestinations.Add(info);
		}

		public void removeTempDestination(ActiveMQDestination destination)
		{
			for(int i = tempDestinations.Count - 1; i >= 0; i--)
			{
				DestinationInfo di = tempDestinations[i];
				if(di.Destination.Equals(destination))
				{
					tempDestinations.RemoveAt(i);
				}
			}
		}

		public void addTransactionState(TransactionId id)
		{
			checkShutdown();
			transactions.Add(id, new TransactionState(id));
		}

		/*
		public TransactionState getTransactionState(TransactionId id) {
			return transactions[id];
		}

		public SynchronizedCollection<TransactionState> getTransactionStates() {
			return transactions.Values;
		}

		public SessionState getSessionState(SessionId id) {
			return sessions[id];
		}

		*/

		public TransactionState this[TransactionId id]
		{
			get
			{
				return transactions[id];
			}
		}

		public SynchronizedCollection<TransactionState> TransactionStates
		{
			get
			{
				return transactions.Values;
			}
		}

		public SessionState this[SessionId id]
		{
			get
			{
				return sessions[id];
			}
		}

		public TransactionState removeTransactionState(TransactionId id)
		{
			TransactionState ret = transactions[id];
			transactions.Remove(id);
			return ret;
		}

		public void addSession(SessionInfo info)
		{
			checkShutdown();
			sessions.Add(info.SessionId, new SessionState(info));
		}

		public SessionState removeSession(SessionId id)
		{
			SessionState ret = sessions[id];
			sessions.Remove(id);
			return ret;
		}

		public ConnectionInfo Info
		{
			get
			{
				return info;
			}
		}

		public SynchronizedCollection<SessionId> SessionIds
		{
			get
			{
				return sessions.Keys;
			}
		}

		public SynchronizedCollection<DestinationInfo> TempDestinations
		{
			get
			{
				return tempDestinations;
			}
		}

		public SynchronizedCollection<SessionState> SessionStates
		{
			get
			{
				return sessions.Values;
			}
		}

		private void checkShutdown()
		{
			if(_shutdown.Value)
			{
				throw new ApplicationException("Disposed");
			}
		}

		public void shutdown()
		{
			if(_shutdown.CompareAndSet(false, true))
			{
				foreach(SessionState ss in sessions.Values)
				{
					ss.shutdown();
				}
			}
		}
	}
}
