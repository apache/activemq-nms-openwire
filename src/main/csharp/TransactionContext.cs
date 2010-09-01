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
using System.Collections;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ
{
	public enum TransactionType
    {
        Begin = 0, Prepare = 1, CommitOnePhase = 2, CommitTwoPhase = 3, Rollback = 4, Recover=5, Forget = 6, End = 7
    }
}

namespace Apache.NMS.ActiveMQ
{
	public class TransactionContext
    {
        private TransactionId transactionId;
        private readonly Session session;
        private readonly ArrayList synchronizations = ArrayList.Synchronized(new ArrayList());
        
        public TransactionContext(Session session)
		{
            this.session = session;
        }

        public bool InTransaction
        {
            get{ return this.transactionId != null; }
        }
        
        public TransactionId TransactionId
        {
            get { return transactionId; }
        }
        
        /// <summary>
        /// Method AddSynchronization
        /// </summary>
        public void AddSynchronization(ISynchronization synchronization)
        {
            synchronizations.Add(synchronization);
        }
        
        public void RemoveSynchronization(ISynchronization synchronization)
        {
            synchronizations.Remove(synchronization);
        }
        
        public void Begin()
        {
            if(!InTransaction)
            {
                this.transactionId = this.session.Connection.CreateLocalTransactionId();
                
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = this.session.Connection.ConnectionId;
                info.TransactionId = transactionId;
                info.Type = (int) TransactionType.Begin;
                
                this.session.Connection.Oneway(info);

                if(Tracer.IsDebugEnabled)
                {
                    Tracer.Debug("Begin:" + this.transactionId.ToString());
                }
            }
        }
        
        public void Rollback()
        {
            if(InTransaction)
            {
                this.BeforeEnd();
    
                if(Tracer.IsDebugEnabled)
                {
                    Tracer.Debug("Rollback: "  + this.transactionId +
                                 " syncCount: " +
                                 (synchronizations != null ? synchronizations.Count : 0));
                }
    
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = this.session.Connection.ConnectionId;
                info.TransactionId = transactionId;
                info.Type = (int) TransactionType.Rollback;
                
                this.transactionId = null;
                this.session.Connection.SyncRequest(info);
    
                this.AfterRollback();
                this.synchronizations.Clear();
            }
        }
        
        public void Commit()
        {
            if(InTransaction)
            {
                this.BeforeEnd();
    
                if(Tracer.IsDebugEnabled)
                {
                    Tracer.Debug("Commit: "  + this.transactionId +
                                 " syncCount: " +
                                 (synchronizations != null ? synchronizations.Count : 0));
                }
    
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = this.session.Connection.ConnectionId;
                info.TransactionId = transactionId;
                info.Type = (int) TransactionType.CommitOnePhase;
                
                this.transactionId = null;
                this.session.Connection.SyncRequest(info);
                
                this.AfterCommit();
                this.synchronizations.Clear();
            }
        }

        internal void BeforeEnd()
        {
            lock(this.synchronizations.SyncRoot)
            {
                foreach(ISynchronization synchronization in this.synchronizations)
                {
                    synchronization.BeforeEnd();
                }
            }
        }

        internal void AfterCommit()
        {
            lock(this.synchronizations.SyncRoot)
            {
                foreach(ISynchronization synchronization in this.synchronizations)
                {
                    synchronization.AfterCommit();
                }
            }
        }

        internal void AfterRollback()
        {
            lock(this.synchronizations.SyncRoot)
            {
                foreach(ISynchronization synchronization in this.synchronizations)
                {
                    synchronization.AfterRollback();
                }
            }
        }        
    }
}
