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
using System.Text;
using System.Transactions;
using System.Collections;
using System.Collections.Generic;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transactions;

namespace Apache.NMS.ActiveMQ
{
	public enum TransactionType
    {
        Begin = 0, Prepare = 1, CommitOnePhase = 2, CommitTwoPhase = 3, Rollback = 4, Recover=5, Forget = 6, End = 7
    }
}

namespace Apache.NMS.ActiveMQ
{
	public class TransactionContext : ISinglePhaseNotification
    {
        private const int XA_OK = 0;
        private const int XA_READONLY = 3;

        private TransactionId transactionId;
        private readonly Session session;
        private readonly Connection connection;
        private readonly ArrayList synchronizations = ArrayList.Synchronized(new ArrayList());
        private Enlistment currentEnlistment;

        public TransactionContext(Session session)
		{
            this.session = session;
            this.connection = session.Connection;
        }

        public bool InTransaction
        {
            get{ return this.transactionId != null; }
        }

        public bool InLocalTransaction
        {
            get{ return this.transactionId != null && this.currentEnlistment == null; }
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
            try
            {
                lock(this.synchronizations.SyncRoot)
                {
                    foreach(ISynchronization synchronization in this.synchronizations)
                    {
                        synchronization.AfterCommit();
                    }
                }
            }
            finally
            {
                synchronizations.Clear();
            }
        }

        internal void AfterRollback()
        {
            try
            {
                lock(this.synchronizations.SyncRoot)
                {
                    foreach(ISynchronization synchronization in this.synchronizations)
                    {
                        synchronization.AfterRollback();
                    }
                }
            }
            finally
            {
                synchronizations.Clear();
            }
        }

        #region Transaction Members used when dealing with .NET System Transactions.

        public bool InNetTransaction
        {
            get{ return this.transactionId != null && this.transactionId is XATransactionId; }
        }

        public void Begin(Transaction transaction)
        {
            Tracer.Debug("Begin notification received");

            if(InNetTransaction)
            {
                throw new TransactionInProgressException("A Transaction is already in Progress");
            }

            Guid rmId = ResourceManagerGuid;

            // Enlist this object in the transaction.
            this.currentEnlistment =
                transaction.EnlistDurable(rmId, this, EnlistmentOptions.None);

            Tracer.Debug("Enlisted in Durable Transaction with RM Id: " + rmId);

            TransactionInformation txInfo = transaction.TransactionInformation;

            XATransactionId xaId = new XATransactionId();
            this.transactionId = xaId;

            if(txInfo.DistributedIdentifier != Guid.Empty)
            {
                xaId.GlobalTransactionId = txInfo.DistributedIdentifier.ToByteArray();
                xaId.BranchQualifier = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());
            }
            else
            {
                xaId.GlobalTransactionId = Encoding.UTF8.GetBytes(txInfo.LocalIdentifier);
                xaId.BranchQualifier = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());
            }

            // Now notify the broker that a new XA'ish transaction has started.
            TransactionInfo info = new TransactionInfo();
            info.ConnectionId = this.connection.ConnectionId;
            info.TransactionId = this.transactionId;
            info.Type = (int) TransactionType.Begin;

            this.session.Connection.Oneway(info);

            if(Tracer.IsDebugEnabled)
            {
                Tracer.Debug("Began XA'ish Transaction:" + xaId.GlobalTransactionId.ToString());
            }
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            try
            {
	            Tracer.Debug("Prepare notification received");
				
                BeforeEnd();

                // Before sending the request to the broker, log the recovery bits, if
                // this fails we can't prepare and the TX should be rolled back.
                RecoveryLogger.LogRecoveryInfo(this.transactionId as XATransactionId,
                                               preparingEnlistment.RecoveryInformation());

	            // Inform the broker that work on the XA'sh TX Branch is complete.
	            TransactionInfo info = new TransactionInfo();
	            info.ConnectionId = this.connection.ConnectionId;
	            info.TransactionId = this.transactionId;
                info.Type = (int) TransactionType.End;

                this.connection.CheckConnected();
				this.connection.SyncRequest(info);

                // Prepare the Transaction for commit.
                info.Type = (int) TransactionType.Prepare;
                IntegerResponse response = (IntegerResponse) this.connection.SyncRequest(info);
                if(response.Result == XA_READONLY)
                {
                    Tracer.Debug("Transaction Prepare Reports Done with no need to Commit: ");

                    this.transactionId = null;
                    this.currentEnlistment = null;

                    // Read Only means there's nothing to recover because there was no
                    // change on the broker.
                    RecoveryLogger.LogRecovered(this.transactionId as XATransactionId);

                    // if server responds that nothing needs to be done, then reply prepared
                    // but clear the current state data so we appear done to the commit method.
                    preparingEnlistment.Prepared();

                    // Done so commit won't be called.
                    AfterCommit();
                }
                else
                {
                    Tracer.Debug("Transaction Prepare finished Successfully: ");

                    // If work finished correctly, reply prepared
                    preparingEnlistment.Prepared();
                }
            }
            catch(Exception ex)
            {
                Tracer.Debug("Transaction Prepare failed with error: " + ex.Message);
                AfterRollback();
                preparingEnlistment.ForceRollback();
                try
                {
                    this.connection.OnException(ex);
                }
                catch (Exception error)
                {
                    Tracer.Error(error.ToString());
                }

                this.currentEnlistment = null;
                this.transactionId = null;
            }
        }

        public void Commit(Enlistment enlistment)
        {
            try
            {
                Tracer.Debug("Commit notification received");

                if (this.transactionId != null)
                {
                    // Now notify the broker that a new XA'ish transaction has completed.
                    TransactionInfo info = new TransactionInfo();
                    info.ConnectionId = this.connection.ConnectionId;
                    info.TransactionId = this.transactionId;
                    info.Type = (int) TransactionType.CommitTwoPhase;

                    this.connection.CheckConnected();
                    this.connection.SyncRequest(info);

                    Tracer.Debug("Transaction Commit Reports Done: ");

                    RecoveryLogger.LogRecovered(this.transactionId as XATransactionId);

                    // if server responds that nothing needs to be done, then reply done.
                    enlistment.Done();

                    AfterCommit();
                }
            }
            catch(Exception ex)
            {
                Tracer.Debug("Transaction Commit failed with error: " + ex.Message);
                AfterRollback();
                try
                {
                    this.connection.OnException(ex);
                }
                catch (Exception error)
                {
                    Tracer.Error(error.ToString());
                }
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;

                CountDownLatch latch = this.recoveryComplete;
                if(latch != null)
                {
                    latch.countDown();
                }
            }
        }

        public void SinglePhaseCommit(SinglePhaseEnlistment enlistment)
        {
            Tracer.Debug("Single Phase Commit notification received");

            try
            {
                if(this.transactionId != null)
                {
                	BeforeEnd();

					// Now notify the broker that a new XA'ish transaction has completed.
                    TransactionInfo info = new TransactionInfo();
                    info.ConnectionId = this.connection.ConnectionId;
                    info.TransactionId = this.transactionId;
                    info.Type = (int) TransactionType.CommitOnePhase;

                    this.connection.CheckConnected();
                    this.connection.SyncRequest(info);

                    Tracer.Debug("Transaction Single Phase Commit Reports Done: ");

                    // if server responds that nothing needs to be done, then reply done.
                    enlistment.Done();

                    AfterCommit();
                }
            }
            catch(Exception ex)
            {
                Tracer.Debug("Transaction Single Phase Commit failed with error: " + ex.Message);
                AfterRollback();
                enlistment.Done();
                try
                {
                    this.connection.OnException(ex);
                }
                catch (Exception error)
                {
                    Tracer.Error(error.ToString());
                }
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;
            }
        }
		
        public void Rollback(Enlistment enlistment)
        {
            try
            {
	            Tracer.Debug("Rollback notification received");

                if (this.transactionId != null)
                {
                    BeforeEnd();

                    // Now notify the broker that a new XA'ish transaction has started.
                    TransactionInfo info = new TransactionInfo();
                    info.ConnectionId = this.connection.ConnectionId;
                    info.TransactionId = this.transactionId;
                    info.Type = (int) TransactionType.End;

                    this.connection.CheckConnected();
                    this.connection.SyncRequest(info);

                    info.Type = (int) TransactionType.Rollback;
                    this.connection.CheckConnected();
                    this.connection.SyncRequest(info);

                    Tracer.Debug("Transaction Rollback Reports Done: ");

                    RecoveryLogger.LogRecovered(this.transactionId as XATransactionId);

                    // if server responds that nothing needs to be done, then reply done.
                    enlistment.Done();

                    AfterRollback();
                }
            }
            catch(Exception ex)
            {
                Tracer.Debug("Transaction Rollback failed with error: " + ex.Message);
                AfterRollback();
                try
                {
                    this.connection.OnException(ex);
                }
                catch (Exception error)
                {
                    Tracer.Error(error.ToString());
                }
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;

                CountDownLatch latch = this.recoveryComplete;
                if (latch != null)
                {
                    latch.countDown();
                }
            }
        }

        public void InDoubt(Enlistment enlistment)
        {
            try
            {
	            Tracer.Debug("In doubt notification received, Rolling Back TX");
				
                BeforeEnd();

                // Now notify the broker that Rollback should be performed.
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = this.connection.ConnectionId;
                info.TransactionId = this.transactionId;
                info.Type = (int)TransactionType.End;

                this.connection.CheckConnected();
                this.connection.SyncRequest(info);

                info.Type = (int)TransactionType.Rollback;
                this.connection.CheckConnected();
                this.connection.SyncRequest(info);

                Tracer.Debug("InDoubt Transaction Rollback Reports Done: ");

                RecoveryLogger.LogRecovered(this.transactionId as XATransactionId);

                // if server responds that nothing needs to be done, then reply done.
                enlistment.Done();

                AfterRollback();
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;

                CountDownLatch latch = this.recoveryComplete;
                if (latch != null)
                {
                    latch.countDown();
                }
            }
        }

        #endregion

        #region Distributed Transaction Recovery Bits

	    private volatile CountDownLatch recoveryComplete = null;

        /// <summary>
        /// Should be called from NetTxSession when created to check if any TX
        /// data is stored for recovery and whether the Broker has matching info
        /// stored.  If an Transaction is found that belongs to this client and is
        /// still alive on the Broker it will be recovered, otherwise the stored 
        /// data should be cleared.
        /// </summary>
        public void InitializeDtcTxContext()
        {
            // initialize the logger with the current Resource Manager Id
            RecoveryLogger.Initialize(ResourceManagerId);

            KeyValuePair<XATransactionId, byte[]>[] localRecoverables = RecoveryLogger.GetRecoverables();
            if (localRecoverables.Length == 0)
            {
                Tracer.Debug("Did not detect any open DTC transaction records on disk.");
                // No local data so anything stored on the broker can't be recovered here.
                return;
            }

            XATransactionId[] recoverables = TryRecoverBrokerTXIds();
            if (recoverables.Length == 0)
            {
                Tracer.Debug("Did not detect any recoverable transactions at Broker.");
                // Broker has no recoverable data so nothing to do here, delete the 
                // old recovery log as its stale.
                RecoveryLogger.Purge();
                return;
            }

            List<KeyValuePair<XATransactionId, byte[]>> matches = new List<KeyValuePair<XATransactionId, byte[]>>();

            foreach(XATransactionId recoverable in recoverables)
            {
                foreach(KeyValuePair<XATransactionId, byte[]> entry in localRecoverables)
                {
                    if(entry.Key.Equals(recoverable))
                    {
                        Tracer.DebugFormat("Found a matching TX on Broker to stored Id: {0} reenlisting.", entry.Key);
                        matches.Add(entry);
                    }
                }
            }

            if (matches.Count != 0)
            {
                this.recoveryComplete = new CountDownLatch(matches.Count);

                foreach (KeyValuePair<XATransactionId, byte[]> recoverable in matches)
                {
                    this.transactionId = recoverable.Key;
                    this.currentEnlistment = 
                        TransactionManager.Reenlist(ResourceManagerGuid, recoverable.Value, this);
                }

                this.recoveryComplete.await();
                TransactionManager.RecoveryComplete(ResourceManagerGuid);
                return;
            }

            // The old recovery information doesn't match what's on the broker so we
            // should discard it as its stale now.
            RecoveryLogger.Purge();
        }

        private XATransactionId[] TryRecoverBrokerTXIds()
        {
            Tracer.Debug("Checking for Recoverable Transactions on Broker.");

            TransactionInfo info = new TransactionInfo();
            info.ConnectionId = this.session.Connection.ConnectionId;
            info.Type = (int)TransactionType.Recover;

            this.connection.CheckConnected();
            DataArrayResponse response = this.connection.SyncRequest(info) as DataArrayResponse;

            if (response != null && response.Data.Length > 0)
            {
                Tracer.DebugFormat("Broker reports there are {0} recoverable XA Transactions", response.Data.Length);

                List<XATransactionId> recovered = new List<XATransactionId>();

                foreach (DataStructure ds in response.Data)
                {
                    XATransactionId xid = ds as XATransactionId;
                    if (xid != null)
                    {
                        recovered.Add(xid);
                    }
                }

                return recovered.ToArray();
            }

            return new XATransactionId[0];
        }

        #endregion

        internal IRecoveryLogger RecoveryLogger
        {
            get { return (this.connection as NetTxConnection).RecoveryPolicy.RecoveryLogger; }
        }

        internal string ResourceManagerId
        {
            get { return (this.connection as NetTxConnection).ResourceManagerGuid.ToString(); }
        }

        internal Guid ResourceManagerGuid
        {
            get { return (this.connection as NetTxConnection).ResourceManagerGuid; }
        }

    }
}
