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
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Text;
using System.Net;
using System.Transactions;
using System.Collections;
using System.Collections.Generic;
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
            get{ return this.currentEnlistment != null; }
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

            Tracer.Debug("Enlisted in Durable Transaction with RM Id: " + rmId.ToString());

            System.Transactions.TransactionInformation txInfo = transaction.TransactionInformation;

            XATransactionId xaId = new XATransactionId();
            this.transactionId = xaId;

            if(txInfo.DistributedIdentifier != Guid.Empty)
            {
                xaId.GlobalTransactionId = txInfo.DistributedIdentifier.ToByteArray();
                xaId.BranchQualifier = Encoding.UTF8.GetBytes(txInfo.LocalIdentifier);
            }
            else
            {
                xaId.GlobalTransactionId = Encoding.UTF8.GetBytes(txInfo.LocalIdentifier);
                xaId.BranchQualifier = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());
            }

            // Now notify the broker that a new XA'ish transaction has started.
            TransactionInfo info = new TransactionInfo();
            info.ConnectionId = this.session.Connection.ConnectionId;
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

	            // Now notify the broker that a new XA'ish transaction has started.
	            TransactionInfo info = new TransactionInfo();
	            info.ConnectionId = this.session.Connection.ConnectionId;
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

                    StoreRecoveryInformation(preparingEnlistment.RecoveryInformation());
                }
            }
            catch(Exception ex)
            {
                Tracer.Debug("Transaction Prepare failed with error: " + ex.Message);
                AfterRollback();
                preparingEnlistment.ForceRollback();
                ClearStoredRecoveryInformation();
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
                    info.ConnectionId = this.session.Connection.ConnectionId;
                    info.TransactionId = this.transactionId;
                    info.Type = (int) TransactionType.CommitTwoPhase;

                    this.connection.CheckConnected();
                    this.connection.SyncRequest(info);

                    Tracer.Debug("Transaction Commit Reports Done: ");

                    ClearStoredRecoveryInformation();

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
                    this.session.Connection.OnException(ex);
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
                    info.ConnectionId = this.session.Connection.ConnectionId;
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
                this.session.Connection.OnException(ex);
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

                BeforeEnd();

                // Now notify the broker that a new XA'ish transaction has started.
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = this.session.Connection.ConnectionId;
                info.TransactionId = this.transactionId;
                info.Type = (int)TransactionType.End;

                this.connection.CheckConnected();
                this.connection.SyncRequest(info);

                info.Type = (int) TransactionType.Rollback;
                this.connection.CheckConnected();
                this.connection.SyncRequest(info);

                Tracer.Debug("Transaction Rollback Reports Done: ");

                ClearStoredRecoveryInformation();

                // if server responds that nothing needs to be done, then reply done.
                enlistment.Done();

                AfterRollback();
            }
            catch(Exception ex)
            {
                Tracer.Debug("Transaction Rollback failed with error: " + ex.Message);
                AfterRollback();
                this.session.Connection.OnException(ex);
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;
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
                info.ConnectionId = this.session.Connection.ConnectionId;
                info.TransactionId = this.transactionId;
                info.Type = (int)TransactionType.End;

                this.connection.CheckConnected();
                this.connection.SyncRequest(info);

                info.Type = (int)TransactionType.Rollback;
                this.connection.CheckConnected();
                this.connection.SyncRequest(info);

                Tracer.Debug("InDoubt Transaction Rollback Reports Done: ");

                ClearStoredRecoveryInformation();

                // if server responds that nothing needs to be done, then reply done.
                enlistment.Done();

                AfterRollback();
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;
            }
        }

        #endregion

        #region Distributed Transaction Recovery Bits

        private object logFileLock = new object();

        /// <summary>
        /// Should be called from NetTxSession when created to check if any TX
        /// data is stored for recovery and whether the Broker has matching info
        /// stored.  If an Transaction is found that belongs to this client and is
        /// still alive on the Broker it will be recovered, otherwise the stored 
        /// data should be cleared.
        /// </summary>
        public void CheckForAndRecoverFailedTransactions()
        {
            RecoveryInformation info = TryOpenRecoveryInfoFile();
            if (info == null)
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
                ClearStoredRecoveryInformation();
                return;
            }

            XATransactionId xid = info.Xid;

            foreach(XATransactionId recoverable in recoverables)
            {
                if(xid.Equals(recoverable))
                {
                    Tracer.DebugFormat("Found a matching TX on Broker to stored Id: {0} reenlisting.", xid);

                    // Reenlist the recovered transaction with the TX Manager.
                    this.transactionId = xid;
                    this.currentEnlistment = TransactionManager.Reenlist(ResourceManagerGuid, info.TxRecoveryInfo, this);
                    TransactionManager.RecoveryComplete(ResourceManagerGuid);
                    return;
                }
            }

            // The old recovery information doesn't match what's on the broker so we
            // should discard it as its stale now.
            ClearStoredRecoveryInformation();
        }

        [Serializable]
        private sealed class RecoveryInformation
        {
            private byte[] txRecoveryInfo;
            private byte[] globalTxId;
            private byte[] branchId;
            private int formatId;

            public RecoveryInformation()
            {
            }

            public RecoveryInformation(XATransactionId xaId, byte[] recoveryInfo)
            {
                this.Xid = xaId;
                this.txRecoveryInfo = recoveryInfo;
            }

            public byte[] TxRecoveryInfo
            {
                get { return this.txRecoveryInfo; }
            }

            public XATransactionId Xid
            {
                get
                {
                    XATransactionId xid = new XATransactionId();
                    xid.BranchQualifier = this.branchId;
                    xid.GlobalTransactionId = this.globalTxId;
                    xid.FormatId = this.formatId;

                    return xid;
                }

                set
                {
                    this.branchId = value.BranchQualifier;
                    this.globalTxId = value.GlobalTransactionId;
                    this.formatId = value.FormatId;
                }
            }
        }

        private RecoveryInformation TryOpenRecoveryInfoFile()
        {
            string filename = ResourceManagerId + ".bin";
            RecoveryInformation result = null;

            Tracer.Debug("Checking for Recoverable Transactions filename: " + filename);

            lock (logFileLock)
            {
                try
                {
                    if (!File.Exists(filename))
                    {
                        return null;
                    }

                    using(FileStream recoveryLog = new FileStream(filename, FileMode.Open, FileAccess.Read))
                    {
                        Tracer.Debug("Found Recovery Log File: " + filename);
                        IFormatter formatter = new BinaryFormatter();
                        result = formatter.Deserialize(recoveryLog) as RecoveryInformation;
                    }
                }
                catch(Exception ex)
                {
                    Tracer.InfoFormat("Error while opening Recovery file {0} error message: {1}", filename, ex.Message);
                    // Nothing to restore.
                    return null;
                }
            }

            return result;
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

        private void StoreRecoveryInformation(byte[] recoveryInfo)
        {
            if (recoveryInfo == null || recoveryInfo.Length == 0)
            {
                return;
            }

            try
            {
                lock (logFileLock)
                {
                    string filename = ResourceManagerId + ".bin";
                    XATransactionId xid = this.transactionId as XATransactionId;

                    RecoveryInformation info = new RecoveryInformation(xid, recoveryInfo);

                    Tracer.Debug("Serializing Recovery Info to file: " + filename);

                    IFormatter formatter = new BinaryFormatter();
                    using (FileStream recoveryLog = new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Write))
                    {
                        formatter.Serialize(recoveryLog, info);
                    }
                }
            }
            catch (Exception ex)
            {
                Tracer.Error("Error while storing TX Recovery Info, message: " + ex.Message);
                throw;
            }
        }

        private void ClearStoredRecoveryInformation()
        {
            lock (logFileLock)
            {
                string filename = ResourceManagerId + ".bin";

                try
                {
                    Tracer.Debug("Attempting to remove stale Recovery Info file: " + filename);
                    File.Delete(filename);
                }
                catch(Exception ex)
                {
                    Tracer.Debug("Caught Exception while removing stale RecoveryInfo file: " + ex.Message);
                    return;
                }
            }
        }

        #endregion

        public string ResourceManagerId
        {
            get { return GuidFromId(this.connection.ResourceManagerId).ToString(); }
        }

        internal Guid ResourceManagerGuid
        {
            get { return GuidFromId(this.connection.ResourceManagerId); }
        }

        private Guid GuidFromId(string id)
        {
            // Remove the ID: prefix, that's non-unique to be sure
            string resId = id.TrimStart("ID:".ToCharArray());

            // Remaing parts should be host-port-timestamp-instance:sequence
            string[] parts = resId.Split(":-".ToCharArray());

            // We don't use the hostname here, just the remaining bits.
            int a = Int32.Parse(parts[1]);
            short b = Int16.Parse(parts[3]);
            short c = Int16.Parse(parts[4]);
            byte[] d = System.BitConverter.GetBytes(Int64.Parse(parts[2]));

            return new Guid(a, b, c, d);
        }

        private string IdFromGuid(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();

            int port = System.BitConverter.ToInt32(bytes, 0);
            int instance = System.BitConverter.ToInt16(bytes, 4);
            int sequence = System.BitConverter.ToInt16(bytes, 6);
            long timestamp = System.BitConverter.ToInt64(bytes, 8);

            StringBuilder builder = new StringBuilder("ID:");

            string hostname = "localhost";

            try
            {
                hostname = Dns.GetHostName();
            }
            catch
            {
            }

            builder.Append(hostname);
            builder.Append("-");
            builder.Append(port);
            builder.Append("-");
            builder.Append(timestamp);
            builder.Append("-");
            builder.Append(instance);
            builder.Append(":");
            builder.Append(sequence);

            return builder.ToString();
        }
    }
}
