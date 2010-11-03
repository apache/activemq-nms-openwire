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
using System.Net;
using System.Transactions;
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
	public class TransactionContext : IEnlistmentNotification
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

            // Enlist this object in the transaction.
            this.currentEnlistment =
                transaction.EnlistVolatile(this, EnlistmentOptions.None);

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
                Tracer.Debug("Begin XA'ish Transaction:" + xaId.GlobalTransactionId.ToString());
            }
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            Tracer.Debug("Prepare notification received");

            // Now notify the broker that a new XA'ish transaction has started.
            TransactionInfo info = new TransactionInfo();
            info.ConnectionId = this.session.Connection.ConnectionId;
            info.TransactionId = this.transactionId;

            try
            {
                BeforeEnd();

                // End the current branch
                info.Type = (int) TransactionType.End;
                this.connection.SyncRequest(info);

                // Prepare the Transaction for commit.
                info.Type = (int) TransactionType.Prepare;
                IntegerResponse response = (IntegerResponse) this.connection.SyncRequest(info);
                if(response.Result == XA_READONLY)
                {
                    Tracer.Debug("Transaction Prepare Reports Done: ");

                    // if server responds that nothing needs to be done, then reply prepared
                    // but clear the current state data so we appear done to the commit method.
                    preparingEnlistment.Prepared();

                    this.transactionId = null;
                    this.currentEnlistment = null;

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
            }
        }

        public void Commit(Enlistment enlistment)
        {
            Tracer.Debug("Commit notification received");

            try
            {
                if(this.transactionId != null)
                {
                    // Now notify the broker that a new XA'ish transaction has started.
                    TransactionInfo info = new TransactionInfo();
                    info.ConnectionId = this.session.Connection.ConnectionId;
                    info.TransactionId = this.transactionId;
                    info.Type = (int) TransactionType.CommitOnePhase;

                    this.connection.CheckConnected();
                    this.connection.SyncRequest(info);

                    Tracer.Debug("Transaction Commit Reports Done: ");

                    // if server responds that nothing needs to be done, then reply done.
                    enlistment.Done();

                    AfterCommit();
                }
            }
            catch(Exception ex)
            {
                Tracer.Debug("Transaction Commit failed with error: " + ex.Message);
                AfterRollback();
                enlistment.Done();
                throw;
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;
            }
        }

        public void Rollback(Enlistment enlistment)
        {
            Tracer.Debug("Rollback notification received");

            // Now notify the broker that a new XA'ish transaction has started.
            TransactionInfo info = new TransactionInfo();
            info.ConnectionId = this.session.Connection.ConnectionId;
            info.TransactionId = this.transactionId;

            try
            {
                BeforeEnd();

                info.Type = (int) TransactionType.End;
                this.connection.SyncRequest(info);

                info.Type = (int) TransactionType.Rollback;
                this.connection.CheckConnected();
                this.connection.SyncRequest(info);

                Tracer.Debug("Transaction Rollback Reports Done: ");

                // if server responds that nothing needs to be done, then reply done.
                enlistment.Done();

                AfterRollback();
            }
            catch(Exception ex)
            {
                Tracer.Debug("Transaction Rollback failed with error: " + ex.Message);
                AfterRollback();
                enlistment.Done();
                throw;
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;
            }
        }

        public void InDoubt(Enlistment enlistment)
        {
            Tracer.Debug("In doubt notification received");

            try
            {
                // Now notify the broker that it should forget this TX.
                TransactionInfo info = new TransactionInfo();
                info.ConnectionId = this.session.Connection.ConnectionId;
                info.TransactionId = this.transactionId;
                info.Type = (int) TransactionType.Forget;
    
                //Declare done on the enlistment
                enlistment.Done();
            }
            finally
            {
                this.currentEnlistment = null;
                this.transactionId = null;
            }
        }

        #endregion

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
