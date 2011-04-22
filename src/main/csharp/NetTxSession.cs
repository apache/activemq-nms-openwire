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
using System.Transactions;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ
{
    public class NetTxSession : Session, INetTxSession
    {
        public NetTxSession(Connection connection, SessionId id)
            : base(connection, id, AcknowledgementMode.AutoAcknowledge)
        {
            TransactionContext.InitializeDtcTxContext();
        }

        /// <summary>
        /// Manually Enlists in the given Transaction.  This can be used to when the
        /// client is using the Session in Asynchronous listener mode since the Session
        /// cannot atuomatically join in this case as there is no Ambient transaction in
        /// the Message Dispatch thread.  This also allows for clients to use the explicit
        /// exception model when necessary.  
        /// </summary>
        public void Enlist(Transaction tx)
        {
            if(tx == null)
            {
                throw new NullReferenceException("Specified Transaction cannot be null");
            }

            this.EnrollInSpecifiedTransaction(tx);
        }

        /// <summary>
        /// Reports Transacted whenever there is an Ambient Transaction or the internal
        /// TransactionContext is still involed in a .NET Transaction beyond the lifetime
        /// of an ambient transaction (can happen during a scoped transaction disposing
        /// without Complete being called and a Rollback is in progress.)
        /// </summary>
        public override bool IsTransacted
        {
            get { return Transaction.Current != null || TransactionContext.InNetTransaction; }
        }

        public override bool IsAutoAcknowledge
        {
            // When not in a .NET Transaction we assume Auto Ack.
            get { return true; }
        }

        internal override void DoRollback()
        {
            // Only the Transaction Manager can do this when in a .NET Transaction.
            throw new TransactionInProgressException("Cannot Rollback() inside an NetTxSession");
        }

        internal override void DoCommit()
        {
            // Only the Transaction Manager can do this when in a .NET Transaction.
            throw new TransactionInProgressException("Cannot Commit() inside an NetTxSession");
        }

        internal override void DoStartTransaction()
        {
            TransactionContext.SyncRoot.WaitOne();

            if (TransactionContext.InNetTransaction && TransactionContext.NetTxState == TransactionContext.TxState.Pending)
            {
                // To late to participate in this TX, we have to wait for it to complete then
                // we can create a new TX and start from there.
                TransactionContext.SyncRoot.ReleaseMutex();
                TransactionContext.DtcWaitHandle.WaitOne();
                TransactionContext.SyncRoot.WaitOne();
            }

            if (!TransactionContext.InNetTransaction && Transaction.Current != null)
            {
                Tracer.Debug("NetTxSession detected Ambient Transaction, start new TX with broker");

                EnrollInSpecifiedTransaction(Transaction.Current);
            }
        }

        private void EnrollInSpecifiedTransaction(Transaction tx)
        {
            // If an Async DTC operation is in progress such as Commit or Rollback
            // we need to let it complete before deciding if the Session is in a TX
            // otherwise we might error out for no reason.
            //TransactionContext.DtcWaitHandle.WaitOne();

            if(TransactionContext.InNetTransaction)
            {
                Tracer.Warn("Enlist attempted while a Net TX was Active.");
                throw new InvalidOperationException("Session is Already enlisted in a Transaction");
            }

            if(Transaction.Current != null && !Transaction.Current.Equals(tx))
            {
                Tracer.Warn("Enlist attempted with a TX that doesn't match the Ambient TX.");
                throw new ArgumentException("Specified TX must match the ambient TX if set.");
            }
            
            // Start a new .NET style transaction, this could be distributed
            // or it could just be a Local transaction that could become
            // distributed later.
            TransactionContext.Begin(tx);
        }
    }
}

