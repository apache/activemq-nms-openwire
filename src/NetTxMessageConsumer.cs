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
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Util.Synchronization;

namespace Apache.NMS.ActiveMQ
{
    public sealed class NetTxMessageConsumer : MessageConsumer
    {
        private readonly NetTxSession session;
        private readonly NetTxTransactionContext transactionContext;

        internal NetTxMessageConsumer(Session session, ConsumerId id, ActiveMQDestination destination, 
                                      string name, string selector, int prefetch, int maxPendingMessageCount,
                                      bool noLocal, bool browser, bool dispatchAsync) : 
            base(session, id, destination, name, selector, prefetch,
                 maxPendingMessageCount, noLocal, browser, dispatchAsync)
        {
            this.session = session as NetTxSession;
            this.transactionContext = session.TransactionContext as NetTxTransactionContext;
        }

        
        public override async Task CloseAsync()
        {
            if (this.Closed)
            {
                return;
            }

            using(await this.transactionContext.SyncRoot.LockAsync().Await())
            {
                if (this.session.IsTransacted || this.session.TransactionContext.InTransaction)
                {
                    Tracer.DebugFormat("Consumer {0} Registering new ConsumerCloseSynchronization",
                                       this.ConsumerId);
                    this.session.TransactionContext.AddSynchronization(
                        new ConsumerCloseSynchronization(this));
                }
                else
                {
                    Tracer.DebugFormat("Consumer {0} No Active TX closing normally.",
                                       this.ConsumerId);
                    await this.DoCloseAsync().Await();                            
                }
            }
        }

        public override async Task BeforeMessageIsConsumedAsync(MessageDispatch dispatch)
        {
            if (!IsAutoAcknowledgeBatch)
            {
                if (this.session.IsTransacted)
                {
                    bool waitForDtcWaitHandle = false;
                    using (await this.transactionContext.SyncRoot.LockAsync().Await())
                    {
                        // In the case where the consumer is operating in concert with a
                        // distributed TX manager we need to wait whenever the TX is being
                        // controlled by the DTC as it completes all operations async and
                        // we cannot start consumption again until all its tasks have completed.)
                        var currentTransactionId = transactionContext.TransactionId as XATransactionId;
                        string currentLocalTxId = currentTransactionId != null
                            ? UTF8Encoding.UTF8.GetString(currentTransactionId.GlobalTransactionId)
                            : "NONE";

                        if (Transaction.Current != null)
                        {
                            waitForDtcWaitHandle = this.transactionContext.InNetTransaction &&
                                               this.transactionContext.NetTxState == NetTxTransactionContext.TxState.Pending ||
                                               currentLocalTxId != Transaction.Current.TransactionInformation.LocalIdentifier;
                        }
                        else
                        {
                            waitForDtcWaitHandle = this.transactionContext.InNetTransaction &&
                                               this.transactionContext.NetTxState == NetTxTransactionContext.TxState.Pending;
                        }
                        
                    }

                    //if session EnlistMsDtcNativeResource the transaction does not need to wait
                    if (this.session.EnlistsMsDtcNativeResource)
                    {
                        waitForDtcWaitHandle = false;
                    }

                    if (waitForDtcWaitHandle)
                    {
                        this.transactionContext.DtcWaitHandle.WaitOne();
                    }
                }
            }

            await base.BeforeMessageIsConsumedAsync(dispatch).Await();
        }

    }
}
