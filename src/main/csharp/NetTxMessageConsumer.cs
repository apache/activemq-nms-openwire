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
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ
{
    class NetTxMessageConsumer : MessageConsumer
    {
        private readonly NetTxSession session;

        internal NetTxMessageConsumer(Session session, ConsumerId id, ActiveMQDestination destination, 
                                      string name, string selector, int prefetch, int maxPendingMessageCount,
                                      bool noLocal, bool browser, bool dispatchAsync) : 
            base(session, id, destination, name, selector, prefetch,
                 maxPendingMessageCount, noLocal, browser, dispatchAsync)
        {
            this.session = session as NetTxSession;
        }

        public override void Close()
        {
            if (this.Closed)
            {
                return;
            }

            lock (this.session.TransactionContext.SyncRoot)
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
                    this.DoClose();                            
                }
            }
        }
    }
}
