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
using Apache.NMS;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Util;

namespace Apache.NMS.ActiveMQ
{
    public class NetTxConnection : Connection, INetTxConnection
    {
        public NetTxConnection(Uri connectionUri, ITransport transport, IdGenerator clientIdGenerator)
            : base(connectionUri, transport, clientIdGenerator)
        {
        }

        public INetTxSession CreateNetTxSession()
        {
            return (INetTxSession) CreateSession(AcknowledgementMode.Transactional);
        }

        protected override Session CreateAtiveMQSession(AcknowledgementMode ackMode)
        {
            CheckConnected();
            return new NetTxSession(this, NextSessionId);
        }

    }
}

