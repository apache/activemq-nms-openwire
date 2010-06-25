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
using System.Collections;
using System.Threading;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.Transport
{
    /// <summary>
    /// A Transport that correlates asynchronous send/receive messages into single request/response.
    /// </summary>
    public class ResponseCorrelator : TransportFilter
    {
        private readonly IDictionary requestMap = Hashtable.Synchronized(new Hashtable());
        private int nextCommandId;

        public ResponseCorrelator(ITransport next) : base(next)
        {
        }

        protected override void OnException(ITransport sender, Exception command)
        {
            base.OnException(sender, command);

            foreach(DictionaryEntry entry in requestMap)
            {
                FutureResponse value = (FutureResponse) entry.Value;
                ExceptionResponse response = new ExceptionResponse();
                BrokerError error = new BrokerError();

                error.Message = command.Message;
                response.Exception = error;
                value.Response = response;
            }

            requestMap.Clear();
        }

        internal int GetNextCommandId()
        {
            return Interlocked.Increment(ref nextCommandId);
        }

        public override void Oneway(Command command)
        {
            if(0 == command.CommandId)
            {
                command.CommandId = GetNextCommandId();
            }

            next.Oneway(command);
        }

        public override FutureResponse AsyncRequest(Command command)
        {
            int commandId = GetNextCommandId();

            command.CommandId = commandId;
            command.ResponseRequired = true;
            FutureResponse future = new FutureResponse();
            requestMap[commandId] = future;
            next.Oneway(command);
            return future;
        }

        public override Response Request(Command command, TimeSpan timeout)
        {
            FutureResponse future = AsyncRequest(command);
            future.ResponseTimeout = timeout;
            Response response = future.Response;

            if(response != null && response is ExceptionResponse)
            {
                ExceptionResponse er = (ExceptionResponse) response;
                BrokerError brokerError = er.Exception;

                if (brokerError == null)
                {
                    throw new BrokerException();
                }
                else
                {
                    throw new BrokerException(brokerError);
                }
            }

            return response;
        }

        protected override void OnCommand(ITransport sender, Command command)
        {
            if(command is Response)
            {
                Response response = (Response) command;
                int correlationId = response.CorrelationId;
                FutureResponse future = (FutureResponse) requestMap[correlationId];

                if(future != null)
                {
                    requestMap.Remove(correlationId);
                    future.Response = response;

                    if(response is ExceptionResponse)
                    {
                        ExceptionResponse er = (ExceptionResponse) response;
                        BrokerError brokerError = er.Exception;
                        BrokerException exception = new BrokerException(brokerError);
                        this.exceptionHandler(this, exception);
                    }
                }
                else
                {
                    if(Tracer.IsDebugEnabled)
                    {
                        Tracer.Debug("Unknown response ID: " + response.CommandId + " for response: " + response);
                    }
                }
            }
            else if(command is ShutdownInfo)
            {
                // lets shutdown
                this.commandHandler(sender, command);
            }
            else
            {
                this.commandHandler(sender, command);
            }
        }
    }
}


