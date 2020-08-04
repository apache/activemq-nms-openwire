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

using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Util;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.Transactions
{
    public class RecoveryLoggerHarness : IRecoveryLogger
    {
        public delegate void HarnessedEventHandler();
        public delegate void HarnessedLogRecoveryInfoHandler(XATransactionId xid, byte[] recoveryInfo);
        public delegate void HarnessedLogRecoveredHandler(XATransactionId xid);
        public delegate void HarnessedGetRecoverablesHandler(KeyValuePair<XATransactionId, byte[]>[] recoverables);
        
        private static readonly FactoryFinder<RecoveryLoggerFactoryAttribute, IRecoveryLoggerFactory> FACTORY_FINDER =
            new FactoryFinder<RecoveryLoggerFactoryAttribute, IRecoveryLoggerFactory>();

        private IRecoveryLogger containedLogger;

        public event HarnessedEventHandler PreInitializeEvent;
        public event HarnessedEventHandler PostInitializeEvent;

        public event HarnessedLogRecoveryInfoHandler PreLogRecoveryInfoEvent;
        public event HarnessedLogRecoveryInfoHandler PostLogRecoveryInfoEvent;

        public event HarnessedLogRecoveredHandler PreLogRecoverdEvent;
        public event HarnessedLogRecoveredHandler PostLogRecoverdEvent;

        public event HarnessedGetRecoverablesHandler PreGetRecoverablesEvent;
        public event HarnessedGetRecoverablesHandler PostGetRecoverablesEvent;

        public event HarnessedEventHandler PrePurgeEvent;
        public event HarnessedEventHandler PostPurgeEvent;

        public RecoveryLoggerHarness()
        {
            this.containedLogger = new RecoveryFileLogger();
        }

        public string ResourceManagerId
        {
            get { return this.containedLogger.ResourceManagerId; }
        }

        public void Initialize(string resourceManagerId)
        {
            if(this.PreInitializeEvent != null)
            {
                this.PreInitializeEvent();
            }
            
            this.containedLogger.Initialize(resourceManagerId);

            if (this.PostInitializeEvent != null)
            {
                this.PostInitializeEvent();
            }
        }

        public void LogRecoveryInfo(XATransactionId xid, byte[] recoveryInformation)
        {
            if (recoveryInformation == null || recoveryInformation.Length == 0)
            {
                return;
            }

            if (this.PreLogRecoveryInfoEvent != null)
            {
                this.PreLogRecoveryInfoEvent(xid, recoveryInformation);
            }

            this.containedLogger.LogRecoveryInfo(xid, recoveryInformation);

            if (this.PostLogRecoveryInfoEvent != null)
            {
                this.PostLogRecoveryInfoEvent(xid, recoveryInformation);
            }
        }

        public KeyValuePair<XATransactionId, byte[]>[] GetRecoverables()
        {
            KeyValuePair<XATransactionId, byte[]>[] result = new KeyValuePair<XATransactionId, byte[]>[0];

            if (this.PreGetRecoverablesEvent != null)
            {
                this.PreGetRecoverablesEvent(result);
            }

            result = this.containedLogger.GetRecoverables();

            if (this.PostGetRecoverablesEvent != null)
            {
                this.PostGetRecoverablesEvent(result);
            }

            return result;
        }

        public void LogRecovered(XATransactionId xid)
        {
            if (this.PreLogRecoverdEvent != null)
            {
                this.PreLogRecoverdEvent(xid);
            }

            this.containedLogger.LogRecovered(xid);

            if (this.PostLogRecoverdEvent != null)
            {
                this.PostLogRecoverdEvent(xid);
            }
        }

        public void Purge()
        {
            if (this.PrePurgeEvent != null)
            {
                this.PrePurgeEvent();
            }

            this.containedLogger.Purge();

            if (this.PostPurgeEvent != null)
            {
                this.PostPurgeEvent();
            }
        }

        public string LoggerType
        {
            get { return this.containedLogger.LoggerType; }
        }

        #region Harness Methods

        public IRecoveryLogger Harnessed
        {
            get { return this.containedLogger; }    
        }

        public string HarnessedType
        {
            get { return this.containedLogger != null ? this.containedLogger.LoggerType : ""; }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new NMSException(String.Format("Recovery Logger name invalid: [{0}]", value));
                }

                IRecoveryLoggerFactory factory = null;

                try
                {
                    factory = NewInstance(value.ToLower());
                }
                catch (NMSException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw NMSExceptionSupport.Create("Error creating Recovery Logger", e);
                }

                this.containedLogger = factory.Create();
            }
        }

        private static IRecoveryLoggerFactory NewInstance(string scheme)
        {
            try
            {
                Type factoryType = FindLoggerFactory(scheme);

                if (factoryType == null)
                {
                    throw new Exception("NewInstance failed to find a match for id = " + scheme);
                }

                return (IRecoveryLoggerFactory)Activator.CreateInstance(factoryType);
            }
            catch (Exception ex)
            {
                Tracer.WarnFormat("NewInstance failed to create an IRecoveryLoggerFactory with error: {1}", ex.Message);
                throw;
            }
        }

        private static Type FindLoggerFactory(string scheme)
        {
            try
            {
                return FACTORY_FINDER.FindFactoryType(scheme);
            }
            catch
            {
                throw new NMSException("Failed to find Factory for Recovery Logger type: " + scheme);
            }
        }

        public Object Clone()
        {
            return this.MemberwiseClone();
        }       

        #endregion
    }
}

