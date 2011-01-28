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
using System.IO;
using System.Reflection;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.Transactions
{
    public class RecoveryFileLogger : IRecoveryLogger
    {
        private string location;
        private string resourceManagerId;
        private object syncRoot = new object();

        public RecoveryFileLogger()
        {
            // Set the path by default to the location of the executing assembly.
            // May need to change this to current working directory, not sure.
            this.location = Assembly.GetExecutingAssembly().Location;
        }

        /// <summary>
        /// The Unique Id of the Resource Manager that this logger is currently
        /// logging recovery information for.
        /// </summary>
        public string ResourceManagerId
        {
            get { return this.resourceManagerId; }
            set { this.resourceManagerId = value; }
        }

        /// <summary>
        /// The Path to the location on disk where the recovery log is written
        /// to and read from.
        /// </summary>
        public string Location
        {
            get { return this.location; }
            set { this.location = value; }
        }

        public void LogRecoveryInfo(XATransactionId xid, byte[] recoveryInformation)
        {
            if (recoveryInformation == null || recoveryInformation.Length == 0)
            {
                return;
            }

            try
            {
                lock (syncRoot)
                {
                    string filename = Location + ResourceManagerId + ".bin";
                    RecoveryInformation info = new RecoveryInformation(xid, recoveryInformation);
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

        public KeyValuePair<XATransactionId, byte[]>[] GetRecoverables()
        {
            KeyValuePair<XATransactionId, byte[]>[] result = new KeyValuePair<XATransactionId, byte[]>[0];
            RecoveryInformation info = TryOpenRecoveryInfoFile();

            if(result != null)
            {
                result = new KeyValuePair<XATransactionId, byte[]>[1];
                result[0] = new KeyValuePair<XATransactionId, byte[]>(info.Xid, info.TxRecoveryInfo);
            }

            return result;
        }

        public void LogRecovered(XATransactionId xid)
        {
            lock (syncRoot)
            {
                string filename = Location + ResourceManagerId + ".bin";

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

        public void Purge()
        {
            lock (syncRoot)
            {
                string filename = Location + ResourceManagerId + ".bin";

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

        public string LoggerType
        {
            get { return "file"; }
        }

        #region Recovery File Opeations

        [Serializable]
        private sealed class RecoveryInformation
        {
            private byte[] txRecoveryInfo;
            private byte[] globalTxId;
            private byte[] branchId;
            private int formatId;

            public RecoveryInformation(XATransactionId xaId, byte[] recoveryInfo)
            {
                this.Xid = xaId;
                this.txRecoveryInfo = recoveryInfo;
            }

            public byte[] TxRecoveryInfo
            {
                get { return this.txRecoveryInfo; }
                set { this.txRecoveryInfo = value; }
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
            string filename = Location + ResourceManagerId + ".bin";
            RecoveryInformation result = null;

            Tracer.Debug("Checking for Recoverable Transactions filename: " + filename);

            lock (syncRoot)
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

        #endregion
    }
}

