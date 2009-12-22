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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.Transport
{
    /// <summary>
    /// This class make sure that the connection is still alive,
    /// by monitoring the reception of commands from the peer of
    /// the transport.
    /// </summary>
    public class InactivityMonitor : TransportFilter
    {
        private Atomic<bool> monitorStarted = new Atomic<bool>(false);

        private Atomic<bool> commandSent = new Atomic<bool>(false);
        private Atomic<bool> commandReceived = new Atomic<bool>(false);

        private Atomic<bool> failed = new Atomic<bool>(false);
        private Atomic<bool> inRead = new Atomic<bool>(false);
        private Atomic<bool> inWrite = new Atomic<bool>(false);

        private Mutex monitor = new Mutex();

        private Timer readCheckTimer;
        private Timer writeCheckTimer;

        private WriteChecker writeChecker;
        private ReadChecker readChecker;

        private long readCheckTime;
        public long ReadCheckTime
        {
            get { return this.readCheckTime; }
            set { this.readCheckTime = value; }
        }

        private long writeCheckTime;
        public long WriteCheckTime
        {
            get { return this.writeCheckTime; }
            set { this.writeCheckTime = value; }
        }

        private long initialDelayTime;
        public long InitialDelayTime
        {
            get { return this.initialDelayTime; }
            set { this.initialDelayTime = value; }
        }

        private Atomic<bool> keepAliveResponseRequired = new Atomic<bool>(false);
        public bool KeepAliveResponseRequired
        {
            get { return this.keepAliveResponseRequired.Value; }
            set { keepAliveResponseRequired.Value = value; }
        }

        // Local and remote Wire Format Information
        private WireFormatInfo localWireFormatInfo;
        private WireFormatInfo remoteWireFormatInfo;

        /// <summary>
        /// Constructor or the Inactivity Monitor
        /// </summary>
        /// <param name="next"></param>
        public InactivityMonitor(ITransport next)
            : base(next)
        {
            Tracer.Debug("Creating Inactivity Monitor");
        }

        #region WriteCheck Related
        /// <summary>
        /// Check the write to the broker
        /// </summary>
        public void WriteCheck()
        {
            if(inWrite.Value)
            {
                return;
            }

            if(!commandSent.Value)
            {
                Tracer.Debug("No Message sent since last write check. Sending a KeepAliveInfo");
                ThreadPool.QueueUserWorkItem(new WaitCallback(SendKeepAlive));
            }
            else
            {
                Tracer.Debug("Message sent since last write check. Resetting flag");
            }

            commandSent.Value = false;
        }

        private void SendKeepAlive(object state)
        {
            if(monitorStarted.Value)
            {
                try
                {
                    KeepAliveInfo info = new KeepAliveInfo();
                    info.ResponseRequired = keepAliveResponseRequired.Value;
                    Oneway(info);
                }
                catch(IOException exception)
                {
                    OnException(this, exception);
                }
            }
        }
        #endregion

        #region ReadCheck Related
        public void ReadCheck()
        {
            if(inRead.Value)
            {
                Tracer.Debug("A receive is in progress");
                return;
            }

            if(!commandReceived.Value)
            {
                Tracer.Debug("No message received since last read check! Sending an InactivityException!");
                ThreadPool.QueueUserWorkItem(new WaitCallback(SendInactivityException));
            }
            else
            {
                commandReceived.Value = false;
            }
        }

        private void SendInactivityException(object state)
        {
            OnException(this, new IOException("Channel was inactive for too long."));
        }

        /// <summary>
        /// Checks if we should allow the read check(if less than 90% of the read
        /// check time elapsed then we dont do the readcheck
        /// </summary>
        /// <param name="elapsed"></param>
        /// <returns></returns>
        public bool AllowReadCheck(long elapsed)
        {
            return (elapsed > (readCheckTime * 9 / 10));
        }
        #endregion

        public override void Stop()
        {
            StopMonitorThreads();
            next.Stop();
        }

        protected override void OnCommand(ITransport sender, Command command)
        {
            commandReceived.Value = true;
            inRead.Value = true;
            try
            {
                if(command is KeepAliveInfo)
                {
                    KeepAliveInfo info = command as KeepAliveInfo;
                    if(info.ResponseRequired)
                    {
                        try
                        {
                            info.ResponseRequired = false;
                            Oneway(info);
                        }
                        catch(IOException ex)
                        {
                            OnException(this, ex);
                        }
                    }
                }
                else if(command is WireFormatInfo)
                {
                    lock(monitor)
                    {
                        remoteWireFormatInfo = command as WireFormatInfo;
                        try
                        {
                            StartMonitorThreads();
                        }
                        catch(IOException ex)
                        {
                            OnException(this, ex);
                        }
                    }
                }
                base.OnCommand(sender, command);
            }
            finally
            {
                inRead.Value = false;
            }
        }

        public override void Oneway(Command command)
        {
            // Disable inactivity monitoring while processing a command.
            //synchronize this method - its not synchronized
            //further down the transport stack and gets called by more 
            //than one thread  by this class
            lock(inWrite)
            {
                inWrite.Value = true;
                try
                {
                    if(failed.Value)
                    {
                        throw new IOException("Channel was inactive for too long: " + next.RemoteAddress.ToString());
                    }
                    if(command is WireFormatInfo)
                    {
                        lock(monitor)
                        {
                            localWireFormatInfo = command as WireFormatInfo;
                            StartMonitorThreads();
                        }
                    }
                    next.Oneway(command);
                }
                finally
                {
                    commandSent.Value = true;
                    inWrite.Value = false;
                }
            }
        }

        protected override void OnException(ITransport sender, Exception command)
        {
            if(failed.CompareAndSet(false, true))
            {
                Tracer.Debug("Exception received in the Inactivity Monitor: " + command.ToString());
                StopMonitorThreads();
                base.OnException(sender, command);
            }
        }

        private void StartMonitorThreads()
        {
            lock(monitor)
            {
                if(monitorStarted.Value)
                {
                    return;
                }
                if(localWireFormatInfo == null)
                {
                    return;
                }
                if(remoteWireFormatInfo == null)
                {
                    return;
                }

                readCheckTime =
                    Math.Min(
                        localWireFormatInfo.MaxInactivityDuration,
                        remoteWireFormatInfo.MaxInactivityDuration);
                initialDelayTime =
                    Math.Min(
                        localWireFormatInfo.MaxInactivityDurationInitialDelay,
                        remoteWireFormatInfo.MaxInactivityDurationInitialDelay);

                if(readCheckTime > 0)
                {
                    monitorStarted.Value = true;
                    writeChecker = new WriteChecker(this);
                    readChecker = new ReadChecker(this);

                    writeCheckTime = readCheckTime > 3 ? readCheckTime / 3 : readCheckTime;

                    writeCheckTimer = new Timer(
                        new TimerCallback(writeChecker.Check),
                        null,
                        initialDelayTime,
                        writeCheckTime
                        );
                    readCheckTimer = new Timer(
                        new TimerCallback(readChecker.Check),
                        null,
                        initialDelayTime,
                        readCheckTime
                        );
                }
            }
        }

        private void StopMonitorThreads()
        {
            lock(monitor)
            {
                if(monitorStarted.CompareAndSet(true, false))
                {
                    readCheckTimer.Dispose();
                    writeCheckTimer.Dispose();
                }
            }
        }
    }

    class WriteChecker
    {
        private readonly InactivityMonitor parent;

        public WriteChecker(InactivityMonitor parent)
        {
            if(parent == null)
            {
                throw new NullReferenceException("WriteChecker created with a NULL parent.");
            }

            this.parent = parent;
        }

        public void Check(object state)
        {
            this.parent.WriteCheck();
        }
    }

    class ReadChecker
    {
        private readonly InactivityMonitor parent;
        private long lastRunTime;

        public ReadChecker(InactivityMonitor parent)
        {
            if(parent == null)
            {
                throw new NullReferenceException("ReadChecker created with a null parent");
            }
            this.parent = parent;
        }

        public void Check(object state)
        {
            long now = DateUtils.ToJavaTimeUtc(DateTime.UtcNow);
            long elapsed = now - lastRunTime;
            if(!parent.AllowReadCheck(elapsed))
            {
                return;
            }
            lastRunTime = now;

            // Invoke the parent check routine.
            this.parent.ReadCheck();
        }
    }
}
