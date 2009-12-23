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
using Apache.NMS.ActiveMQ.Threads;
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

		private CompositeTaskRunner asyncTasks;
        private AsyncSignalReadErrorkTask asyncErrorTask;
        private AsyncWriteTask asyncWriteTask;
		
        private Mutex monitor = new Mutex();

        private Timer readCheckTimer;
        private Timer writeCheckTimer;

        private DateTime lastReadCheckTime;

        //private WriteChecker writeChecker;
        //private ReadChecker readChecker;

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

        ~InactivityMonitor()
        {
            Dispose(false);
        }

        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                // get rid of unmanaged stuff
            }

            StopMonitorThreads();

            base.Dispose(disposing);
        }

        #region WriteCheck Related
        /// <summary>
        /// Check the write to the broker
        /// </summary>
        public void WriteCheck(object state)
        {
            if(this.inWrite.Value || this.failed.Value)
            {
                return;
            }

            if(!commandSent.Value)
            {
                Tracer.Debug("No Message sent since last write check. Sending a KeepAliveInfo");
                Console.WriteLine("No Message sent since last write check. Sending a KeepAliveInfo");
                this.asyncWriteTask.IsPending = true;
                this.asyncTasks.Wakeup();
            }
            else
            {
                Tracer.Debug("Message sent since last write check. Resetting flag");
                Console.WriteLine("Message sent since last write check. Resetting flag");
            }

            commandSent.Value = false;
        }
        #endregion

        #region ReadCheck Related
        public void ReadCheck(object state)
        {
            DateTime now = DateTime.Now;
            TimeSpan elapsed = now - this.lastReadCheckTime;

            if(!AllowReadCheck(elapsed))
            {
                return;
            }

            this.lastReadCheckTime = now;

            if(this.inRead.Value || this.failed.Value)
            {
                Tracer.Debug("A receive is in progress or already failed.");
                return;
            }

            if(!commandReceived.Value)
            {
                Tracer.Debug("No message received since last read check! Sending an InactivityException!");
                Console.WriteLine("No message received since last read check! Sending an InactivityException!");
                this.asyncErrorTask.IsPending = true;
                this.asyncTasks.Wakeup();
            }
            else
            {
                commandReceived.Value = false;
            }
        }

        /// <summary>
        /// Checks if we should allow the read check(if less than 90% of the read
        /// check time elapsed then we dont do the readcheck
        /// </summary>
        /// <param name="elapsed"></param>
        /// <returns></returns>
        public bool AllowReadCheck(TimeSpan elapsed)
        {
            return (elapsed.TotalMilliseconds > (readCheckTime * 9 / 10));
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
                Console.WriteLine("Exception received in the Inactivity Monitor: " + command.Message);
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

                this.asyncTasks = new CompositeTaskRunner();

                this.asyncErrorTask = new AsyncSignalReadErrorkTask(this, next.RemoteAddress);
                this.asyncWriteTask = new AsyncWriteTask(this);

                this.asyncTasks.AddTask(this.asyncErrorTask);
                this.asyncTasks.AddTask(this.asyncWriteTask);

                if(readCheckTime > 0)
                {
                    monitorStarted.Value = true;

                    writeCheckTime = readCheckTime > 3 ? readCheckTime / 3 : readCheckTime;

                    writeCheckTimer = new Timer(
                        new TimerCallback(WriteCheck),
                        null,
                        initialDelayTime,
                        writeCheckTime
                        );
                    readCheckTimer = new Timer(
                        new TimerCallback(ReadCheck),
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
                    AutoResetEvent shutdownEvent = new AutoResetEvent(false);

                    this.readCheckTimer.Dispose(shutdownEvent);
                    shutdownEvent.WaitOne();
                    this.writeCheckTimer.Dispose(shutdownEvent);
                    shutdownEvent.WaitOne();

					this.asyncTasks.Shutdown();
                    this.asyncTasks = null;
                    this.asyncWriteTask = null;
                    this.asyncErrorTask = null;
                }
            }
        }

        #region Async Tasks
        // Task that fires when the TaskRunner is signaled by the ReadCheck Timer Task.
        class AsyncSignalReadErrorkTask : CompositeTask
        {
            private InactivityMonitor parent;
            private Uri remote;
            private Atomic<bool> pending = new Atomic<bool>(false);
    
            public AsyncSignalReadErrorkTask(InactivityMonitor parent, Uri remote)
            {
                this.parent = parent;
                this.remote = remote;
            }

            public bool IsPending
            {
                get { return this.pending.Value; }
                set { this.pending.Value = value; }
            }
    
            public bool Iterate()
            {
                if(this.pending.CompareAndSet(true, false) && this.parent.monitorStarted.Value)
                {
                    Console.WriteLine("AsyncSignalReadErrorkTask - Sending Pending Read Error");
                    IOException ex = new IOException("Channel was inactive for too long: " + remote);
                    this.parent.OnException(parent, ex);
                }
    
                return this.pending.Value;
            }
        }
    
        // Task that fires when the TaskRunner is signaled by the WriteCheck Timer Task.
        class AsyncWriteTask : CompositeTask
        {
            private InactivityMonitor parent;
            private Atomic<bool> pending = new Atomic<bool>(false);
    
            public AsyncWriteTask(InactivityMonitor parent)
            {
                this.parent = parent;
            }
    
            public bool IsPending
            {
                get { return this.pending.Value; }
                set { this.pending.Value = value; }
            }
    
            public bool Iterate()
            {
                if(this.pending.CompareAndSet(true, false) && this.parent.monitorStarted.Value)
                {
                    try
                    {
                        Console.WriteLine("AsyncWriteTask - Sending Pending KeepAlive");
                        KeepAliveInfo info = new KeepAliveInfo();
                        info.ResponseRequired = this.parent.keepAliveResponseRequired.Value;
                        this.parent.Oneway(info);
                    }
                    catch(IOException e)
                    {
                        this.parent.OnException(parent, e);
                    }
                }
    
                return this.pending.Value;
            }
        }
        #endregion
    }

}
