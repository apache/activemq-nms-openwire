/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Threading;

namespace Apache.NMS.ActiveMQ.Threads
{
    /// <summary>
    /// This class provides a wrapper around the ThreadPool mechanism in .NET
    /// to allow for serial execution of jobs in the ThreadPool and provide
    /// a means of shutting down the execution of jobs in a deterministic
    /// way.
    /// </summary>
    public class ThreadPoolExecutor
    {
        private Queue<Future> workQueue = new Queue<Future>();
        private Mutex syncRoot = new Mutex();
        private bool running = false;
        private bool closing = false;
        private bool closed = false;
        private ManualResetEvent executionComplete = new ManualResetEvent(true);
        private Thread workThread = null;

        /// <summary>
        /// Represents an asynchronous task that is executed on the ThreadPool
        /// at some point in the future.
        /// </summary>
        internal class Future
        {
            private readonly WaitCallback callback;
            private readonly object callbackArg;

            public Future(WaitCallback callback, object arg)
            {
                this.callback = callback;
                this.callbackArg = arg;
            }

            public void Run()
            {
                if(this.callback == null)
                {
                    throw new Exception("Future executed with null WaitCallback");
                }

                try
                {
                    this.callback(callbackArg);
                }
                catch
                {
                }
            }
        }

        public void QueueUserWorkItem(WaitCallback worker)
        {
            this.QueueUserWorkItem(worker, null);
        }

        public void QueueUserWorkItem(WaitCallback worker, object arg)
        {
            if(worker == null)
            {
                throw new ArgumentNullException("Invalid WaitCallback passed");
            }

            if(!this.closed)
            {
                lock(syncRoot)
                {
                    if(!this.closed || !this.closing)
                    {
                        this.workQueue.Enqueue(new Future(worker, arg));

                        if(!this.running)
                        {
                            this.executionComplete.Reset();
                            this.running = true;
                            ThreadPool.QueueUserWorkItem(new WaitCallback(QueueProcessor), null);
                        }
                    }
                }
            }
        }

        public bool IsShutdown
        {
            get { return this.closed; }
        }

        public void Shutdown()
        {
            if(!this.closed)
            {
                syncRoot.WaitOne();

                if(!this.closed)
                {
                    this.closing = true;
                    this.workQueue.Clear();

                    if(this.running && Thread.CurrentThread != this.workThread)
                    {
                        syncRoot.ReleaseMutex();
                        this.executionComplete.WaitOne();
                        syncRoot.WaitOne();
                    }

                    this.closed = true;
                }

                syncRoot.ReleaseMutex();
            }
        }

        private void QueueProcessor(object unused)
        {
            Future theTask = null;

            lock(syncRoot)
            {
                this.workThread = Thread.CurrentThread;

                if(this.workQueue.Count == 0 || this.closing)
                {
                    this.running = false;
                    this.executionComplete.Set();
                    return;
                }

                theTask = this.workQueue.Dequeue();
            }

            try
            {
                theTask.Run();
            }
            finally
            {
                this.workThread = null;

                if(this.closing)
                {
                    this.running = false;
                    this.executionComplete.Set();
                }
                else
                {
                    ThreadPool.QueueUserWorkItem(new WaitCallback(QueueProcessor), null);
                }
            }
        }
    }
}

