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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.NMS.ActiveMQ.Util.Synchronization
{
    /// <summary>
    /// Goal of this is to replace lock(syncRoot) for sync and async methods, and also have Wait and Pulse(All) capabilities
    /// Relies on AsyncLocal construct, and should be valid along the flow of executioncontext
    /// </summary>
    public class NmsSynchronizationMonitor
    {
        // Main locking mechanism
        private readonly SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);

        // Lists of executions sleeping in Wait
        private readonly List<SemaphoreSlim> waitingLocks = new List<SemaphoreSlim>();

        // SyncRoot used in locking related to Wait/Pulse
        private readonly object waitSyncRoot = new object();

        // Holds RALock in current flow of execution, should be like ThreadStatic but for async flow
        private readonly AsyncLocal<NmsLock> asyncLocal;

        public NmsSynchronizationMonitor()
        {
            asyncLocal = new AsyncLocal<NmsLock>();
        }

        /// <summary>
        /// Synchronous Wait operation
        /// </summary>
        /// <param name="timeout"></param>
        public void Wait(int? timeout = null)
        {
            var raLock = GetCurrentLock();

            if (raLock == null)
            {
                throw new IllegalStateException("Wait called without acquiring Lock first");
            }

            // In one synchronized context we will Release monitor and sign ourself on list of sleeping locks
            SemaphoreSlim waitSemaphore = new SemaphoreSlim(0, 1);
            lock (waitSyncRoot)
            {
                ReleaseMonitor();
                waitingLocks.Add(waitSemaphore);
                // raLock.WaitSemaphore = new SemaphoreSlim(0, 1);
            }

            try
            {
                // Now wait, if our lock was pulsed just before, we will not really sleep, but instead continue ...
                waitSemaphore.Wait(timeout ?? System.Threading.Timeout.Infinite);

                lock (waitSyncRoot)
                {
                    waitingLocks.Remove(waitSemaphore);
                    waitSemaphore.Dispose();
                }
            }
            finally
            {
                // Enter again, but we need to use the same raLock as before
                EnterMonitor();
            }
        }


        public async Task WaitAsync(int? timeout = null)
        {
            var raLock = GetCurrentLock();

            if (raLock == null)
            {
                throw new IllegalStateException("Wait called without acquiring Lock first");
            }

            SemaphoreSlim waitSemaphore = new SemaphoreSlim(0, 1);

            lock (waitSyncRoot)
            {
                ReleaseMonitor();
                waitingLocks.Add(waitSemaphore);
            }

            try
            {
                // Here between lock and waiting is a problematic thing, two pulses can release the same thing
                await waitSemaphore.WaitAsync(timeout ?? -1).Await();

                lock (waitSyncRoot)
                {
                    waitingLocks.Remove(waitSemaphore);
                    waitSemaphore.Dispose();
                    waitSemaphore.Dispose();
                }
            }
            finally
            {
                // Enter again, but we need to use the same raLock as before, and also asyncLocal a
                await EnterMonitorAsync().Await();
            }
        }

        public void Pulse()
        {
            lock (waitSyncRoot)
            {
                var firstWaiting = waitingLocks.FirstOrDefault();
                if (firstWaiting != null)
                {
                    firstWaiting.Release();
                    waitingLocks.Remove(firstWaiting);
                }
            }
        }

        public void PulseAll()
        {
            lock (waitSyncRoot)
            {
                waitingLocks.ForEach(a => { a.Release(); });
                waitingLocks.Clear();
            }
        }


        /// <summary>
        /// Allows to create a sub context where asyncLocal will be removed and thus not passed for example to something that could carry it and thus has wrong locks acquired
        /// </summary>
        /// <returns></returns>
        public IDisposable Exclude()
        {
            return new ExcludeLock(this);
        }


        public NmsLock Lock()
        {
            NmsLock nmsLock = GetOrCreateCurrentLock();
            nmsLock.Enter();
            return nmsLock;
        }

        public Task<NmsLock>
            LockAsync() // This should not be async method, cause setting asyncLocal inside GetOrCreateCurrentLock may be only limited to this method in such case
        {
            NmsLock nmsLock = GetOrCreateCurrentLock();
            return nmsLock.EnterAsync();
        }

        public Task<NmsLock>
            TryLockAsync(
                int timeout) // This should not be async method, cause setting asyncLocal inside GetOrCreateCurrentLock may be only limited to this method in such case
        {
            NmsLock nmsLock = GetOrCreateCurrentLock();
            return TryEnterAsync(timeout, nmsLock);
        }

        private async Task<NmsLock> TryEnterAsync(int timeout, NmsLock nmsLock)
        {
            try
            {
                nmsLock = await nmsLock.EnterAsync(timeout).Await();
                return nmsLock;
            }
            catch (Exception)
            {
                return null;
            }
        }


        private NmsLock GetOrCreateCurrentLock()
        {
            if (asyncLocal.Value == null)
            {
                asyncLocal.Value = new NmsLock(this);
            }

            return asyncLocal.Value;
        }

        private NmsLock GetCurrentLock()
        {
            var context = asyncLocal.Value;
            return context;
        }

        private void SetCurrentLock(NmsLock nmsLock)
        {
            asyncLocal.Value = nmsLock;
        }

        private bool EnterMonitor(int timeout = System.Threading.Timeout.Infinite)
        {
            return semaphoreSlim.Wait(timeout);
        }

        private Task<bool> EnterMonitorAsync(int timeout = System.Threading.Timeout.Infinite)
        {
            return semaphoreSlim.WaitAsync(timeout);
        }

        private void ReleaseMonitor()
        {
            semaphoreSlim.Release();
        }

        public  class NmsLock : IDisposable
        {
            private int NestCounter { get; set; }

            private readonly NmsSynchronizationMonitor parent;

            public NmsLock(NmsSynchronizationMonitor parent)
            {
                this.parent = parent;
            }

            public void Enter(int timeout = System.Threading.Timeout.Infinite)
            {
                bool entered = true;
                if (NestCounter == 0)
                {
                    entered = parent.EnterMonitor(timeout);
                }

                CheckIfEntered(entered);

                NestCounter++;
            }

            public async Task<NmsLock> EnterAsync(int timeout = System.Threading.Timeout.Infinite)
            {
                bool entered = true;
                if (NestCounter == 0)
                {
                    entered = await parent.EnterMonitorAsync(timeout).Await();
                }

                CheckIfEntered(entered);

                NestCounter++;
                return this;
            }

            private void CheckIfEntered(bool entered)
            {
                if (!entered)
                {
                    throw new IllegalStateException();
                }
            }
            
            
            private void Leave()
            {
                NestCounter--;
                if (NestCounter <= 0)
                {
                    parent.ReleaseMonitor();
                    parent.SetCurrentLock(null);
                }
            }

            public void Dispose()
            {
                Leave();
            }
        }
        
        private class ExcludeLock : IDisposable
        {
            private readonly NmsSynchronizationMonitor parent;

            private readonly NmsLock currentLock;

            public ExcludeLock(NmsSynchronizationMonitor parent)
            {
                this.parent = parent;

                currentLock = parent.GetCurrentLock();
                parent.SetCurrentLock(null);
            }

            public void Dispose()
            {
                parent.SetCurrentLock(this.currentLock);
            }
        }
    }
}