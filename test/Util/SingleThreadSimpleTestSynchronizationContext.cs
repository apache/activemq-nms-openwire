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
using System.Threading;

namespace Apache.NMS.ActiveMQ.Test
{
    /// <summary>
    /// Very simple, basic and incomplete, just trying to recreate single threaded synchronization context for tests
    /// </summary>
    internal class SingleThreadSimpleTestSynchronizationContext : SynchronizationContext
    {
        private Queue<Work> queue = new Queue<Work>();

        public SingleThreadSimpleTestSynchronizationContext()
        {
            var th = new Thread(Run);
            th.IsBackground = true;
            th.Name = nameof(SingleThreadSimpleTestSynchronizationContext);
            th.Start();
        }

        public SingleThreadSimpleTestSynchronizationContext(Queue<Work> queue)
        {
            this.queue = queue;
        }

        internal class Work
        {
            public SendOrPostCallback Callback { get; set; }
            public object State { get; set; }
        }

       

        public override void Post(SendOrPostCallback d, object? state)
        {
            queue.Enqueue(new Work() {Callback = d, State = state});
        }

        public override void Send(SendOrPostCallback d, object? state)
        {
            if (SynchronizationContext.Current == this)
            {
                d(state);
            }
            else
            {
                queue.Enqueue(new Work() {Callback = d, State = state});
            }
        }

        private void Run()
        {
            SynchronizationContext.SetSynchronizationContext(this);
            while (true)
            {
                if (queue.Count > 0)
                {
                    var work = queue.Dequeue();
                    Console.WriteLine(SynchronizationContext.Current);
                    work.Callback.Invoke(work.State);
                }
                else
                {
                    Thread.Sleep(1);
                }
            }
        }
        
        public override SynchronizationContext CreateCopy()
        {
            return new SingleThreadSimpleTestSynchronizationContext(this.queue);
        }

        public override void OperationCompleted()
        {
            throw new NotSupportedException();
        }

        public override void OperationStarted()
        {
            throw new NotSupportedException();
        }
        
        

        public override int Wait(IntPtr[] waitHandles, bool waitAll, int millisecondsTimeout)
        {
            throw new NotSupportedException();
        }
    }
}