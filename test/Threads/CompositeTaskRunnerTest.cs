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
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test.Threads
{
    [TestFixture]
    public class CompositeTaskRunnerTest
    {
        class CountingTask : CompositeTask
        {
            private int count;
            private int goal;
            private string name;

            public CountingTask( string name, int goal )
            {
                this.name = name;
                this.goal = goal;
            }

            public string Name
            {
                get { return name; }
            }

            public int Count
            {
                get { return count; }
            }

            public bool IsPending
            {
                get { return count != goal; }
            }

            public bool Iterate()
            {
                return !( ++count == goal );
            }
        }

        [Test]
        public void TestCompositeTaskRunner()
        {

            int attempts = 0;

            CompositeTaskRunner runner = new CompositeTaskRunner();

            CountingTask task1 = new CountingTask("task1", 100);
            CountingTask task2 = new CountingTask("task2", 200);

            runner.AddTask( task1 );
            runner.AddTask( task2 );

            runner.Wakeup();

            while( attempts++ != 10 )
            {
                Thread.Sleep( 1000 );

                if(task1.Count == 100 && task2.Count == 200)
                {
                    break;
                }
            }

            Assert.IsTrue(task1.Count == 100);
            Assert.IsTrue(task2.Count == 200);

            runner.RemoveTask(task1);
            runner.RemoveTask(task2);
        }

        [Test]
        public void CompositeTaskRunnerDoesntHoldLockWhileCallingIterate()
        {
            object lockObj = new object();
            
            // Start a task running that takes a shared lock during it's iterate.
            CompositeTaskRunner runner = new CompositeTaskRunner();
            runner.AddTask(new LockingTask(lockObj));
           
            // Start a separate thread that holds that same lock whilst manipulating the CompositeTaskRunner (See InactivityMonitor for real example).
            AutoResetEvent resetEvent = new AutoResetEvent(false);

            ThreadPool.QueueUserWorkItem(_ =>
            {
                for (int i = 0; i < 10000; i++)
                {
                    lock (lockObj)
                    {
                        var countingTask = new CountingTask("task1", 100);
                        runner.AddTask(countingTask);
                        runner.RemoveTask(countingTask);
                    }
                }

                resetEvent.Set();
            });

            // Wait for the second thread to finish 10000 attempts.
            Assert.That(resetEvent.WaitOne(TimeSpan.FromSeconds(10)), "The secondary lock user didn't finish 10000 iterations in less than 10 seconds. Probably dead locked!");
            runner.Shutdown();
        }

        private class LockingTask : CompositeTask
        {
            private readonly object _lockObj;

            public LockingTask(object lockObj)
            {
                _lockObj = lockObj;
                IsPending = true;
            }

            public bool Iterate()
            {
                lock (_lockObj)
                {
                    Thread.Sleep(10);
                }
                return true;
            }

            public bool IsPending { get; private set; }
        }
    }
}