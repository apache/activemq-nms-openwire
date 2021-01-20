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

using System.Threading;
using Apache.NMS.ActiveMQ.Threads;
using NUnit.Framework;
using System;

namespace Apache.NMS.ActiveMQ.Test.Threads
{
    [TestFixture]
    public class DedicatedTaskRunnerTest
    {

        class SimpleCountingTask : Task
        {
            private uint count;

            public SimpleCountingTask()
            {
                this.count = 0;
            }

            public bool Iterate()
            {
                count++;
                return false;
            }

            public uint Count
            {
                get { return count; }
            }
        }

        class InfiniteCountingTask : Task
        {
            private uint count;

            public InfiniteCountingTask()
            {
                this.count = 0;
            }

            public bool Iterate()
            {
                count++;
                return true;
            }

            public uint Count
            {
                get { return count; }
            }
        }

        [Test]
        public void TestSimple()
        {
            try
            {
                new DedicatedTaskRunner(null);
                Assert.Fail("Should throw a NullReferenceException");
            }
            catch(NullReferenceException)
            {
            }

            SimpleCountingTask simpleTask = new SimpleCountingTask();
            Assert.IsTrue(simpleTask.Count == 0);
            DedicatedTaskRunner simpleTaskRunner = new DedicatedTaskRunner(simpleTask);

            simpleTaskRunner.Wakeup();
            Thread.Sleep(500);
            Assert.IsTrue(simpleTask.Count >= 1);
            simpleTaskRunner.Wakeup();
            Thread.Sleep(500);
            Assert.IsTrue(simpleTask.Count >= 2);

            InfiniteCountingTask infiniteTask = new InfiniteCountingTask();
            Assert.IsTrue(infiniteTask.Count == 0);
            DedicatedTaskRunner infiniteTaskRunner = new DedicatedTaskRunner(infiniteTask);
            Thread.Sleep(500);
            Assert.IsTrue(infiniteTask.Count != 0);
            infiniteTaskRunner.Shutdown();
            uint count = infiniteTask.Count;
            Thread.Sleep(500);
            Assert.IsTrue(infiniteTask.Count == count);
        }
    }
}
