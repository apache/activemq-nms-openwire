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
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Threads;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class ThreadPoolExecutorTest
    {
        private const int JOB_COUNT = 100;
        private ManualResetEvent complete = new ManualResetEvent(false);
        private bool waitingTaskCompleted = false;
        private CountDownLatch doneLatch;
        private int count = 0;

        internal class DummyClass
        {
            private int data;

            public DummyClass(int data)
            {
                this.data = data;
            }

            public int Data
            {
                get { return data; }
            }
        }

        public ThreadPoolExecutorTest()
        {
        }

        private void TaskThatSignalsWhenItsComplete(object unused)
        {
            waitingTaskCompleted = true;
            complete.Set();
        }

        private void TaskThatCountsDown(object unused)
        {
            doneLatch.countDown();
        }

        private void TaskThatSleeps(object unused)
        {
            Thread.Sleep(5000);
        }

        private void TaskThatIncrementsCount(object unused)
        {
            count++;
        }

        private void TaskThatThrowsAnException(object unused)
        {
            throw new Exception("Throwing an Exception just because");
        }

        private void TaskThatValidatesTheArg(object arg)
        {
            DummyClass state = arg as DummyClass;
            if(state != null && state.Data == 10 )
            {
                waitingTaskCompleted = true;
            }
            complete.Set();
        }

	    /// <summary>
	    /// Wait out termination of a thread pool or fail doing so
	    /// </summary>
	    public void JoinPool(ThreadPoolExecutor exec) 
		{
	        try 
			{
	            exec.Shutdown();
	            Assert.IsTrue(exec.AwaitTermination(TimeSpan.FromSeconds(20)));
	        } 
			catch(Exception) 
			{
	            Assert.Fail("Unexpected exception");
	        }
	    }

        [SetUp]
        public void SetUp()
        {
            this.complete.Reset();
            this.waitingTaskCompleted = false;
            this.doneLatch = new CountDownLatch(JOB_COUNT);
            this.count = 0;
        }

        [Test]
        public void TestConstructor()
        {
            ThreadPoolExecutor executor = new ThreadPoolExecutor();
            Assert.IsNotNull(executor);
            Assert.IsFalse(executor.IsShutdown);
            executor.Shutdown();
            Assert.IsTrue(executor.IsShutdown);
        }

        [Test]
        public void TestSingleTaskExecuted()
        {
            ThreadPoolExecutor executor = new ThreadPoolExecutor();
            Assert.IsNotNull(executor);
            Assert.IsFalse(executor.IsShutdown);

            executor.QueueUserWorkItem(TaskThatSignalsWhenItsComplete);

            this.complete.WaitOne();
            Assert.IsTrue(this.waitingTaskCompleted);

            executor.Shutdown();
			JoinPool(executor);
        }

        [Test]
        public void TestTaskParamIsPropagated()
        {
            ThreadPoolExecutor executor = new ThreadPoolExecutor();
            Assert.IsNotNull(executor);
            Assert.IsFalse(executor.IsShutdown);

            executor.QueueUserWorkItem(TaskThatValidatesTheArg, new DummyClass(10));

            this.complete.WaitOne();
            Assert.IsTrue(this.waitingTaskCompleted);

            executor.Shutdown();
            Assert.IsTrue(executor.IsShutdown);
        }

        [Test]
        public void TestAllTasksComplete()
        {
            ThreadPoolExecutor executor = new ThreadPoolExecutor();
            Assert.IsNotNull(executor);
            Assert.IsFalse(executor.IsShutdown);

            for(int i = 0; i < JOB_COUNT; ++i)
            {
                executor.QueueUserWorkItem(TaskThatCountsDown);
            }

            Assert.IsTrue(this.doneLatch.await(TimeSpan.FromMilliseconds(30 * 1000)));

            executor.Shutdown();
			JoinPool(executor);
            Assert.IsTrue(executor.IsShutdown);
        }

        [Test]
        public void TestAllTasksCompleteAfterException()
        {
            ThreadPoolExecutor executor = new ThreadPoolExecutor();
            Assert.IsNotNull(executor);
            Assert.IsFalse(executor.IsShutdown);

            executor.QueueUserWorkItem(TaskThatThrowsAnException);

            for(int i = 0; i < JOB_COUNT; ++i)
            {
                executor.QueueUserWorkItem(TaskThatCountsDown);
            }

            Assert.IsTrue(this.doneLatch.await(TimeSpan.FromMilliseconds(30 * 1000)));

            executor.Shutdown();
			JoinPool(executor);
            Assert.IsTrue(executor.IsShutdown);
        }

        [Test]
        public void TestThatShutdownDoesntPurgeTasks()
        {
            ThreadPoolExecutor executor = new ThreadPoolExecutor();
            Assert.IsNotNull(executor);
            Assert.IsFalse(executor.IsShutdown);

            executor.QueueUserWorkItem(TaskThatSleeps);

            for(int i = 0; i < JOB_COUNT; ++i)
            {
                executor.QueueUserWorkItem(TaskThatIncrementsCount);
            }

            executor.Shutdown();

            Thread.Sleep(100);

			JoinPool(executor);

            Assert.AreEqual(JOB_COUNT, count);
            Assert.IsTrue(executor.IsShutdown);
        }

		[Test]
	    public void TestIsTerminated() 
		{
            ThreadPoolExecutor executor = new ThreadPoolExecutor();
            Assert.IsNotNull(executor);
            Assert.IsFalse(executor.IsShutdown);
            Assert.IsFalse(executor.IsTerminated);

            executor.QueueUserWorkItem(TaskThatSleeps);
            executor.Shutdown();

			JoinPool(executor);
            Assert.IsTrue(executor.IsTerminated);
		}

		[Test]
	    public void TestAwaitTermination() 
		{
            ThreadPoolExecutor executor = new ThreadPoolExecutor();
            Assert.IsNotNull(executor);
            Assert.IsFalse(executor.IsShutdown);
            Assert.IsFalse(executor.IsTerminated);

            executor.QueueUserWorkItem(TaskThatSleeps);
            executor.Shutdown();

            Assert.IsFalse(executor.IsTerminated, "Terminated before await.");
			Assert.IsFalse(executor.AwaitTermination(TimeSpan.FromMilliseconds(500)), "Should be terminated yet.");
            Assert.IsFalse(executor.IsTerminated, "Terminated after await.");

			JoinPool(executor);
            Assert.IsTrue(executor.IsTerminated);
		}
    }
}

