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
	public class SchedulerTest
	{
		private int counter;

		private void CounterCallback(object arg)
		{
			this.counter++;
		}

		[SetUp]
		public void SetUp()
		{
			counter = 0;
		}

		[Test]
		public void TestConstructor()
		{
		    Scheduler scheduler = new Scheduler("TestConstructor");
		    Assert.IsFalse(scheduler.Started);
		    scheduler.Start();
		    Assert.IsTrue(scheduler.Started);
		    scheduler.Stop();
		    Assert.IsFalse(scheduler.Started);
		}

		[Test]
		public void TestNullWaitCallbackThrows()
		{
		    Scheduler scheduler = new Scheduler("TestNullWaitCallbackThrows");
		    scheduler.Start();

			try
			{
				scheduler.ExecuteAfterDelay(null, null, 100);
				Assert.Fail("Should have thrown an exception");
			}
			catch (ArgumentNullException)
			{
			}

			try
			{
				scheduler.ExecuteAfterDelay(null, null, TimeSpan.FromMilliseconds(100));
				Assert.Fail("Should have thrown an exception");
			}
			catch (ArgumentNullException)
			{
			}

			try
			{
				scheduler.ExecutePeriodically(null, null, 100);
				Assert.Fail("Should have thrown an exception");
			}
			catch (ArgumentNullException)
			{
			}

			try
			{
				scheduler.ExecutePeriodically(null, null, TimeSpan.FromMilliseconds(100));
				Assert.Fail("Should have thrown an exception");
			}
			catch (ArgumentNullException)
			{
			}
		}

		[Test]
		public void TestExecutePeriodically()
		{
		    {
		        Scheduler scheduler = new Scheduler("TestExecutePeriodically");
		        scheduler.Start();
		        WaitCallback task = new WaitCallback(CounterCallback);
		        scheduler.ExecutePeriodically(task, null, 500);
		        Assert.AreEqual(0, counter);
		        Thread.Sleep(700);
		        Assert.IsTrue(counter >= 1, "Should have executed at least once.");
		        Thread.Sleep(600);
		        Assert.IsTrue(counter >= 2, "Should have executed at least twice.");
		        Assert.IsTrue(counter < 5, "Should have executed less than five times.");
		        scheduler.Stop();
		    }

			this.counter = 0;

		    {
		        Scheduler scheduler = new Scheduler("TestExecutePeriodically");
		        scheduler.Start();
		        WaitCallback task = new WaitCallback(CounterCallback);
		        scheduler.ExecutePeriodically(task, null, 1000);
		        Assert.AreEqual(0, counter);
		        scheduler.Cancel(task);
		        Thread.Sleep(1200);
		        Assert.AreEqual(0, counter);
		        scheduler.Stop();
		    }
		}

		[Test]
		public void TestExecuteAfterDelay()
		{
	        Scheduler scheduler = new Scheduler("TestExecuteAfterDelay");
	        scheduler.Start();
	        WaitCallback task = new WaitCallback(CounterCallback);
	        scheduler.ExecuteAfterDelay(task, null, 500);
	        Assert.AreEqual(0, counter);
	        Thread.Sleep(700);
	        Assert.IsTrue(counter == 1, "Should have executed at least once.");
	        Thread.Sleep(600);
	        Assert.IsTrue(counter == 1, "Should have executed no more than once.");
	        scheduler.Stop();
		}

		[Test]
		public void TestExecuteAfterDelayNoDelay()
		{
	        Scheduler scheduler = new Scheduler("TestExecuteAfterDelay");
	        scheduler.Start();
	        scheduler.ExecuteAfterDelay(CounterCallback, null, 0);
	        scheduler.ExecuteAfterDelay(CounterCallback, null, 0);
	        scheduler.ExecuteAfterDelay(CounterCallback, null, 0);
	        Thread.Sleep(500);
	        Assert.IsTrue(counter == 3, "Should have executed Three tasks.");
	        scheduler.Stop();
		}

		[Test]
		public void TestCancel()
		{
	        Scheduler scheduler = new Scheduler("TestCancel");
	        scheduler.Start();
	        WaitCallback task = new WaitCallback(CounterCallback);
	        scheduler.ExecutePeriodically(task, null, 1000);
	        Assert.AreEqual(0, counter);
	        scheduler.Cancel(task);
	        Thread.Sleep(1200);
	        Assert.AreEqual(0, counter);
	        scheduler.Stop();
		}

		[Test]
		public void TestStop()
		{
	        Scheduler scheduler = new Scheduler("TestStop");
	        scheduler.Start();
	        WaitCallback task = new WaitCallback(CounterCallback);
	        scheduler.ExecutePeriodically(task, null, 1000);
	        Assert.AreEqual(0, counter);
	        scheduler.Stop ();
	        Thread.Sleep(1200);
	        Assert.AreEqual(0, counter);
		}
	}
}

