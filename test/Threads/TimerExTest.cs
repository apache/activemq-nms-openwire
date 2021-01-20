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
	public class TimerExTest
	{
		TestData data;

		class TestData
		{
			public object sync = new object();
			public int timerCounter = 0;
		}

		class TimerTestTask : TimerTask
		{
			int wasRun = 0;

			// Should we sleep for 200 ms each run()?
			bool sleepInRun = false;

			// Should we increment the timerCounter?
			bool incrementCount = false;

			// Should we terminate the timer at a specific timerCounter?
			int terminateCount = -1;

			// The timer we belong to
			TimerEx timer = null;

			TestData data;

			public TimerTestTask(TestData data) 
			{
				this.data = data;
			}

			public TimerTestTask(TimerEx t, TestData data) 
			{
				this.timer = t;
				this.data = data;
			}

			public override void Run() 
			{
				lock(this) 
				{
					wasRun++;
				}
				if (incrementCount)
				{
					data.timerCounter++;
				}
				if (terminateCount == data.timerCounter && timer != null)
				{
					timer.Cancel();
				}
				if (sleepInRun) 
				{
					try 
					{
						Thread.Sleep(200);
					}
					catch (ThreadInterruptedException) 
					{
					}
				}

				lock(data.sync)
				{
					Monitor.Pulse(data.sync);
				}
			}

			public int WasRun()
			{
				lock(this)
				{
					return wasRun;
				}
			}

			public void SleepInRun(bool sleepInRun) 
			{
				this.sleepInRun = sleepInRun;
			}

			public void IncrementCount(bool incrementCount) 
			{
				this.incrementCount = incrementCount;
			}

			public void TerminateCount(int terminateCount) 
			{
				this.terminateCount = terminateCount;
			}
		}

		private void WaitCallbackTask(object arg)
		{
			TimerTestTask task = arg as TimerTestTask;
			task.Run();
		}

		class SlowThenFastTask : TimerTask 
		{
			int wasRun = 0;
			DateTime startedAt;
			TimeSpan lastDelta;

			public override void Run() 
			{
				if (wasRun == 0)
				{
					startedAt = DateTime.Now;
				}
				lastDelta = DateTime.Now - 
					(startedAt + (TimeSpan.FromMilliseconds(100 * wasRun)));
				wasRun++;
				if (wasRun == 2) 
				{
					try 
					{
						Thread.Sleep(200);
					}
					catch (ThreadInterruptedException) 
					{
					}
				}
			}

			public TimeSpan LastDelta
			{
				get { return lastDelta; }
			}

			public int WasRun() 
			{
				return wasRun;
			}
		}

		private void Sleep(int milliseconds)
		{
			try 
			{
				Thread.Sleep(milliseconds);
			}
			catch (ThreadInterruptedException) 
			{
			}
		}

		private void Wait(int milliseconds)
		{
			lock (data.sync) 
			{
				try 
				{
					Monitor.Wait(data.sync, milliseconds);
				}
				catch (ThreadInterruptedException) 
				{
				}
			}
		}

		[SetUp]
		public void SetUp()
		{
			this.data = new TestData();
		}

		[Test]
		public void TestConstructorBool() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a task is run
				t = new TimerEx(true);
				TimerTestTask testTask = new TimerTestTask(data);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(200));
				Wait(1000);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.Run() method not called after 200ms");
				t.Cancel();
			} 
			finally 
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestConstructor() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(200));
				Wait(1000);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.Run() method not called after 200ms");
				t.Cancel();
			} 
			finally 
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestConstructorStringBool() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a task is run
				t = new TimerEx("TestConstructorStringBool", true);
				TimerTestTask testTask = new TimerTestTask(data);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(200));
				Wait(1000);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.Run() method not called after 200ms");
				t.Cancel();
			} 
			finally 
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestConstructorString() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a task is run
				t = new TimerEx("TestConstructorString");
				TimerTestTask testTask = new TimerTestTask(data);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(200));
				Wait(1000);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.Run() method not called after 200ms");
				t.Cancel();
			}
			finally 
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
	    public void TestConstructorThrowsException() 
		{
	        try 
			{
	            new TimerEx(null, true);
	            Assert.Fail("NullReferenceException expected");
	        }
			catch (NullReferenceException) 
			{
	            //expected
	        }

	        try 
			{
	            new TimerEx(null, false);
	            Assert.Fail("NullReferenceException expected");
	        }
			catch (NullReferenceException) 
			{
	            //expected
	        }
	    }

		[Test]
		public void TestCancel() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a task throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				t.Cancel();
				bool exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200));
				}
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception, "Scheduling a task after Timer.Cancel() should throw exception");

				// Ensure a task is run but not after cancel
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500));
				Wait(1000);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.run() method not called after 200ms");
				t.Cancel();
				Wait(500);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.run() method should not have been called after cancel");

				// Ensure you can call cancel more than once
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(500));
				Wait(500);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.run() method not called after 200ms");
				t.Cancel();
				t.Cancel();
				t.Cancel();
				Wait(500);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.run() method should not have been called after cancel");

				// Ensure that a call to cancel from within a timer ensures no more
				// run
				t = new TimerEx();
				testTask = new TimerTestTask(t, data);
				testTask.IncrementCount(true);
				testTask.TerminateCount(5); // Terminate after 5 runs
				t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
				lock (data.sync) 
				{
					try 
					{
						Monitor.Wait(data.sync, 500);
						Monitor.Wait(data.sync, 500);
						Monitor.Wait(data.sync, 500);
						Monitor.Wait(data.sync, 500);
						Monitor.Wait(data.sync, 500);
						Monitor.Wait(data.sync, 500);
					}
					catch (ThreadInterruptedException) 
					{
					}
				}
				Assert.AreEqual(5, testTask.WasRun(),
					"TimerTask.run() method should be called 5 times not " + testTask.WasRun());
				t.Cancel();
				Sleep(200);
			} 
			finally 
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
	    public void TestPurge()
		{
	        TimerEx t = null;
	        try 
			{
	            t = new TimerEx();
	            Assert.AreEqual(0, t.Purge());

	            TimerTestTask[] tasks = new TimerTestTask[100];
	            int[] delayTime = { 50, 80, 20, 70, 40, 10, 90, 30, 60 };

	            int j = 0;
	            for (int i = 0; i < 100; i++) 
				{
	                tasks[i] = new TimerTestTask(data);
	                t.Schedule(tasks[i], TimeSpan.FromMilliseconds(delayTime[j++]), TimeSpan.FromMilliseconds(200));
	                if (j == 9) 
					{
	                    j = 0;
	                }
	            }

	            for (int i = 0; i < 50; i++)
				{
	                tasks[i].Cancel();
	            }

	            Assert.IsTrue(t.Purge() <= 50);
	            Assert.AreEqual(0, t.Purge());
	        } 
			finally 
			{
	            if (t != null) 
				{
	                t.Cancel();
	            }
	        }
	    }

		[Test]
		public void TestScheduleWaitCallbackDateTime() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a TimerEx throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				DateTime d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.Cancel();
				bool exception = false;
				try 
				{
					t.Schedule(callback, testTask, d);
				}
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a task after Timer.Cancel() should throw exception");

				// Ensure that the returned task from the WaitCallback schedule method
				// cancel the task and prevent it from being run.
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				d = DateTime.Now + TimeSpan.FromMilliseconds(200);
				TimerTask scheduled = t.Schedule(callback, testTask, d);
				scheduled.Cancel();
				Sleep(1000);
				Assert.AreEqual(0, testTask.WasRun(), "Cancelled task shouldn't have run.");

				// Ensure a TimerEx throws a ArgumentNullException if the task is null
				t = new TimerEx();
				exception = false;
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				try 
				{
					t.Schedule(null, testTask, d);
				}
				catch (ArgumentNullException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a null task should throw ArgumentNullException");
				t.Cancel();

				// Ensure a task is run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				d = DateTime.Now + TimeSpan.FromMilliseconds(200);
				t.Schedule(callback, testTask, d);
				Sleep(400);

				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.run() method not called after 200ms");
				t.Cancel();

				// Ensure multiple tasks are run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.Schedule(callback, testTask, d);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(150);
				t.Schedule(callback, testTask, d);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(70);
				t.Schedule(callback, testTask, d);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(10);
				t.Schedule(callback, testTask, d);
				Sleep(400);

				Assert.AreEqual(4, data.timerCounter,
					"Multiple tasks should have incremented counter 4 times not " + data.timerCounter);
				t.Cancel();
			}
			finally 
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleWaitCallbackWithDelay()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				t.Schedule(callback, testTask, 200);
				Sleep(1000);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.run() method not called after 200ms");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleWaitCallbackWithDelayAsTimeSpan()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				t.Schedule(callback, testTask, TimeSpan.FromMilliseconds(200));
				Sleep(1000);
				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.run() method not called after 200ms");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleWaitCallbackWithDelayAndPeriod()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				t.Schedule(callback, testTask, 200, 100);
				Sleep(1000);
				Assert.IsTrue(testTask.WasRun() >= 2, 
				    "TimerTask.run() method not called at least twice after 1 second sleep");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleWaitCallbackWithDelayAndPeriodTimeSpan()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				t.Schedule(callback, testTask, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(100));
				Sleep(1000);
				Assert.IsTrue(testTask.WasRun() >= 2, 
				    "TimerTask.run() method not called at least twice after 1 second sleep");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleWaitCallbackWithDateTimePeriod()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				DateTime d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.Schedule(callback, testTask, d, 100);
				Sleep(1000);
				Assert.IsTrue(testTask.WasRun() >= 2, 
				    "TimerTask.run() method not called at least twice after 1 second sleep");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleWaitCallbackWithDateTimePeriodTimeSpan()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				DateTime d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.Schedule(callback, testTask, d, TimeSpan.FromMilliseconds(100));
				Sleep(1000);
				Assert.IsTrue(testTask.WasRun() >= 2, 
				    "TimerTask.run() method not called at least twice after 1 second sleep");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleAtFixedRateWaitCallbackWithDelayPeriod()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				t.ScheduleAtFixedRate(callback, testTask, 200, 100);
				Sleep(1000);
				Assert.IsTrue(testTask.WasRun() >= 2, 
				    "TimerTask.run() method not called at least twice after 1 second sleep");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleAtFixedRateWaitCallbackWithDelayPeriodTimeSpan()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				t.ScheduleAtFixedRate(
					callback, testTask, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(100));
				Sleep(1000);
				Assert.IsTrue(testTask.WasRun() >= 2, 
				    "TimerTask.run() method not called at least twice after 1 second sleep");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleAtFixedRateWaitCallbackWithDateTimePeriod()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				DateTime d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.ScheduleAtFixedRate(callback, testTask, d, 100);
				Sleep(1000);
				Assert.IsTrue(testTask.WasRun() >= 2, 
				    "TimerTask.run() method not called at least twice after 1 second sleep");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleAtFixedRateWaitCallbackWithDateTimePeriodTimeSpan()
		{
			TimerEx t = null;
			try
			{
				// Ensure a task is run
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				WaitCallback callback = new WaitCallback(WaitCallbackTask);
				DateTime d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.ScheduleAtFixedRate(callback, testTask, d, TimeSpan.FromMilliseconds(100));
				Sleep(1000);
				Assert.IsTrue(testTask.WasRun() >= 2, 
				    "TimerTask.run() method not called at least twice after 1 second sleep");
				t.Cancel();
			}
			finally
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleTimerTaskDateTime() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a TimerEx throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				DateTime d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.Cancel();
				bool exception = false;
				try 
				{
					t.Schedule(testTask, d);
				}
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a task after Timer.Cancel() should throw exception");

				// Ensure a TimerEx throws an InvalidOperationException if task already cancelled
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				testTask.Cancel();
				exception = false;
				try 
				{
					t.Schedule(testTask, d);
				}
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a task after cancelling it should throw exception");
				t.Cancel();

				// Ensure a TimerEx throws a ArgumentNullException if the task is null
				t = new TimerEx();
				exception = false;
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				try 
				{
					t.Schedule(null, d);
				}
				catch (ArgumentNullException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a null task should throw ArgumentNullException");
				t.Cancel();

				// Ensure a task is run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				d = DateTime.Now + TimeSpan.FromMilliseconds(200);
				t.Schedule(testTask, d);
				Sleep(400);

				Assert.AreEqual(1, testTask.WasRun(), "TimerTask.run() method not called after 200ms");
				t.Cancel();

				// Ensure multiple tasks are run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.Schedule(testTask, d);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(150);
				t.Schedule(testTask, d);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(70);
				t.Schedule(testTask, d);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(10);
				t.Schedule(testTask, d);
				Sleep(400);

				Assert.AreEqual(4, data.timerCounter,
					"Multiple tasks should have incremented counter 4 times not " + data.timerCounter);
				t.Cancel();
			}
			finally 
			{
				if (t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleTimerTaskDelay() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a TimerEx throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				t.Cancel();
				bool exception = false;
				try 
				{
					t.Schedule(testTask, 100);
				} 
				catch (InvalidOperationException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a task after Timer.Cancel() should throw exception");

				// Ensure a TimerEx throws an InvalidOperationException if task already
				// cancelled
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.Cancel();
				exception = false;
				try 
				{
					t.Schedule(testTask, 100);
				} 
				catch (InvalidOperationException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task after cancelling it should throw exception");
				t.Cancel();

				// Ensure a TimerEx throws an ArgumentOutOfRangeException if delay is
				// negative
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.Schedule(testTask, -100);
				}
				catch (ArgumentOutOfRangeException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a task with negative delay should throw IllegalArgumentException");
				t.Cancel();

				// Ensure a TimerEx throws a ArgumentNullException if the task is null
				t = new TimerEx();
				exception = false;
				try
				{
					t.Schedule(null, 10);
				} 
				catch (ArgumentNullException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a null task should throw ArgumentNullException");
				t.Cancel();

				// Ensure proper sequence of exceptions
				t = new TimerEx();
				exception = false;
				try 
				{
					t.Schedule(null, -10);
				}
				catch (ArgumentNullException)
				{
				}
				catch (ArgumentOutOfRangeException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception, 
					"Scheduling a null task with negative delays should throw IllegalArgumentException first");
				t.Cancel();

				// Ensure a task is run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				t.Schedule(testTask, 200);
				Sleep(400);
				Assert.AreEqual(1, testTask.WasRun(),
					"TimerTask.run() method not called after 200ms");
				t.Cancel();

				// Ensure multiple tasks are run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, 100);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, 150);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, 70);
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, 10);
				Sleep(400);
				Assert.AreEqual(4, data.timerCounter,
					"Multiple tasks should have incremented counter 4 times not " + data.timerCounter);
				t.Cancel();
			}
			finally 
			{
				if(t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleTimerTaskDelayTimeSpan() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a TimerEx throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				t.Cancel();
				bool exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(100));
				} 
				catch (InvalidOperationException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a task after Timer.Cancel() should throw exception");

				// Ensure a TimerEx throws an InvalidOperationException if task already
				// cancelled
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.Cancel();
				exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(100));
				} 
				catch (InvalidOperationException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task after cancelling it should throw exception");
				t.Cancel();

				// Ensure a TimerEx throws an ArgumentOutOfRangeException if delay is
				// negative
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(-100));
				}
				catch (ArgumentOutOfRangeException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a task with negative delay should throw IllegalArgumentException");
				t.Cancel();

				// Ensure a TimerEx throws a ArgumentNullException if the task is null
				t = new TimerEx();
				exception = false;
				try
				{
					t.Schedule(null, TimeSpan.FromMilliseconds(10));
				} 
				catch (ArgumentNullException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a null task should throw ArgumentNullException");
				t.Cancel();

				// Ensure proper sequence of exceptions
				t = new TimerEx();
				exception = false;
				try 
				{
					t.Schedule(null, TimeSpan.FromMilliseconds(-10));
				}
				catch (ArgumentNullException)
				{
				}
				catch (ArgumentOutOfRangeException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception, 
					"Scheduling a null task with negative delays should throw IllegalArgumentException first");
				t.Cancel();

				// Ensure a task is run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(200));
				Sleep(400);
				Assert.AreEqual(1, testTask.WasRun(),
					"TimerTask.run() method not called after 200ms");
				t.Cancel();

				// Ensure multiple tasks are run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(100));
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(150));
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(70));
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(10));
				Sleep(400);
				Assert.AreEqual(4, data.timerCounter,
					"Multiple tasks should have incremented counter 4 times not " + data.timerCounter);
				t.Cancel();
			}
			finally 
			{
				if(t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleTimerTaskDelayPeriod() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a TimerEx throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				t.Cancel();
				bool exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
				}
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task after Timer.Cancel() should throw exception");

				// Ensure a TimerEx throws an InvalidOperationException if task already
				// cancelled
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.Cancel();
				exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
				}
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
					"Scheduling a task after cancelling it should throw exception");
				t.Cancel();

				// Ensure a TimerEx throws an ArgumentOutOfRangeException if delay is  negative
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(-100), TimeSpan.FromMilliseconds(100));
				}
				catch (ArgumentOutOfRangeException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task with negative delay should throw IllegalArgumentException");
				t.Cancel();

				// Ensure a TimerEx throws an ArgumentOutOfRangeException if period is negative
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(-100));
				}
				catch (ArgumentOutOfRangeException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task with negative period should throw IllegalArgumentException");
				t.Cancel();

				// Ensure a TimerEx throws an ArgumentOutOfRangeException if period is
				// zero
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(0));
				} 
				catch (ArgumentOutOfRangeException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task with 0 period should throw IllegalArgumentException");
				t.Cancel();

				// Ensure a TimerEx throws a ArgumentNullException if the task is null
				t = new TimerEx();
				exception = false;
				try 
				{
					t.Schedule(null, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));
				} 
				catch (ArgumentNullException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a null task should throw ArgumentNullException");
				t.Cancel();

				// Ensure proper sequence of exceptions
				t = new TimerEx();
				exception = false;
				try
				{
					t.Schedule(null, TimeSpan.FromMilliseconds(-10), TimeSpan.FromMilliseconds(-10));
				} 
				catch (ArgumentNullException) 
				{
				} 
				catch (ArgumentOutOfRangeException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a null task with negative delays should throw ArgumentOutOfRangeException first");
				t.Cancel();

				// Ensure a task is run at least twice
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
				Sleep(400);
				Assert.IsTrue(testTask.WasRun() >= 2,
					"TimerTask.run() method should have been called at least twice (" + testTask.WasRun() + ")");
				t.Cancel();

				// Ensure multiple tasks are run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100)); // at least 9 times
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(100)); // at least 7 times
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(200)); // at least 4 times
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				t.Schedule(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200)); // at least 4 times
				Sleep(1200);
				Assert.IsTrue(data.timerCounter >= 24,
					"Multiple tasks should have incremented counter 24 times not " + data.timerCounter);
				t.Cancel();
			}
			finally 
			{
				if(t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleTimerTaskDateTimePeriod() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a TimerEx throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				DateTime d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.Cancel();
				bool exception = false;
				try 
				{
					t.Schedule(testTask, d, TimeSpan.FromMilliseconds(100));
				}
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task after Timer.Cancel() should throw exception");

				// Ensure a TimerEx throws an InvalidOperationException if task already cancelled
				t = new TimerEx();
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				testTask = new TimerTestTask(data);
				testTask.Cancel();
				exception = false;
				try 
				{
					t.Schedule(testTask, d, TimeSpan.FromMilliseconds(100));
				} 
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task after cancelling it should throw exception");
				t.Cancel();

				// Ensure a TimerEx throws an ArgumentOutOfRangeException if period is
				// negative
				t = new TimerEx();
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.Schedule(testTask, d, TimeSpan.FromMilliseconds(-100));
				}
				catch (ArgumentOutOfRangeException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a task with negative period should throw IllegalArgumentException");
				t.Cancel();

				// Ensure a TimerEx throws a ArgumentNullException if the task is null
				t = new TimerEx();
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				exception = false;
				try 
				{
					t.Schedule(null, d, TimeSpan.FromMilliseconds(10));
				}
				catch (ArgumentNullException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"Scheduling a null task should throw ArgumentNullException");
				t.Cancel();

				// Ensure a task is run at least twice
				t = new TimerEx();
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				testTask = new TimerTestTask(data);
				t.Schedule(testTask, d, TimeSpan.FromMilliseconds(100));
				Sleep(800);
				Assert.IsTrue( testTask.WasRun() >= 2,
					"TimerTask.Run() method should have been called at least twice (" + testTask.WasRun() + ")");
				t.Cancel();

				// Ensure multiple tasks are run
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.Schedule(testTask, d, TimeSpan.FromMilliseconds(100)); // at least 9 times
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(200);
				t.Schedule(testTask, d, TimeSpan.FromMilliseconds(100)); // at least 7 times
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(300);
				t.Schedule(testTask, d, TimeSpan.FromMilliseconds(200)); // at least 4 times
				testTask = new TimerTestTask(data);
				testTask.IncrementCount(true);
				d = DateTime.Now + TimeSpan.FromMilliseconds(400);
				t.Schedule(testTask, d, TimeSpan.FromMilliseconds(200)); // at least 4 times
				Sleep(3000);
				Assert.IsTrue(data.timerCounter >= 24,
					"Multiple tasks should have incremented counter 24 times not " + data.timerCounter);
				t.Cancel();
			}
			finally 
			{
				if(t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleAtFixedRateTimerTaskDelayPeriod()
		{
			TimerEx t = null;
			try 
			{
				// Ensure a TimerEx throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				t.Cancel();
				bool exception = false;
				try
				{
					t.ScheduleAtFixedRate(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
				}
				catch (InvalidOperationException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"scheduleAtFixedRate after Timer.Cancel() should throw exception");

				// Ensure a TimerEx throws an ArgumentOutOfRangeException if delay is
				// negative
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.ScheduleAtFixedRate(testTask, TimeSpan.FromMilliseconds(-100), TimeSpan.FromMilliseconds(100));
				}
				catch (ArgumentOutOfRangeException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"scheduleAtFixedRate with negative delay should throw IllegalArgumentException");
				t.Cancel();

				// Ensure a TimerEx throws an ArgumentOutOfRangeException if period is
				// negative
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.ScheduleAtFixedRate(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(-100));
				} 
				catch (ArgumentOutOfRangeException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception, 
						"scheduleAtFixedRate with negative period should throw IllegalArgumentException");
				t.Cancel();

				// Ensure a task is run at least twice
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				t.ScheduleAtFixedRate(testTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
				Sleep(400);
				Assert.IsTrue(testTask.WasRun() >= 2, 
					"TimerTask.run() method should have been called at least twice (" + testTask.WasRun() + ")");
				t.Cancel();

				// Ensure multiple tasks are run
				t = new TimerEx();
				SlowThenFastTask slowThenFastTask = new SlowThenFastTask();

				// at least 9 times even when asleep
				t.ScheduleAtFixedRate(slowThenFastTask, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
				Sleep(1000);
				long lastDelta = (long) slowThenFastTask.LastDelta.TotalMilliseconds;
				Assert.IsTrue(lastDelta < 300,
					"Fixed Rate Schedule should catch up, but is off by " + lastDelta + " ms");
				t.Cancel();
			}
			finally 
			{
				if(t != null)
				{
					t.Cancel();
				}
			}
		}

		[Test]
		public void TestScheduleAtFixedRateTimerTaskDateTimePeriod() 
		{
			TimerEx t = null;
			try 
			{
				// Ensure a TimerEx throws an InvalidOperationException after cancelled
				t = new TimerEx();
				TimerTestTask testTask = new TimerTestTask(data);
				t.Cancel();
				bool exception = false;
				DateTime d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				try 
				{
					t.ScheduleAtFixedRate(testTask, d, TimeSpan.FromMilliseconds(100));
				}
				catch (InvalidOperationException) 
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"scheduleAtFixedRate after Timer.Cancel() should throw exception");

				// Ensure a TimerEx throws an IllegalArgumentException if period is
				// negative
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				exception = false;
				try 
				{
					t.ScheduleAtFixedRate(testTask, d, TimeSpan.FromMilliseconds(-100));
				} 
				catch (ArgumentOutOfRangeException)
				{
					exception = true;
				}
				Assert.IsTrue(exception,
						"scheduleAtFixedRate with negative period should throw ArgumentOutOfRangeException");
				t.Cancel();

				// Ensure a task is run at least twice
				t = new TimerEx();
				testTask = new TimerTestTask(data);
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);
				t.ScheduleAtFixedRate(testTask, d, TimeSpan.FromMilliseconds(100));
				Sleep(400);
				Assert.IsTrue(testTask.WasRun() >= 2,
					"TimerTask.run() method should have been called at least twice (" + testTask.WasRun() + ")");
				t.Cancel();

				// Ensure multiple tasks are run
				t = new TimerEx();
				SlowThenFastTask slowThenFastTask = new SlowThenFastTask();
				d = DateTime.Now + TimeSpan.FromMilliseconds(100);

				// at least 9 times even when asleep
				t.ScheduleAtFixedRate(slowThenFastTask, d, TimeSpan.FromMilliseconds(100));
				Sleep(1000);
				long lastDelta = (long) slowThenFastTask.LastDelta.TotalMilliseconds;
				Assert.IsTrue(lastDelta < 300, 
					"Fixed Rate Schedule should catch up, but is off by " + lastDelta + " ms");
				t.Cancel();
			}
			finally 
			{
				if(t != null)
				{
					t.Cancel();
				}
			}
		}
	}
}

