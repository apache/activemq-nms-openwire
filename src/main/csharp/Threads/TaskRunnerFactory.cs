/**
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
using System.Threading;

namespace Apache.NMS.ActiveMQ.Threads
{
	/// <summary>
	/// Manages the thread pool for long running tasks. Long running tasks are not
	/// always active but when they are active, they may need a few iterations of
	/// processing for them to become idle. The manager ensures that each task is
	/// processes but that no one task overtakes the system. This is kina like
	/// cooperative multitasking.
 	/// </summary>
	public class TaskRunnerFactory
	{

		private int maxIterationsPerRun;
		private String name;
		private ThreadPriority priority;
		private bool daemon;

		public TaskRunnerFactory()
		{
			initTaskRunnerFactory("ActiveMQ Task", ThreadPriority.Normal, true, 1000, false);
		}

		public TaskRunnerFactory(String name, ThreadPriority priority, bool daemon, int maxIterationsPerRun)
		{
			initTaskRunnerFactory(name, priority, daemon, maxIterationsPerRun, false);
		}

		public TaskRunnerFactory(String name, ThreadPriority priority, bool daemon, int maxIterationsPerRun, bool dedicatedTaskRunner)
		{
			initTaskRunnerFactory(name, priority, daemon, maxIterationsPerRun, dedicatedTaskRunner);
		}

		public void initTaskRunnerFactory(String name, ThreadPriority priority, bool daemon, int maxIterationsPerRun, bool dedicatedTaskRunner)
		{

			this.name = name;
			this.priority = priority;
			this.daemon = daemon;
			this.maxIterationsPerRun = maxIterationsPerRun;

			// If your OS/JVM combination has a good thread model, you may want to avoid
			// using a thread pool to run tasks and use a DedicatedTaskRunner instead.
		}

		public void shutdown()
		{
		}

		public TaskRunner CreateTaskRunner(Task task, String name)
		{
			return new PooledTaskRunner(task, maxIterationsPerRun);
		}
	}
}
