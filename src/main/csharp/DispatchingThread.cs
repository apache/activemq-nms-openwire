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


namespace Apache.NMS.ActiveMQ
{
	internal class DispatchingThread
	{
		public delegate void DispatchFunction();
		public delegate void ExceptionHandler(Exception exception);

		private readonly AutoResetEvent m_event = new AutoResetEvent(false);
		private object initobjectLock = new object();
		private bool m_bStopFlag = false;
		private Thread m_thread = null;
		private readonly DispatchFunction m_dispatchFunc;
		private event ExceptionHandler m_exceptionListener;

		public DispatchingThread(DispatchFunction dispatchFunc)
		{
			m_dispatchFunc = dispatchFunc;
		}

		public AutoResetEvent EventHandle
		{
			get { return m_event; }
		}

		internal event ExceptionHandler ExceptionListener
		{
			add { m_exceptionListener += value; }
			remove { m_exceptionListener -= value; }
		}

		internal void Start()
		{
			lock(initobjectLock)
			{
				if(m_thread == null)
				{
					m_bStopFlag = false;
					m_thread = new Thread(new ThreadStart(MyThreadFunc));
					m_thread.IsBackground = true;
					m_event.Set();
					Tracer.Info("Starting dispatcher thread for session");
					m_thread.Start();
				}
			}
		}

		internal void Stop()
		{
			Stop(System.Threading.Timeout.Infinite);
		}

		internal void Stop(int timeoutMilliseconds)
		{
			Tracer.Info("Stopping dispatcher thread for session");
			Thread localThread = null;
			lock(initobjectLock)
			{
				localThread = m_thread;
				m_thread = null;
				if(!m_bStopFlag)
				{
					m_bStopFlag = true;
					m_event.Set();
				}
			}

			if(localThread != null)
			{
				if(!localThread.Join(timeoutMilliseconds))
				{
					Tracer.Info("!! Timeout waiting for Dispatcher localThread to stop");
					localThread.Abort();
				}
			}

			Tracer.Info("Dispatcher thread joined");
		}

		private void MyThreadFunc()
		{
			Tracer.Info("Dispatcher thread started");
			try
			{
				while(true) // loop forever (well, at least until we've been asked to stop)
				{
					lock(initobjectLock)
					{
						if(m_bStopFlag)
						{
							break;
						}
					}

					try
					{
						m_dispatchFunc();
					}
					catch(Exception ex)
					{
						if(m_exceptionListener != null)
						{
							m_exceptionListener(ex);
						}
					}
					m_event.WaitOne();
				}
				Tracer.Info("Dispatcher thread stopped");
			}
			catch(ThreadAbortException)
			{
				Tracer.Info("Dispatcher thread aborted");
			}
		}
	}
}
