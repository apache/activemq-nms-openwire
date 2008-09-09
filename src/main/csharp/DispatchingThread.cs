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
using Apache.NMS;


namespace Apache.NMS.ActiveMQ
{
	internal class DispatchingThread
	{
		public delegate void DispatchFunction();
		public delegate void ExceptionHandler(Exception exception);

		private readonly AutoResetEvent m_event = new AutoResetEvent(false);
		private readonly ManualResetEvent m_stopEvent = new ManualResetEvent(false);
		private Thread m_thread = null;
		private readonly DispatchFunction m_dispatchFunc;
		private event ExceptionHandler m_exceptionListener;

		public DispatchingThread(DispatchFunction dispatchFunc)
		{
			m_dispatchFunc = dispatchFunc;
		}

		public bool IsStarted
		{
			get
			{
				lock(this)
				{
					return (null != m_thread);
				}
			}
		}

			   // TODO can't use EventWaitHandle on MONO 1.0
		public AutoResetEvent EventHandle
		{
			get { return m_event; }
		}

		internal event ExceptionHandler ExceptionListener
		{
			add
			{
				m_exceptionListener += value;
			}
			remove
			{
				m_exceptionListener -= value;
			}
		}

		internal void Start()
		{
			lock (this)
			{
				if (m_thread == null)
				{
					m_stopEvent.Reset();
					m_thread = new Thread(new ThreadStart(MyThreadFunc));
					m_thread.IsBackground = true;
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
			Thread localThread;
			lock (this)
			{
				localThread = m_thread;
				m_thread = null;
				m_stopEvent.Set();
			}
			if(localThread!=null)
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

			//
			// Put m_stopEvent first so it is preferred if both are signaled
			//
			WaitHandle[] signals = new WaitHandle[] {
				m_stopEvent,
				m_event
			};
			const int kStopEventOffset = 0;

			try
			{
				while (true) // loop forever (well, at least until we've been asked to stop)
				{
					try
					{
						m_dispatchFunc();
					}
					catch(ThreadAbortException)
					{
						// Throw for handling down below
						throw;
					}
					catch(Exception ex)
					{
						if(m_exceptionListener != null)
						{
							m_exceptionListener(ex);
						}
					}

					int sigOffset = WaitHandle.WaitAny(signals);
					if(kStopEventOffset == sigOffset)
					{
						break;
					}
					// otherwise, continue the loop
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
