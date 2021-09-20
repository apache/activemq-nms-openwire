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
using System.Threading.Tasks;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.Transport
{
	/// <summary>
	/// Used to implement a filter on the transport layer.
	/// </summary>
	public class TransportFilter : ITransport
	{
		protected readonly ITransport next;
		protected CommandHandlerAsync commandHandlerAsync;
		protected ExceptionHandler exceptionHandler;
		protected InterruptedHandler interruptedHandler;
		protected ResumedHandler resumedHandler;
		private bool disposed = false;

		public TransportFilter(ITransport next)
		{
			this.next = next;
			this.next.CommandAsync = new CommandHandlerAsync(OnCommand);
			this.next.Exception = new ExceptionHandler(OnException);
            this.next.Interrupted = new InterruptedHandler(OnInterrupted);
            this.next.Resumed = new ResumedHandler(OnResumed);
		}

		~TransportFilter()
		{
			Dispose(false);
		}

		protected virtual Task OnCommand(ITransport sender, Command command)
		{
			return this.commandHandlerAsync(sender, command);
		}

		protected virtual void OnException(ITransport sender, Exception command)
		{
			this.exceptionHandler(sender, command);
		}

        protected virtual void OnInterrupted(ITransport sender)
        {
            if(this.interruptedHandler != null)
            {
                this.interruptedHandler(sender);
            }
        }

        protected virtual void OnResumed(ITransport sender)
        {
            if(this.resumedHandler != null)
            {
                this.resumedHandler(sender);
            }
        }
        
		/// <summary>
		/// Method Oneway
		/// </summary>
		/// <param name="command">A  Command</param>
		public virtual void Oneway(Command command)
		{
			this.next.Oneway(command);
		}

		/// <summary>
		/// Method AsyncRequest
		/// </summary>
		/// <returns>A FutureResponse</returns>
		/// <param name="command">A  Command</param>
		public virtual FutureResponse AsyncRequest(Command command)
		{
			return this.next.AsyncRequest(command);
		}

		/// <summary>
		/// Method Request
		/// </summary>
		/// <returns>A Response</returns>
		/// <param name="command">A  Command</param>
		public virtual Task<Response> RequestAsync(Command command)
		{			
			return RequestAsync(command, TimeSpan.FromMilliseconds(System.Threading.Timeout.Infinite));
		}

		/// <summary>
		/// Method Request with time out for Response.
		/// </summary>
		/// <returns>A Response</returns>
		/// <param name="command">A  Command</param>
		/// <param name="timeout">Timeout in milliseconds</param>
		public virtual Task<Response> RequestAsync(Command command, TimeSpan timeout)
		{
			return this.next.RequestAsync(command, timeout);
		}

		/// <summary>
		/// Method Start
		/// </summary>
		public virtual void Start()
		{
			if(commandHandlerAsync == null)
			{
				throw new InvalidOperationException("command cannot be null when Start is called.");
			}

			if(exceptionHandler == null)
			{
				throw new InvalidOperationException("exception cannot be null when Start is called.");
			}

			this.next.Start();
		}

		public Task StartAsync()
		{
			Start();
			return Task.CompletedTask;
		}

		/// <summary>
		/// Property IsStarted
		/// </summary>
		public bool IsStarted
		{
			get { return this.next.IsStarted; }
		}

		/// <summary>
		/// Method Dispose
		/// </summary>
		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing)
		{
			if(disposing && !disposed)
			{
                Tracer.Debug("TransportFilter disposing of next Transport: " +
                             this.next.GetType().Name);
				this.next.Dispose();
			}
			disposed = true;
		}

		public bool IsDisposed
		{
			get
			{
				return disposed;
			}
		}

		public CommandHandlerAsync CommandAsync
		{
			get { return commandHandlerAsync; }
			set { this.commandHandlerAsync = value; }
		}

		public ExceptionHandler Exception
		{
			get { return exceptionHandler; }
			set { this.exceptionHandler = value; }
		}

		public InterruptedHandler Interrupted
		{
			get { return interruptedHandler; }
			set { this.interruptedHandler = value; }
		}
		
		public ResumedHandler Resumed
		{
			get { return resumedHandler; }
			set { this.resumedHandler = value; }
		}
		
		public virtual void Stop()
		{
            this.next.Stop();
		}

		public Task StopAsync()
		{
			Stop();
			return Task.CompletedTask;
		}

		public Object Narrow(Type type)
        {
            if( this.GetType().Equals( type ) ) {
                return this;
            } else if( this.next != null ) {
                return this.next.Narrow( type );
            }
        
            return null;
        }

		/// <summary>
		/// Timeout in milliseconds to wait for sending synchronous messages or commands.
		/// Set to -1 for infinite timeout.
		/// </summary>
		public int Timeout
		{
			get { return next.Timeout; }
			set { next.Timeout = value; }
		}

		/// <summary>
		/// Timeout in milliseconds to wait for sending asynchronous messages or commands.
		/// Set to -1 for infinite timeout.
		/// </summary>
		public int AsyncTimeout
		{
			get { return next.AsyncTimeout; }
			set { next.AsyncTimeout = value; }
		}
		
		public bool IsFaultTolerant
        {
            get{ return next.IsFaultTolerant; }
        }

        public bool IsConnected
        {
            get{ return next.IsConnected; }
        }

        public Uri RemoteAddress
        {
            get{ return next.RemoteAddress; }
        }
		
	    public bool IsReconnectSupported
		{
			get{ return next.IsReconnectSupported; }
		}
	    
	    public bool IsUpdateURIsSupported
		{
			get{ return next.IsUpdateURIsSupported; }
		}
		
		public void UpdateURIs(bool rebalance, Uri[] updatedURIs)
		{
			next.UpdateURIs(rebalance, updatedURIs);
		}

        public IWireFormat WireFormat
        {
            get { return next.WireFormat; }
        }
    }
}

