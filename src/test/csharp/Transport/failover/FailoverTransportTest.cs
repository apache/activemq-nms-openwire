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
using System.Collections;
using System.Collections.Generic;
using Apache.NMS;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Mock;
using Apache.NMS.ActiveMQ.Transport.Failover;
using Apache.NMS.ActiveMQ.Commands;
using NUnit.Framework;
using NUnit.Framework.Extensions;

namespace Apache.NMS.ActiveMQ.Test
{
    internal class ConsoleTracer : ITrace
    {
        public bool IsDebugEnabled { get { return true; } }
        public bool IsInfoEnabled { get { return true; } }
        public bool IsWarnEnabled { get { return true; } }
        public bool IsErrorEnabled { get { return true; } }
        public bool IsFatalEnabled { get { return true; } }
        public void Debug(string message) { Console.WriteLine("DEBUG:" + message); }
        public void Info(string message) { Console.WriteLine("INFO:" + message); }
        public void Warn(string message) { Console.WriteLine("WARN:" + message); }
        public void Error(string message) { Console.WriteLine("ERROR:" + message); }
        public void Fatal(string message) { Console.WriteLine("FATAL:" + message); }
    }
    
	[TestFixture]
	public class FailoverTransportTest
	{
        private List<Command> received = new List<Command>();
        private List<Exception> exceptions = new List<Exception>();
        
        public void OnException(ITransport transport, Exception exception)
        {
            exceptions.Add( exception );
        }
        
        public void OnCommand(ITransport transport, Command command)
        {
            received.Add( command );
        }
        
        [Test]
        public void FailoverTransportCreateTest()
        {
            Uri uri = new Uri("failover:(mock://localhost:61616)?randomize=false");
            Tracer.Trace = new ConsoleTracer();

            FailoverTransportFactory factory = new FailoverTransportFactory();

            ITransport transport = factory.CreateTransport(uri);
            Assert.IsNotNull(transport);

            transport.Command = new CommandHandler(OnCommand);
            transport.Exception = new ExceptionHandler(OnException);

            FailoverTransport failover = (FailoverTransport) transport.Narrow(typeof(FailoverTransport));
            Assert.IsNotNull(failover);
            Assert.IsFalse(failover.Randomize);
          
            transport.Start();

            Thread.Sleep(1000);
            Assert.IsTrue(failover.IsConnected);
            
            transport.Stop();

        }
	}
}
