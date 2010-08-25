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
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class FifoMessageDispatchChannelTest
    {
        [Test]
        public void TestCtor()
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
            Assert.IsTrue( channel.Running == false );
            Assert.IsTrue( channel.Empty == true );
            Assert.IsTrue( channel.Count == 0 );
            Assert.IsTrue( channel.Closed == false );
        }
        
        [Test]
        public void TestStart() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
            channel.Start();
            Assert.IsTrue( channel.Running == true );
        }
        
        [Test]
        public void TestStop() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
            channel.Start();
            Assert.IsTrue( channel.Running == true );
            channel.Stop();
            Assert.IsTrue( channel.Running == false );
        }
        
        [Test]
        public void TestClose() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
            channel.Start();
            Assert.IsTrue( channel.Running == true );
            Assert.IsTrue( channel.Closed == false );
            channel.Close();
            Assert.IsTrue( channel.Running == false );
            Assert.IsTrue( channel.Closed == true );
            channel.Start();
            Assert.IsTrue( channel.Running == false );
            Assert.IsTrue( channel.Closed == true );
        }
        
        [Test]
        public void TestEnqueue() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
            MessageDispatch dispatch1 = new MessageDispatch();
            MessageDispatch dispatch2 = new MessageDispatch();
        
            Assert.IsTrue( channel.Empty == true );
            Assert.IsTrue( channel.Count == 0 );
        
            channel.Enqueue( dispatch1 );
        
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 1 );
        
            channel.Enqueue( dispatch2 );
        
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 2 );
        }
        
        [Test]
        public void TestEnqueueFront() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
            MessageDispatch dispatch1 = new MessageDispatch();
            MessageDispatch dispatch2 = new MessageDispatch();
        
            channel.Start();
        
            Assert.IsTrue( channel.Empty == true );
            Assert.IsTrue( channel.Count == 0 );
        
            channel.EnqueueFirst( dispatch1 );
        
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 1 );
        
            channel.EnqueueFirst( dispatch2 );
        
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 2 );
        
            Assert.IsTrue( channel.DequeueNoWait() == dispatch2 );
            Assert.IsTrue( channel.DequeueNoWait() == dispatch1 );
        }
        
        [Test]
        public void TestPeek() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
            MessageDispatch dispatch1 = new MessageDispatch();
            MessageDispatch dispatch2 = new MessageDispatch();
        
            Assert.IsTrue( channel.Empty == true );
            Assert.IsTrue( channel.Count == 0 );
        
            channel.EnqueueFirst( dispatch1 );
        
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 1 );
        
            channel.EnqueueFirst( dispatch2 );
        
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 2 );
        
            Assert.IsTrue( channel.Peek() == null );
        
            channel.Start();
        
            Assert.IsTrue( channel.Peek() == dispatch2 );
            Assert.IsTrue( channel.DequeueNoWait() == dispatch2 );
            Assert.IsTrue( channel.Peek() == dispatch1 );
            Assert.IsTrue( channel.DequeueNoWait() == dispatch1 );
        }
        
        [Test]
        public void TestDequeueNoWait() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
        
            MessageDispatch dispatch1 = new MessageDispatch();
            MessageDispatch dispatch2 = new MessageDispatch();
            MessageDispatch dispatch3 = new MessageDispatch();
        
            Assert.IsTrue( channel.Running == false );
            Assert.IsTrue( channel.DequeueNoWait() == null );
        
            channel.Enqueue( dispatch1 );
            channel.Enqueue( dispatch2 );
            channel.Enqueue( dispatch3 );
        
            Assert.IsTrue( channel.DequeueNoWait() == null );
            channel.Start();
            Assert.IsTrue( channel.Running == true );
        
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 3 );
            Assert.IsTrue( channel.DequeueNoWait() == dispatch1 );
            Assert.IsTrue( channel.DequeueNoWait() == dispatch2 );
            Assert.IsTrue( channel.DequeueNoWait() == dispatch3 );
        
            Assert.IsTrue( channel.Count == 0 );
            Assert.IsTrue( channel.Empty == true );
        }
        
        [Test]
        public void TestDequeue() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
        
            MessageDispatch dispatch1 = new MessageDispatch();
            MessageDispatch dispatch2 = new MessageDispatch();
            MessageDispatch dispatch3 = new MessageDispatch();
        
            channel.Start();
            Assert.IsTrue( channel.Running == true );
        
            DateTime timeStarted = DateTime.Now;
        
            Assert.IsTrue( channel.Dequeue(TimeSpan.FromMilliseconds(1000)) == null );

            DateTime timeFinished = DateTime.Now;

            TimeSpan elapsed = timeFinished - timeStarted;
            Assert.IsTrue( elapsed.TotalMilliseconds >= 999 );
        
            channel.Enqueue( dispatch1 );
            channel.Enqueue( dispatch2 );
            channel.Enqueue( dispatch3 );
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 3 );
            Assert.IsTrue( channel.Dequeue( TimeSpan.FromMilliseconds(Timeout.Infinite) ) == dispatch1 );
            Assert.IsTrue( channel.Dequeue( TimeSpan.Zero ) == dispatch2 );
            Assert.IsTrue( channel.Dequeue( TimeSpan.FromMilliseconds(1000) ) == dispatch3 );
        
            Assert.IsTrue( channel.Count == 0 );
            Assert.IsTrue( channel.Empty == true );
        }
        
        [Test]
        public void TestRemoveAll() 
        {
            FifoMessageDispatchChannel channel = new FifoMessageDispatchChannel();
        
            MessageDispatch dispatch1 = new MessageDispatch();
            MessageDispatch dispatch2 = new MessageDispatch();
            MessageDispatch dispatch3 = new MessageDispatch();
        
            channel.Enqueue( dispatch1 );
            channel.Enqueue( dispatch2 );
            channel.Enqueue( dispatch3 );
        
            channel.Start();
            Assert.IsTrue( channel.Running == true );
            Assert.IsTrue( channel.Empty == false );
            Assert.IsTrue( channel.Count == 3 );
            Assert.IsTrue( channel.RemoveAll().Length == 3 );
            Assert.IsTrue( channel.Count == 0 );
            Assert.IsTrue( channel.Empty == true );
        }
    }
}
