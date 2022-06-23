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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.Transport;
using Apache.NMS.ActiveMQ.Transport.Mock;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class BasicMockTransportTest
	{
		private Uri mockUri = new Uri("mock://mock");

		[Test]
		public void CreateMockTransportTest()
		{
			MockTransport transport = new MockTransport(mockUri);

			Assert.IsNotNull(transport);
			Assert.IsFalse(transport.IsStarted);
			Assert.IsFalse(transport.IsDisposed);
		}

		[Test]
		public void StartInitializedTransportTest()
		{
			MockTransport transport = new MockTransport(mockUri);

			transport.CommandAsync = new CommandHandlerAsync(OnCommand);
			transport.Exception = new ExceptionHandler(OnException);

			transport.Start();
		}

		[Test]
		[ExpectedException(typeof(InvalidOperationException))]
		public void StartUnitializedTransportTest()
		{
			MockTransport transport = new MockTransport(mockUri);
			transport.Start();
		}

		public void OnException(ITransport transport, Exception exception)
		{
			Tracer.DebugFormat("MockTransportTest::onException - " + exception);
		}

		public async Task OnCommand(ITransport transport, Command command)
		{
			Tracer.DebugFormat("MockTransportTest::OnCommand - " + command);
			await Task.CompletedTask;
		}
	}

	[TestFixture]
	public class MockTransportTest
	{
		private Uri mockUri = new Uri("mock://mock");
		private List<Command> sent;
		private List<Command> received;
		private List<Exception> exceptions;

		private MockTransport transport;

		public void OnException(ITransport transport, Exception exception)
		{
			Tracer.DebugFormat("MockTransportTest::onException - " + exception);
			exceptions.Add(exception);
		}

		public async Task OnCommand(ITransport transport, Command command)
		{
			Tracer.DebugFormat("MockTransportTest::OnCommand - " + command);
			received.Add(command);
			await Task.CompletedTask;
		}

		public async Task OnOutgoingCommand(ITransport transport, Command command)
		{
			Tracer.DebugFormat("MockTransportTest::OnOutgoingCommand - " + command);
			sent.Add(command);
			await Task.CompletedTask;
		}

		[SetUp]
		public void Init()
		{
			transport = new MockTransport(mockUri);
			sent = new List<Command>();
			received = new List<Command>();
			exceptions = new List<Exception>();

			transport.CommandAsync = new CommandHandlerAsync(OnCommand);
			transport.Exception = new ExceptionHandler(OnException);
			transport.OutgoingCommand = new CommandHandlerAsync(OnOutgoingCommand);
		}

		[Test]
		public void OneWaySendMessageTest()
		{
			transport.Start();
			ActiveMQTextMessage message = new ActiveMQTextMessage();
			message.Text = "Hello, World";
			transport.Oneway(message);
			Assert.IsTrue(transport.NumSentMessages == 1);
			Assert.IsTrue(sent.Count == 1);
			Assert.AreEqual(message.Text, (sent[0] as ActiveMQTextMessage).Text);
		}

		[Test]
		public async Task RequestMessageTest()
		{
			await transport.StartAsync();
			ActiveMQTextMessage message = new ActiveMQTextMessage();
			message.Text = "Hello, World";
			await transport.RequestAsync(message);
			Assert.IsTrue(transport.NumSentMessages == 1);
			Assert.IsTrue(sent.Count == 1);
			Assert.AreEqual(message.Text, (sent[0] as ActiveMQTextMessage).Text);
		}

		[Test, ExpectedException(typeof(IOException))]
		public void OneWayFailOnSendMessageTest()
		{
			transport.FailOnSendMessage = true;
			transport.Start();
			ActiveMQTextMessage message = new ActiveMQTextMessage();
			transport.Oneway(message);
		}

		[Test, ExpectedException(typeof(IOException))]
		public async System.Threading.Tasks.Task RequestFailOnSendMessageTest()
		{
			transport.FailOnSendMessage = true;
			await transport.StartAsync();
			ActiveMQTextMessage message = new ActiveMQTextMessage();
			Assert.IsNotNull(await transport.RequestAsync(message));
		}

		[Test, ExpectedException(typeof(IOException))]
		public void AsyncRequestFailOnSendMessageTest()
		{
			transport.FailOnSendMessage = true;
			transport.Start();
			ActiveMQTextMessage message = new ActiveMQTextMessage();
			Assert.IsNotNull(transport.AsyncRequest(message));
		}

		[Test, ExpectedException(typeof(IOException))]
		public void OnewayFailOnSendTwoMessagesTest()
		{
			transport.FailOnSendMessage = true;
			transport.NumSentMessagesBeforeFail = 2;
			transport.Start();
			ActiveMQTextMessage message = new ActiveMQTextMessage();
			transport.Oneway(message);
			transport.Oneway(message);
			transport.Oneway(message);
		}

		[Test, ExpectedException(typeof(IOException))]
		public async System.Threading.Tasks.Task RequestFailOnSendTwoMessagesTest()
		{
			transport.FailOnSendMessage = true;
			transport.NumSentMessagesBeforeFail = 2;
			await transport.StartAsync();
			ActiveMQTextMessage message = new ActiveMQTextMessage();
			await transport.RequestAsync(message);
			await transport.RequestAsync(message);
			await transport.RequestAsync(message);
		}

		[Test, ExpectedException(typeof(IOException))]
		public void AsyncRequestFailOnSendTwoMessagesTest()
		{
			transport.FailOnSendMessage = true;
			transport.NumSentMessagesBeforeFail = 2;
			transport.Start();
			ActiveMQTextMessage message = new ActiveMQTextMessage();
			transport.AsyncRequest(message);
			transport.AsyncRequest(message);
			transport.AsyncRequest(message);
		}

		[Test]
		public void InjectCommandTest()
		{
			ActiveMQMessage message = new ActiveMQMessage();

			transport.Start();
			transport.InjectCommand(message);

			Thread.Sleep(1000);

			Assert.IsTrue(this.received.Count > 0);
			Assert.IsTrue(transport.NumReceivedMessages == 1);
		}

		[Test]
		public void FailOnReceiveMessageTest()
		{
			ActiveMQMessage message = new ActiveMQMessage();

			transport.FailOnReceiveMessage = true;
			transport.Start();
			transport.InjectCommand(message);

			Thread.Sleep(1000);

			Assert.IsTrue(this.exceptions.Count > 0);
			Assert.IsTrue(transport.NumReceivedMessages == 1);
		}
	}
}
