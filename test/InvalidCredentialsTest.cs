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
using Apache.NMS.Test;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
	[TestFixture]
	public class InvalidCredentialsTest : NMSTestSupport
	{
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		[TearDown]
		public override void TearDown()
		{
			base.TearDown();
		}

		// Maximum time to run is 20 seconds.
		[Test, Timeout(20000)]
		public void TestRestartInvalidCredentialsWithFailover()
		{
			// To run this test successfully, the broker must have secure login enabled.
			// This test will attempt to login to the server using invalid credentials.
			// It should not connect, and should not go into failover retry loop.
			// It will then attempt to login with correct credentials, and it should be 
			// succcessful on second attempt.
			Assert.IsTrue(CreateNMSFactory("InvalidCredentials-BogusUser"));
			using(IConnection connection = CreateConnection())
			{
				Assert.Throws(typeof(NMSSecurityException), () => { connection.Start(); }, "You may not have enabled credential login on the broker.  Credentials must be enabled for this test to pass.");
			}

			// Now connect with a valid user account.
			Assert.IsTrue(CreateNMSFactory("InvalidCredentials-AuthenticUser"));
			using(IConnection connection = CreateConnection())
			{
				Assert.DoesNotThrow(() => { connection.Start(); }, "You may not have set the InvalidCredentials-AuthenticUser node in the nmsprovider-test.config file.");
			}
		}
	}
}
