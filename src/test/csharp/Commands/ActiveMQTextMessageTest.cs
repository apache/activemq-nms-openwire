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

using Apache.NMS.ActiveMQ.Commands;
using NUnit.Framework;
using System;
using System.Text;

namespace Apache.NMS.ActiveMQ.Test.Commands
{
    [TestFixture]
    public class ActiveMQTextMessageTest
    {
        [Test]
        public void TestCommand()
        {
            ActiveMQTextMessage message = new ActiveMQTextMessage();

            Assert.IsNull(message.Text);            

            // Test with ASCII Data.
            message.Text = "Hello World";
            Assert.IsNotNull(message.Text);
            Assert.AreEqual("Hello World", message.Text);

            String unicodeString =
                "This unicode string contains two characters " +
                "with codes outside an 8-bit code range, " +
                "Pi (\u03a0) and Sigma (\u03a3).";

            message.Text = unicodeString;
            Assert.IsNotNull(message.Text);
            Assert.AreEqual(unicodeString, message.Text);
        }
    }
}
