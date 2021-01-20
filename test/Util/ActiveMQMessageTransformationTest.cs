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

using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Util;
using Apache.NMS.ActiveMQ.Commands;

using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class ActiveMQMessageTransformationTest
    {
        MessageTransformation transformer;

        [SetUp]
        public void SetUp()
        {
            this.transformer = new ActiveMQMessageTransformation(null);
        }

        [Test]
        public void TestMessageTransformation()
        {
            ActiveMQMessage message = new ActiveMQMessage();

            Assert.AreSame(message, transformer.TransformMessage<ActiveMQMessage>(message));
        }

        [Test]
        public void TestTextMessageTransformation()
        {
            ActiveMQTextMessage message = new ActiveMQTextMessage();

            Assert.AreSame(message, transformer.TransformMessage<ActiveMQMessage>(message));
        }

        [Test]
        public void TestBytesMessageTransformation()
        {
            ActiveMQBytesMessage message = new ActiveMQBytesMessage();

            Assert.AreSame(message, transformer.TransformMessage<ActiveMQMessage>(message));
        }

        [Test]
        public void TestStreamMessageTransformation()
        {
            ActiveMQStreamMessage message = new ActiveMQStreamMessage();

            Assert.AreSame(message, transformer.TransformMessage<ActiveMQMessage>(message));
        }

        [Test]
        public void TestObjectMessageTransformation()
        {
            ActiveMQObjectMessage message = new ActiveMQObjectMessage();

            Assert.AreSame(message, transformer.TransformMessage<ActiveMQMessage>(message));
        }

        [Test]
        public void TestMapMessageTransformation()
        {
            ActiveMQMapMessage message = new ActiveMQMapMessage();

            Assert.AreSame(message, transformer.TransformMessage<ActiveMQMessage>(message));
        }
    }
}

