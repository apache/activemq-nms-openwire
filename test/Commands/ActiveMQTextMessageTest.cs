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
using System.IO;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;
using NUnit.Framework;

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

        [Test]
        public void TestShallowCopy()
        {
            ActiveMQTextMessage msg = new ActiveMQTextMessage();
            string testString = "str";
            msg.Text = testString;
            Message copy = msg.Clone() as Message;
            Assert.IsTrue(msg.Text == ((ActiveMQTextMessage) copy).Text);
        }
    
        [Test]
        public void TestSetText() 
        {
            ActiveMQTextMessage msg = new ActiveMQTextMessage();
            string str = "testText";
            msg.Text = str;
            Assert.AreEqual(msg.Text, str);
        }
    
        [Test]
        public void TestGetBytes() 
        {
            ActiveMQTextMessage msg = new ActiveMQTextMessage();
            String str = "testText";
            msg.Text = str;
            msg.BeforeMarshall(null);
            
            byte[] bytes = msg.Content;
            msg = new ActiveMQTextMessage();
            msg.Content = bytes;
            
            Assert.AreEqual(msg.Text, str);
        }
    
        [Test]
        public void TestClearBody()
        {
            ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
            textMessage.Text = "string";
            textMessage.ClearBody();
            Assert.IsFalse(textMessage.ReadOnlyBody);
            Assert.IsNull(textMessage.Text);
            try
            {
                textMessage.Text = "String";
                Assert.IsTrue(textMessage.Text.Length > 0);
            }
            catch(MessageNotWriteableException)
            {
                Assert.Fail("should be writeable");
            }
            catch(MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            }
        }
    
        [Test]
        public void TestReadOnlyBody() 
        {
            ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
            textMessage.Text = "test";
            textMessage.ReadOnlyBody = true;
            try 
            {
                Assert.IsTrue(textMessage.Text.Length > 0);
            } 
            catch(MessageNotReadableException) 
            {
                Assert.Fail("should be readable");
            }
            try 
            {
                textMessage.Text = "test";
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
        }
    
        [Test]
        public void TtestWriteOnlyBody() 
        { 
            // should always be readable
            ActiveMQTextMessage textMessage = new ActiveMQTextMessage();
            textMessage.ReadOnlyBody = false;
            try 
            {
                textMessage.Text = "test";
                Assert.IsTrue(textMessage.Text.Length > 0);
            } 
            catch(MessageNotReadableException) 
            {
                Assert.Fail("should be readable");
            }
            textMessage.ReadOnlyBody = true;
            try 
            {
                Assert.IsTrue(textMessage.Text.Length > 0);
                textMessage.Text = "test";
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotReadableException)
            {
                Assert.Fail("should be readable");
            } 
            catch(MessageNotWriteableException) 
            {
            }
        }
        
        [Test]
        public void TestShortText() 
        {
            string shortText = "Content";
            ActiveMQTextMessage shortMessage = new ActiveMQTextMessage();
            SetContent(shortMessage, shortText);
            Assert.IsTrue(shortMessage.ToString().Contains("Text = " + shortText));
            Assert.IsTrue(shortMessage.Text == shortText);
            
            string longText = "Very very very very veeeeeeery loooooooooooooooooooooooooooooooooong text";
            string longExpectedText = "Very very very very veeeeeeery looooooooooooo...ooooong text";
            ActiveMQTextMessage longMessage = new ActiveMQTextMessage();
            SetContent(longMessage, longText);
            Assert.IsTrue(longMessage.ToString().Contains("Text = " + longExpectedText));
            Assert.IsTrue(longMessage.Text == longText);         
        }
        
        [Test]
        public void TestNullText() 
        {
            ActiveMQTextMessage nullMessage = new ActiveMQTextMessage();
            SetContent(nullMessage, null);
            Assert.IsNull(nullMessage.Text);
            Assert.IsTrue(nullMessage.ToString().Contains("Text = null"));
        }
        
        protected void SetContent(Message message, String text)
        {
            MemoryStream mstream = new MemoryStream();
            EndianBinaryWriter dataOut = new EndianBinaryWriter(mstream);
            dataOut.WriteString32(text);
            dataOut.Close();
            message.Content = mstream.ToArray();
        }
    }
}
