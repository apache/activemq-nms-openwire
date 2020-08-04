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
using System.Collections;
using System.Globalization;
using System.Text;
using Apache.NMS;
using Apache.NMS.Test;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Commands;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test.Commands
{    
    [TestFixture]
    public class ActiveMQMapMessageTest : NMSTestSupport
    {
        private string name = "testName";
        
        [Test]
        public void TestBytesConversion()
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            msg.Body.SetBool("boolean", true);
            msg.Body.SetByte("byte", (byte)1);
            msg.Body["bytes"] = new byte[1];
            msg.Body.SetChar("char", 'a');
            msg.Body.SetDouble("double", 1.5);
            msg.Body.SetFloat("float", 1.5f);
            msg.Body.SetInt("int", 1);
            msg.Body.SetLong("long", 1);
            msg.Body["object"] = "stringObj";
            msg.Body.SetShort("short", (short)1);
            msg.Body.SetString("string", "string");
    
            // Test with a 1Meg String
            StringBuilder bigSB = new StringBuilder(1024 * 1024);
            for(int i = 0; i < 1024 * 1024; i++) 
            {
                bigSB.Append((char)'a' + i % 26);
            }
            String bigString = bigSB.ToString();
    
            msg.Body.SetString("bigString", bigString);
    
            msg = (ActiveMQMapMessage)msg.Clone();
    
            Assert.AreEqual(msg.Body.GetBool("boolean"), true);
            Assert.AreEqual(msg.Body.GetByte("byte"), (byte)1);
            Assert.AreEqual((msg.Body["bytes"] as byte[]).Length, 1);
            Assert.AreEqual(msg.Body.GetChar("char"), 'a');
            Assert.AreEqual(msg.Body.GetDouble("double"), 1.5, 0);
            Assert.AreEqual(msg.Body.GetFloat("float"), 1.5f, 0);
            Assert.AreEqual(msg.Body.GetInt("int"), 1);
            Assert.AreEqual(msg.Body.GetLong("long"), 1);
            Assert.AreEqual(msg.Body["object"], "stringObj");
            Assert.AreEqual(msg.Body.GetShort("short"), (short)1);
            Assert.AreEqual(msg.Body.GetString("string"), "string");
            Assert.AreEqual(msg.Body.GetString("bigString"), bigString);
        }

        [Test]
        public void TestGetBoolean()
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            msg.Body.SetBool(name, true);
            msg.ReadOnlyBody = true;
            Assert.IsTrue(msg.Body.GetBool(name));
            msg.ClearBody();
            msg.Body.SetString(name, "True");
    
            msg = (ActiveMQMapMessage)msg.Clone();
    
            Assert.IsTrue(msg.Body.GetBool(name));
        }
    
        [Test]
        public void TestGetByte() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            msg.Body.SetByte(this.name, (byte)1);
            msg = (ActiveMQMapMessage)msg.Clone();
            Assert.IsTrue(msg.Body.GetByte(this.name) == (byte)1);
        }
    
        [Test]
        public void TestGetShort() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            try 
            {
                msg.Body.SetShort(this.name, (short)1);
                msg = (ActiveMQMapMessage)msg.Clone();
                Assert.IsTrue(msg.Body.GetShort(this.name) == (short)1);
            } 
            catch(NMSException)
            {
                Assert.IsTrue(false);
            }
        }
    
        [Test]
        public void TestGetChar() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            try 
            {
                msg.Body.SetChar(this.name, 'a');
                msg = (ActiveMQMapMessage)msg.Clone();
                Assert.IsTrue(msg.Body.GetChar(this.name) == 'a');
            }
            catch(NMSException) 
            {
                Assert.IsTrue(false);
            }
        }
    
        [Test]
        public void TestGetInt() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            try 
            {
                msg.Body.SetInt(this.name, 1);
                msg = (ActiveMQMapMessage)msg.Clone();
                Assert.IsTrue(msg.Body.GetInt(this.name) == 1);
            } 
            catch(NMSException) 
            {
                Assert.IsTrue(false);
            }
        }
    
        [Test]
        public void TestGetLong() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            try 
            {
                msg.Body.SetLong(this.name, 1);
                msg = (ActiveMQMapMessage)msg.Clone();
                Assert.IsTrue(msg.Body.GetLong(this.name) == 1);
            } 
            catch(NMSException) 
            {
                Assert.IsTrue(false);
            }
        }
    
        [Test]
        public void TestGetFloat() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            try 
            {
                msg.Body.SetFloat(this.name, 1.5f);
                msg = (ActiveMQMapMessage)msg.Clone();
                Assert.IsTrue(msg.Body.GetFloat(this.name) == 1.5f);
            } 
            catch(NMSException)
            {
                Assert.IsTrue(false);
            }
        }
    
        [Test]
        public void TestGetDouble() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            try 
            {
                msg.Body.SetDouble(this.name, 1.5);
                msg = (ActiveMQMapMessage)msg.Clone();
                Assert.IsTrue(msg.Body.GetDouble(this.name) == 1.5);
            } 
            catch(NMSException)
            {
                Assert.IsTrue(false);
            }
        }
    
        [Test]
        public void TestGetString()
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            try 
            {
                String str = "test";
                msg.Body.SetString(this.name, str);
                msg = (ActiveMQMapMessage)msg.Clone();
                Assert.AreEqual(msg.Body.GetString(this.name), str);
            } 
            catch(NMSException)
            {
                Assert.IsTrue(false);
            }
        }
    
        [Test]
        public void TestGetBytes() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            try
            {
                byte[] bytes1 = new byte[3];
                byte[] bytes2 = new byte[2];
                System.Array.Copy(bytes1, 0, bytes2, 0, 2);
                msg.Body[this.name] = bytes1;
                msg.Body[this.name + "2"] = bytes2;
                msg = (ActiveMQMapMessage)msg.Clone();
                Assert.IsTrue(System.Array.Equals(msg.Body[this.name], bytes1));
                Assert.AreEqual(((byte[]) msg.Body[this.name + "2"]).Length, bytes2.Length);
            } 
            catch(NMSException) 
            {
                Assert.IsTrue(false);
            }
        }
    
        [Test]
        public void TestGetObject() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            Boolean booleanValue = true;
            Byte byteValue = Byte.Parse("1");
            byte[] bytesValue = new byte[3];
            Char charValue = (Char) 'a';
			Double doubleValue = Double.Parse("1.5", CultureInfo.InvariantCulture);
			Single floatValue = Single.Parse("1.5", CultureInfo.InvariantCulture);
            Int32 intValue = Int32.Parse("1");
            Int64 longValue = Int64.Parse("1");
            Int16 shortValue = Int16.Parse("1");
            String stringValue = "string";
    
            try 
            {
                msg.Body["boolean"] = booleanValue;
                msg.Body["byte"] = byteValue;
                msg.Body["bytes"] = bytesValue;
                msg.Body["char"] = charValue;
                msg.Body["double"] = doubleValue;
                msg.Body["float"] = floatValue;
                msg.Body["int"] = intValue;
                msg.Body["long"] = longValue;
                msg.Body["short"] = shortValue;
                msg.Body["string"] = stringValue;
            } 
            catch(MessageFormatException)
            {
                Assert.Fail("object formats should be correct");
            }
    
            msg = (ActiveMQMapMessage)msg.Clone();
    
            Assert.IsTrue(msg.Body["boolean"] is Boolean);
            Assert.AreEqual(msg.Body["boolean"], booleanValue);
            Assert.AreEqual(msg.Body.GetBool("boolean"), booleanValue);
            Assert.IsTrue(msg.Body["byte"] is Byte);
            Assert.AreEqual(msg.Body["byte"], byteValue);
            Assert.AreEqual(msg.Body.GetByte("byte"), byteValue);
            Assert.IsTrue(msg.Body["bytes"] is byte[]);
            Assert.AreEqual(((byte[])msg.Body["bytes"]).Length, bytesValue.Length);
            Assert.AreEqual((msg.Body["bytes"] as byte[]).Length, bytesValue.Length);
            Assert.IsTrue(msg.Body["char"] is Char);
            Assert.AreEqual(msg.Body["char"], charValue);
            Assert.AreEqual(msg.Body.GetChar("char"), charValue);
            Assert.IsTrue(msg.Body["double"] is Double);
            Assert.AreEqual(msg.Body["double"], doubleValue);
            Assert.AreEqual(msg.Body.GetDouble("double"), doubleValue, 0);
            Assert.IsTrue(msg.Body["float"] is Single);
            Assert.AreEqual(msg.Body["float"], floatValue);
            Assert.AreEqual(msg.Body.GetFloat("float"), floatValue, 0);
            Assert.IsTrue(msg.Body["int"] is Int32);
            Assert.AreEqual(msg.Body["int"], intValue);
            Assert.AreEqual(msg.Body.GetInt("int"), intValue);
            Assert.IsTrue(msg.Body["long"] is Int64);
            Assert.AreEqual(msg.Body["long"], longValue);
            Assert.AreEqual(msg.Body.GetLong("long"), longValue);
            Assert.IsTrue(msg.Body["short"] is Int16);
            Assert.AreEqual(msg.Body["short"], shortValue);
            Assert.AreEqual(msg.Body.GetShort("short"), shortValue);
            Assert.IsTrue(msg.Body["string"] is String);
            Assert.AreEqual(msg.Body["string"], stringValue);
            Assert.AreEqual(msg.Body.GetString("string"), stringValue);
    
            msg.ClearBody();
            try 
            {
                msg.Body["object"] = new Object();
                Assert.Fail("should have thrown exception");
            }
            catch(MessageFormatException)
            {
            }
        }
    
        [Test]
        public void TestGetMapNames() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            msg.Body.SetBool("boolean", true);
            msg.Body.SetByte("byte", (byte)1);
            msg.Body["bytes1"] = new byte[1];
            msg.Body.SetChar("char", 'a');
            msg.Body.SetDouble("double", 1.5);
            msg.Body.SetFloat("float", 1.5f);
            msg.Body.SetInt("int", 1);
            msg.Body.SetLong("long", 1);
            msg.Body["object"] = "stringObj";
            msg.Body.SetShort("short", (short)1);
            msg.Body.SetString("string", "string");
    
            msg = (ActiveMQMapMessage)msg.Clone();
    
            ICollection mapNames = msg.Body.Keys;
            System.Collections.ArrayList mapNamesList = new System.Collections.ArrayList(mapNames);
    
            Assert.AreEqual(mapNamesList.Count, 11);
            Assert.IsTrue(mapNamesList.Contains("boolean"));
            Assert.IsTrue(mapNamesList.Contains("byte"));
            Assert.IsTrue(mapNamesList.Contains("bytes1"));
            Assert.IsTrue(mapNamesList.Contains("char"));
            Assert.IsTrue(mapNamesList.Contains("double"));
            Assert.IsTrue(mapNamesList.Contains("float"));
            Assert.IsTrue(mapNamesList.Contains("int"));
            Assert.IsTrue(mapNamesList.Contains("long"));
            Assert.IsTrue(mapNamesList.Contains("object"));
            Assert.IsTrue(mapNamesList.Contains("short"));
            Assert.IsTrue(mapNamesList.Contains("string"));
        }
    
        [Test]
        public void TestItemExists() 
        {
            ActiveMQMapMessage mapMessage = new ActiveMQMapMessage();
    
            mapMessage.Body.SetString("exists", "test");
    
            mapMessage = (ActiveMQMapMessage)mapMessage.Clone();
    
            Assert.IsTrue(mapMessage.Body.Contains("exists"));
            Assert.IsFalse(mapMessage.Body.Contains("doesntExist"));
        }
    
        [Test]
        public void TestClearBody() 
        {
            ActiveMQMapMessage mapMessage = new ActiveMQMapMessage();
            mapMessage.Body.SetString("String", "String");
            mapMessage.ClearBody();
            Assert.IsFalse(mapMessage.ReadOnlyBody);
    
            mapMessage.OnSend();
            mapMessage.Content = mapMessage.Content;
            Assert.IsNull(mapMessage.Body.GetString("String"));
            mapMessage.ClearBody();
            mapMessage.Body.SetString("String", "String");
    
            mapMessage = (ActiveMQMapMessage)mapMessage.Clone();
    
            mapMessage.Body.GetString("String");
        }

        [Test]
        public void TestReadOnlyBody() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            msg.Body.SetBool("boolean", true);
            msg.Body.SetByte("byte", (byte)1);
            msg.Body["bytes"] = new byte[1];
            msg.Body.SetChar("char", 'a');
            msg.Body.SetDouble("double", 1.5);
            msg.Body.SetFloat("float", 1.5f);
            msg.Body.SetInt("int", 1);
            msg.Body.SetLong("long", 1);
            msg.Body["object"] = "stringObj";
            msg.Body.SetShort("short", (short)1);
            msg.Body.SetString("string", "string");
    
            msg.ReadOnlyBody = true;
    
            try 
            {
                msg.Body.GetBool("boolean");
                msg.Body.GetByte("byte");
                Assert.IsNotNull(msg.Body["bytes"]);
                msg.Body.GetChar("char");
                msg.Body.GetDouble("double");
                msg.Body.GetFloat("float");
                msg.Body.GetInt("int");
                msg.Body.GetLong("long");
                Assert.IsNotNull(msg.Body["object"]); 
                msg.Body.GetShort("short");
                msg.Body.GetString("string");
            } 
            catch(MessageNotReadableException) 
            {
                Assert.Fail("should be readable");
            }
            
            try 
            {
                msg.Body.SetBool("boolean", true);
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Body.SetByte("byte", (byte)1);
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Body["bytes"] = new byte[1];
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Body.SetChar("char", 'a');
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Body.SetDouble("double", 1.5);
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Body.SetFloat("float", 1.5f);
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try
            {
                msg.Body.SetInt("int", 1);
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException)
            {
            }
            
            try 
            {
                msg.Body.SetLong("long", 1);
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try
            {
                msg.Body["object"] = "stringObj";
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Body.SetShort("short", (short)1);
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Body.SetString("string", "string");
                Assert.Fail("should throw exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
        }
    
        [Test]
        public void TestWriteOnlyBody() 
        {
            ActiveMQMapMessage msg = new ActiveMQMapMessage();
            msg.ReadOnlyBody = false;
    
            msg.Body.SetBool("boolean", true);
            msg.Body.SetByte("byte", (byte)1);
            msg.Body["bytes"] = new byte[1];
            msg.Body.SetChar("char", 'a');
            msg.Body.SetDouble("double", 1.5);
            msg.Body.SetFloat("float", 1.5f);
            msg.Body.SetInt("int", 1);
            msg.Body.SetLong("long", 1);
            msg.Body["object"] = "stringObj";
            msg.Body.SetShort("short", (short)1);
            msg.Body.SetString("string", "string");
    
            msg.ReadOnlyBody = true;
    
            msg.Body.GetBool("boolean");
            msg.Body.GetByte("byte");
            Assert.IsNotNull(msg.Body["bytes"]); 
            msg.Body.GetChar("char");
            msg.Body.GetDouble("double");
            msg.Body.GetFloat("float");
            msg.Body.GetInt("int");
            msg.Body.GetLong("long");
            Assert.IsNotNull(msg.Body["object"]); 
            msg.Body.GetShort("short");
            msg.Body.GetString("string");
        }

        [Test]
        public void TestMessageQueueDequeQueueDequeue()
        {
            using (IConnection connection = CreateConnection())
            using (ISession session = connection.CreateSession())
            {
                IDestination destination = session.GetQueue("TestMessageQueueDequeQueueDequeue");

                (connection as Connection).DeleteDestination(destination);

                using (IMessageConsumer consumer = session.CreateConsumer(destination))
                using (IMessageProducer producer = session.CreateProducer(destination))
                {
                    connection.Start();

                    producer.DeliveryMode = MsgDeliveryMode.Persistent;

                    IMapMessage request = session.CreateMapMessage();
                    request.Body.SetString("Unit-Test-Key", "Unit-Test-Value");
                    Assert.IsNotNull(request, "request is null");
                    Assert.IsTrue(request.Body.Contains("Unit-Test-Key"), "Unit-Test-Key does not exist");
                    producer.Send(request);

                    // Relay from Queue back again.
                    IMessage received = consumer.Receive(TimeSpan.FromSeconds(2));
                    Assert.IsNotNull(received);
                    IMapMessage mapMessage = received as IMapMessage;
                    Assert.IsNotNull(mapMessage);
                    producer.Send(mapMessage);

                    // Read it again and validate.
                    received = consumer.Receive(TimeSpan.FromSeconds(2));
                    Assert.IsNotNull(received);
                    mapMessage = received as IMapMessage;
                    Assert.IsNotNull(mapMessage, "currentMessage is null");

                    // This entry in the map message should not be removed
                    Assert.IsTrue(mapMessage.Body.Contains("Unit-Test-Key"), "Unit-Test-Key does not exist");
                }
            }
        }
    }
}
