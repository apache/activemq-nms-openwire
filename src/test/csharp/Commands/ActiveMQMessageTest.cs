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
using System.Text;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.ActiveMQ.OpenWire;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test.Commands
{
    [TestFixture]
    public class ActiveMQMessageTest
    {
        private string nmsMessageID;
        private string nmsCorrelationID;
        private ActiveMQTopic nmsDestination;
        private ActiveMQTempTopic nmsReplyTo;
        private MsgDeliveryMode nmsDeliveryMode;
        private bool nmsRedelivered;
        private string nmsType;
        private MsgPriority nmsPriority;
        private DateTime nmsTimestamp;
        private long[] consumerIDs;

        [SetUp]
        public virtual void SetUp()
        {
            this.nmsMessageID = "testid";
            this.nmsCorrelationID = "testcorrelationid";
            this.nmsDestination = new ActiveMQTopic("TEST.test.topic");
            this.nmsReplyTo = new ActiveMQTempTopic("TEST.test.replyto.topic:001");
            this.nmsDeliveryMode = MsgDeliveryMode.NonPersistent;
            this.nmsRedelivered = true;
            this.nmsType = "test type";
            this.nmsPriority = MsgPriority.High;
            this.nmsTimestamp = DateTime.Now;
            this.consumerIDs = new long[3];
            
            for(int i = 0; i < this.consumerIDs.Length; i++) 
            {
                this.consumerIDs[i] = i;
            }
        }

        [Test]
        public void TestSetReadOnly() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.ReadOnlyProperties =  true;
            bool test = false;
            
            try
            {
                msg.Properties.SetInt("test", 1);
            } 
            catch(MessageNotWriteableException) 
            {
                test = true;
            } 
            catch(NMSException) 
            {
                test = false;
            }
            
            Assert.IsTrue(test);
        }

        [Test]
        public void TestSetToForeignNMSID() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.NMSMessageId = "ID:EMS-SERVER.8B443C380083:429";
        }

        [Test]
        public void TestEqualsObject() 
        {
            ActiveMQMessage msg1 = new ActiveMQMessage();
            ActiveMQMessage msg2 = new ActiveMQMessage();
            msg1.NMSMessageId = this.nmsMessageID;
            Assert.IsTrue(!msg1.Equals(msg2));
            msg2.NMSMessageId = this.nmsMessageID;
            Assert.IsTrue(msg1.Equals(msg2));
        }

        [Test]
        public void TestShallowCopy() 
        {
            ActiveMQMessage msg1 = new ActiveMQMessage();
            msg1.NMSMessageId = nmsMessageID;
            ActiveMQMessage msg2 = (ActiveMQMessage) msg1.Clone();
            Assert.IsTrue(msg1 != msg2 && msg1.Equals(msg2));
        }

        [Test]
        public void TestCopy() 
        {
            this.nmsMessageID = "ID:1141:45278:429";
            this.nmsCorrelationID = "testcorrelationid";
            this.nmsDestination = new ActiveMQTopic("test.topic");
            this.nmsReplyTo = new ActiveMQTempTopic("test.replyto.topic:001");
            this.nmsDeliveryMode = MsgDeliveryMode.NonPersistent;
            this.nmsType = "test type";
            this.nmsPriority = MsgPriority.High;
            this.nmsTimestamp = DateTime.Now;
    
            ActiveMQMessage msg1 = new ActiveMQMessage();
            msg1.NMSMessageId = this.nmsMessageID;
            msg1.NMSCorrelationID = this.nmsCorrelationID;
            msg1.FromDestination = this.nmsDestination;
            msg1.NMSReplyTo = this.nmsReplyTo;
            msg1.NMSDeliveryMode = this.nmsDeliveryMode;
            msg1.NMSType = this.nmsType;
            msg1.NMSPriority = this.nmsPriority;
            msg1.NMSTimestamp = this.nmsTimestamp;
            msg1.ReadOnlyProperties = true;
            
            ActiveMQMessage msg2 = msg1.Clone() as ActiveMQMessage;
            
            Assert.IsTrue(msg1.NMSMessageId.Equals(msg2.NMSMessageId));
            Assert.IsTrue(msg1.NMSCorrelationID.Equals(msg2.NMSCorrelationID));
            Assert.IsTrue(msg1.NMSDestination.Equals(msg2.NMSDestination));
            Assert.IsTrue(msg1.NMSReplyTo.Equals(msg2.NMSReplyTo));
            Assert.IsTrue(msg1.NMSDeliveryMode == msg2.NMSDeliveryMode);
            Assert.IsTrue(msg1.NMSRedelivered == msg2.NMSRedelivered);
            Assert.IsTrue(msg1.NMSType.Equals(msg2.NMSType));
            Assert.IsTrue(msg1.NMSPriority == msg2.NMSPriority);
            Assert.IsTrue(msg1.NMSTimestamp == msg2.NMSTimestamp);
        }

        [Test]
        public void TestGetAndSetNMSCorrelationID() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.NMSCorrelationID = this.nmsCorrelationID;
            Assert.IsTrue(msg.NMSCorrelationID.Equals(this.nmsCorrelationID));		
			
			// Check that we get the real value even if we go through the properties
			string nmsCorrelationId = msg.Properties.GetString("NMSCorrelationID");
			Assert.IsTrue(nmsCorrelationId.Equals(this.nmsCorrelationID));
			
			// We shouldn't get the expected value though if we use the inner property 
			// name from ActiveMQMessage
			string correlationId = (string) msg.Properties["correlationId"];
			Assert.IsNullOrEmpty(correlationId);			
			msg.Properties["correlationId"] = "TEST";
			correlationId = (string) msg.Properties["correlationId"];
			Assert.IsFalse(msg.Properties["NMSCorrelationID"].Equals(msg.Properties["correlationId"]));
        }
    
        [Test]
        public void TestGetAndSetNMSReplyTo()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.NMSReplyTo = this.nmsReplyTo;
            Assert.AreEqual(msg.NMSReplyTo, this.nmsReplyTo);
        }
    
        [Test]
        public void TestGetAndSetNMSDestination() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.FromDestination = this.nmsDestination;
            Assert.AreEqual(msg.NMSDestination, this.nmsDestination);
        }
    
        [Test]
        public void TestGetAndSetNMSDeliveryMode()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.NMSDeliveryMode = this.nmsDeliveryMode;
            Assert.IsTrue(msg.NMSDeliveryMode == this.nmsDeliveryMode);
        }
    
        [Test]
        public void TestGetAndSetMSRedelivered()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.RedeliveryCounter = 2;
            Assert.IsTrue(msg.NMSRedelivered == this.nmsRedelivered);
        }
    
        [Test]
        public void TestGetAndSetNMSType() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.NMSType = this.nmsType;
            Assert.AreEqual(msg.NMSType, this.nmsType);
        }
    
        [Test]
        public void TestGetAndSetNMSPriority() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.NMSPriority = this.nmsPriority;
            Assert.IsTrue(msg.NMSPriority == this.nmsPriority);
        }
    
        public void TestClearProperties()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.Properties.SetString("test", "test");
            msg.Content = new byte[1];
            msg.NMSMessageId = this.nmsMessageID;
            msg.ClearProperties();
            Assert.IsNull(msg.Properties.GetString("test"));
            Assert.IsNotNull(msg.NMSMessageId);
            Assert.IsNotNull(msg.Content);
        }

        [Test]
        public void TestPropertyExists() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.Properties.SetString("test", "test");
            Assert.IsTrue(msg.Properties.Contains("test"));
        }
    
        [Test]
        public void TestGetBooleanProperty() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "booleanProperty";
            msg.Properties.SetBool(name, true);
            Assert.IsTrue(msg.Properties.GetBool(name));
        }
    
        [Test]
        public void TestGetByteProperty() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "byteProperty";
            msg.Properties.SetByte(name, (byte)1);
            Assert.IsTrue(msg.Properties.GetByte(name) == 1);
        }
    
        [Test]
        public void TestGetShortProperty()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "shortProperty";
            msg.Properties.SetShort(name, (short)1);
            Assert.IsTrue(msg.Properties.GetShort(name) == 1);
        }
    
        [Test]
        public void TestGetIntProperty()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "intProperty";
            msg.Properties.SetInt(name, 1);
            Assert.IsTrue(msg.Properties.GetInt(name) == 1);
        }
    
        [Test]
        public void TestGetLongProperty() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "longProperty";
            msg.Properties.SetLong(name, 1);
            Assert.IsTrue(msg.Properties.GetLong(name) == 1);
        }
    
        [Test]
        public void TestGetFloatProperty()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "floatProperty";
            msg.Properties.SetFloat(name, 1.3f);
            Assert.IsTrue(msg.Properties.GetFloat(name) == 1.3f);
        }
    
        [Test]
        public void TestGetDoubleProperty()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "doubleProperty";
            msg.Properties.SetDouble(name, 1.3d);
            Assert.IsTrue(msg.Properties.GetDouble(name) == 1.3);
        }
    
        [Test]
        public void TestGetStringProperty()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "stringProperty";
            msg.Properties.SetString(name, name);
            Assert.IsTrue(msg.Properties.GetString(name).Equals(name));
        }
    
        [Test]
        public void TestGetObjectProperty()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "floatProperty";
            msg.Properties.SetFloat(name, 1.3f);
            Assert.IsTrue(msg.Properties[name] is float);
            Assert.IsTrue((float)msg.Properties[name] == 1.3f);
        }
    
        [Test]
        public void TestGetPropertyNames() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "floatProperty";
            msg.Properties.SetFloat(name, 1.3f);
            
            foreach(string key in msg.Properties.Keys)
            {
                Assert.IsTrue(key.Equals(name));
            }
        }

        [Test]
        public void TestSetObjectProperty() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "property";
    
            try 
            {
                msg.Properties[name] = "string";
                msg.Properties[name] = (Byte) 1;
                msg.Properties[name] = (Int16) 1;
                msg.Properties[name] = (Int32) 1;
                msg.Properties[name] = (Int64) 1;
                msg.Properties[name] = (Single) 1.1f;
                msg.Properties[name] = (Double) 1.1;
                msg.Properties[name] = (Boolean) true;
                msg.Properties[name] = null;
            } 
            catch(MessageFormatException) 
            {
                Assert.Fail("should accept object primitives and String");
            }

            try
            {
                msg.Properties[name] = new Object();
                Assert.Fail("should accept only object primitives and String");
            } 
            catch(MessageFormatException) 
            {
            }

            try
            {
                msg.Properties[name] = new StringBuilder();
                Assert.Fail("should accept only object primitives and String");
            } 
            catch(MessageFormatException) 
            {
            }            
        }

        [Test]
        public void TestConvertProperties() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
    
            msg.Properties["stringProperty"] = "string";
            msg.Properties["byteProperty"] = (Byte) 1;
            msg.Properties["shortProperty"] = (Int16) 1;
            msg.Properties["intProperty"] = (Int32) 1;
            msg.Properties["longProperty"] = (Int64) 1;
            msg.Properties["floatProperty"] = (Single) 1.1f;
            msg.Properties["doubleProperty"] = (Double) 1.1;
            msg.Properties["booleanProperty"] = (Boolean) true;
            msg.Properties["nullProperty"] = null;
    
            msg.BeforeMarshall(new OpenWireFormat());

            IPrimitiveMap properties = msg.Properties;
            Assert.AreEqual(properties["stringProperty"], "string");
            Assert.AreEqual((byte)properties["byteProperty"], (byte) 1);
            Assert.AreEqual((short)properties["shortProperty"], (short) 1);
            Assert.AreEqual((int)properties["intProperty"], (int) 1);
            Assert.AreEqual((long)properties["longProperty"], (long) 1);
            Assert.AreEqual((float)properties["floatProperty"], 1.1f, 0);
            Assert.AreEqual((double)properties["doubleProperty"], 1.1, 0);
            Assert.AreEqual((bool)properties["booleanProperty"], true);
            Assert.IsNull(properties["nullProperty"]);
    
        }

        [Test]
        public void TestSetNullProperty() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            string name = "cheese";
            msg.Properties.SetString(name, "Cheddar");
            Assert.AreEqual("Cheddar", msg.Properties.GetString(name));
    
            msg.Properties.SetString(name, null);
            Assert.AreEqual(null, msg.Properties.GetString(name));
        }

        [Test]
        public void TestSetNullPropertyName() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();

            try
            {
                msg.Properties.SetString(null, "Cheese");
                Assert.Fail("Should have thrown exception");
            }
            catch(Exception)
            {
            }
        }

        [Test]
        public void TestSetEmptyPropertyName() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
    
            try 
            {
                msg.Properties.SetString("", "Cheese");
                Assert.Fail("Should have thrown exception");
            }
            catch(Exception)
            {
            }
        }

        [Test]
        public void TestGetAndSetNMSXDeliveryCount() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.Properties.SetInt("NMSXDeliveryCount", 1);
            int count = msg.Properties.GetInt("NMSXDeliveryCount");
            Assert.IsTrue(count == 1, "expected delivery count = 1 - got: " + count);
        }

        [Test]
        public void TestClearBody()
        {
            ActiveMQBytesMessage message = new ActiveMQBytesMessage();
            message.ClearBody();
            Assert.IsFalse(message.ReadOnlyBody);
        }
    
        [Test]
        public void TestBooleanPropertyConversion() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
            msg.Properties.SetBool(propertyName, true);
    
            Assert.AreEqual((bool)msg.Properties[propertyName], true);
            Assert.IsTrue(msg.Properties.GetBool(propertyName));
            Assert.AreEqual(msg.Properties.GetString(propertyName), "True");
            try 
            {
                msg.Properties.GetByte(propertyName);
                Assert.Fail("Should have thrown exception");
            }
            catch(MessageFormatException)
            {
            }
            
            try
            {
                msg.Properties.GetShort(propertyName);
                Assert.Fail("Should have thrown exception");
            }
            catch(MessageFormatException)
            {
            }
            
            try 
            {
                msg.Properties.GetInt(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetLong(propertyName);
                Assert.Fail("Should have thrown exception");
            }
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetFloat(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetDouble(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
        }

        [Test]
        public void TestBytePropertyConversion() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
            msg.Properties.SetByte(propertyName, (byte)1);
    
            Assert.AreEqual((byte)msg.Properties[propertyName], 1);
            Assert.AreEqual(msg.Properties.GetByte(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetShort(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetInt(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetLong(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetString(propertyName), "1");
            
            try 
            {
                msg.Properties.GetBool(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
            
            try 
            {
                msg.Properties.GetFloat(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
            
            try {
                msg.Properties.GetDouble(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
        }

        [Test]
        public void TestShortPropertyConversion() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
            msg.Properties.SetShort(propertyName, (short)1);
    
            Assert.AreEqual((short)msg.Properties[propertyName], 1);
            Assert.AreEqual(msg.Properties.GetShort(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetInt(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetLong(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetString(propertyName), "1");
            
            try 
            {
                msg.Properties.GetBool(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetByte(propertyName);
                Assert.Fail("Should have thrown exception");
            }
            catch(MessageFormatException)
            {
            }
            
            try 
            {
                msg.Properties.GetFloat(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetDouble(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
        }

        [Test]
        public void TestIntPropertyConversion() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
            msg.Properties.SetInt(propertyName, (int)1);
    
            Assert.AreEqual((int)msg.Properties[propertyName], 1);
            Assert.AreEqual(msg.Properties.GetInt(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetLong(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetString(propertyName), "1");
            
            try
            {
                msg.Properties.GetBool(propertyName);
                Assert.Fail("Should have thrown exception");
            }
            catch(MessageFormatException) 
            {
            }
            
            try
            {
                msg.Properties.GetByte(propertyName);
                Assert.Fail("Should have thrown exception");
            }
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetShort(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetFloat(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetDouble(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
        }

        [Test]
        public void TestLongPropertyConversion()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
            msg.Properties.SetLong(propertyName, 1);
    
            Assert.AreEqual((long)msg.Properties[propertyName], 1);
            Assert.AreEqual(msg.Properties.GetLong(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetString(propertyName), "1");
            
            try
            {
                msg.Properties.GetBool(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetByte(propertyName);
                Assert.Fail("Should have thrown exception");
            } catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetShort(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetInt(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetFloat(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try
            {
                msg.Properties.GetDouble(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
        }

        [Test]
        public void TestFloatPropertyConversion() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
			float floatValue = (float)1.5;
			msg.Properties.SetFloat(propertyName, floatValue);
			Assert.AreEqual((float)msg.Properties[propertyName], floatValue, 0);
			Assert.AreEqual(msg.Properties.GetFloat(propertyName), floatValue, 0);
			Assert.AreEqual(msg.Properties.GetDouble(propertyName), floatValue, 0);
			Assert.AreEqual(msg.Properties.GetString(propertyName), floatValue.ToString());
            
            try
            {
                msg.Properties.GetBool(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetByte(propertyName);
                Assert.Fail("Should have thrown exception");
            }
            catch(MessageFormatException) 
            {
            }
            
            try
            {
                msg.Properties.GetShort(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try
            {
                msg.Properties.GetInt(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
            
            try 
            {
                msg.Properties.GetLong(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
        }

        [Test]
        public void TestDoublePropertyConversion() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
			Double doubleValue = 1.5;
            msg.Properties.SetDouble(propertyName, doubleValue);
			Assert.AreEqual((double)msg.Properties[propertyName], doubleValue, 0);
			Assert.AreEqual(msg.Properties.GetDouble(propertyName), doubleValue, 0);
			Assert.AreEqual(msg.Properties.GetString(propertyName), doubleValue.ToString());
            
            try 
            {
                msg.Properties.GetBool(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
            
            try 
            {
                msg.Properties.GetByte(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
            
            try 
            {
                msg.Properties.GetShort(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetInt(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetLong(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetFloat(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
        }
    
        [Test]
        public void TestStringPropertyConversion()
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
            String stringValue = "True";
            msg.Properties.SetString(propertyName, stringValue);
            Assert.AreEqual(msg.Properties.GetString(propertyName), stringValue);
            Assert.AreEqual((string)msg.Properties[propertyName], stringValue);
            Assert.AreEqual(msg.Properties.GetBool(propertyName), true);
    
            stringValue = "1";
            msg.Properties.SetString(propertyName, stringValue);
            Assert.AreEqual(msg.Properties.GetByte(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetShort(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetInt(propertyName), 1);
            Assert.AreEqual(msg.Properties.GetLong(propertyName), 1);

			Double doubleValue = 1.5;
			stringValue = doubleValue.ToString();
            msg.Properties.SetString(propertyName, stringValue);
            Assert.AreEqual(msg.Properties.GetFloat(propertyName), 1.5, 0);
            Assert.AreEqual(msg.Properties.GetDouble(propertyName), 1.5, 0);
    
            stringValue = "bad";
            msg.Properties.SetString(propertyName, stringValue);
            
            try
            {
                msg.Properties.GetByte(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
            
            try 
            {
                msg.Properties.GetShort(propertyName);
                Assert.Fail("Should have thrown exception");
            }
            catch(MessageFormatException)
            {
            }
            
            try 
            {
                msg.Properties.GetInt(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetLong(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException) 
            {
            }
            
            try 
            {
                msg.Properties.GetFloat(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
            
            try
            {
                msg.Properties.GetDouble(propertyName);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageFormatException)
            {
            }
            
            Assert.IsFalse(msg.Properties.GetBool(propertyName));
        }

        [Test]
        public void TestReadOnlyProperties() 
        {
            ActiveMQMessage msg = new ActiveMQMessage();
            String propertyName = "property";
            msg.ReadOnlyProperties = true;
    
            try 
            {
                msg.Properties[propertyName] = new Object();
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Properties.SetString(propertyName, "test");
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Properties.SetBool(propertyName, true);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Properties.SetByte(propertyName, (byte)1);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException) {
            }
            
            try 
            {
                msg.Properties.SetShort(propertyName, (short)1);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Properties.SetInt(propertyName, 1);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException) 
            {
            }
            
            try 
            {
                msg.Properties.SetLong(propertyName, 1);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException)
            {
            }
            
            try
            {
                msg.Properties.SetFloat(propertyName, (float)1.5);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException)
            {
            }
            
            try 
            {
                msg.Properties.SetDouble(propertyName, 1.5);
                Assert.Fail("Should have thrown exception");
            } 
            catch(MessageNotWriteableException)
            {
            }
        }

//        public void TestIsExpired() {
//            ActiveMQMessage msg = new ActiveMQMessage();
//            msg.NMSExpiration(System.currentTimeMillis() - 1);
//            Assert.IsTrue(msg.isExpired());
//            msg.NMSExpiration(System.currentTimeMillis() + 10000);
//            Assert.IsFalse(msg.isExpired());
//        }
    }
}
