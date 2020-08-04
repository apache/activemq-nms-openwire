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
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test.Commands
{	
    [TestFixture]	
	public class ActiveMQStreamMessageTest
	{
		[Test]
		public void TestCommand()
		{
			ActiveMQStreamMessage message = new ActiveMQStreamMessage();
			
			Assert.IsNull( message.Content );
			Assert.IsTrue( !message.ReadOnlyBody );
			Assert.IsTrue( !message.ReadOnlyProperties );
		}
		
		[Test]
	    public void TestReadBoolean() {
	        
			ActiveMQStreamMessage msg = new ActiveMQStreamMessage();

	        msg.WriteBoolean(true);
	        msg.Reset();
            Assert.IsTrue(msg.ReadBoolean());
			msg.Reset();
            Assert.IsTrue(msg.ReadString() == "True");
            
			msg.Reset();
            try 
			{
                msg.ReadByte();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadInt16();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadInt32();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadInt64();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadSingle();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadDouble();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadBytes(new byte[1]);
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
	    }
	
		[Test]
	    public void TestReadByte() 
		{
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();

            byte test = (byte)4;
            msg.WriteByte(test);
            msg.Reset();
            Assert.IsTrue(msg.ReadByte() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt16() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt32() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt64() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadString() == (test).ToString());
            
			msg.Reset();
            try 
			{
                msg.ReadBoolean();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadSingle();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try
			{
                msg.ReadDouble();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try 
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
			
            msg.Reset();
            try
			{
                msg.ReadBytes(new byte[1]);
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
	    }
	
		[Test]
	    public void TestReadInt16() {
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();

            short test = (short)4;
            msg.WriteInt16(test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt16() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt32() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt64() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadString() == (test).ToString());
            msg.Reset();
            try 
			{
                msg.ReadBoolean();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadByte();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadSingle();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadDouble();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadBytes(new byte[1]);
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
	    }
	
		[Test]
	    public void TestReadChar() {
			
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();

            char test = 'z';
            msg.WriteChar(test);
            msg.Reset();
            Assert.IsTrue(msg.ReadChar() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadString() == (test).ToString());
            msg.Reset();
            try 
			{
                msg.ReadBoolean();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadByte();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt16();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt32();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt64();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadSingle();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadDouble();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadBytes(new byte[1]);
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException) 
			{
            }
	    }
	
		[Test]
	    public void TestReadInt() {
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
			
            int test = 4;
            msg.WriteInt32(test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt32() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt64() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadString() == (test).ToString());
            msg.Reset();
            try
			{
                msg.ReadBoolean();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadByte();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try
			{
                msg.ReadInt16();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadSingle();
                Assert.Fail("Should have thrown exception");
			}
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadDouble();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try
			{
                msg.ReadBytes(new byte[1]);
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
	    }
	
		[Test]
	    public void TestReadLong() 
		{
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();

            long test = 4L;
            msg.WriteInt64(test);
            msg.Reset();
            Assert.IsTrue(msg.ReadInt64() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadString() == (test).ToString());
            msg.Reset();
            try 
			{
                msg.ReadBoolean();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadByte();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadInt16();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadInt32();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadSingle();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadDouble();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadBytes(new byte[1]);
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg = new ActiveMQStreamMessage();
            msg.WriteObject((Int64) 1);
            // Reset so it's readable now
            msg.Reset();
            Assert.AreEqual( (Int64) 1, msg.ReadObject());
	    }
	
		[Test]
	    public void TestReadFloat() 
		{
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();

			float test = 4.4f;
            msg.WriteSingle(test);
            msg.Reset();
            Assert.IsTrue(msg.ReadSingle() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadDouble() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadString() == (test).ToString());
            msg.Reset();
            try
			{
                msg.ReadBoolean();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try
			{
                msg.ReadByte();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadInt16();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt32();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt64();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadBytes(new byte[1]);
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
	    }
	
		[Test]
	    public void TestReadDouble() 
		{
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();

			double test = 4.4;
            msg.WriteDouble(test);
            msg.Reset();
            Assert.IsTrue(msg.ReadDouble() == test);
            msg.Reset();
            Assert.IsTrue(msg.ReadString() == (test).ToString());
            msg.Reset();
            try
			{
                msg.ReadBoolean();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadByte();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt16();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt32();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt64();
                Assert.Fail("Should have thrown exception");
            }
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadSingle();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.Reset();
            try
			{
                msg.ReadBytes(new byte[1]);
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
	    }
	
		[Test]
	    public void TestReadString() 
		{
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();

			byte testByte = (byte)2;
            msg.WriteString(((Byte)testByte).ToString());
            msg.Reset();
            Assert.IsTrue(msg.ReadByte() == testByte);
            msg.ClearBody();
            short testShort = 3;
            msg.WriteString(((Int16)testShort).ToString());
            msg.Reset();
            Assert.IsTrue(msg.ReadInt16() == testShort);
            msg.ClearBody();
            int testInt = 4;
            msg.WriteString(((Int32)testInt).ToString());
            msg.Reset();
            Assert.IsTrue(msg.ReadInt32() == testInt);
            msg.ClearBody();
            long testLong = 6L;
            msg.WriteString(((Int64)testLong).ToString());
            msg.Reset();
            Assert.IsTrue(msg.ReadInt64() == testLong);
            msg.ClearBody();
            float testFloat = 6.6f;
            msg.WriteString(((Single)testFloat).ToString());
            msg.Reset();
            Assert.IsTrue(msg.ReadSingle() == testFloat);
            msg.ClearBody();
            double testDouble = 7.7d;
            msg.WriteString(((Double)testDouble).ToString());
            msg.Reset();
			Assert.AreEqual( testDouble, msg.ReadDouble(), 0.05 );
            msg.ClearBody();
            msg.WriteString("true");
            msg.Reset();
            Assert.IsTrue(msg.ReadBoolean());
            msg.ClearBody();
            msg.WriteString("a");
            msg.Reset();
            try 
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
            msg.ClearBody();
            msg.WriteString("777");
            msg.Reset();
            try
			{
                msg.ReadBytes(new byte[3]);
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException)
			{
            }
	    }
	
		[Test]
	    public void TestReadBigString() 
		{
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
            // Test with a 1Meg String
            StringBuilder bigSB = new StringBuilder(1024 * 1024);
            for(int i = 0; i < 1024 * 1024; i++) 
		    {
                bigSB.Append((char)'a' + i % 26);
            }
            String bigString = bigSB.ToString();

            msg.WriteString(bigString);
            msg.Reset();
            Assert.AreEqual(bigString, msg.ReadString());
	    }
	
		[Test]
	    public void TestReadBytes()
		{
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
            byte[] test = new byte[50];
            for(int i = 0; i < test.Length; i++)
			{
                test[i] = (byte)i;
            }
            msg.WriteBytes(test);
            msg.Reset();
            byte[] valid = new byte[test.Length];
            msg.ReadBytes(valid);
            for(int i = 0; i < valid.Length; i++) 
			{
                Assert.IsTrue(valid[i] == test[i]);
            }
            msg.Reset();
            try
			{
                msg.ReadByte();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt16();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt32();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadInt64();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try
			{
                msg.ReadSingle();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadChar();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
            msg.Reset();
            try 
			{
                msg.ReadString();
                Assert.Fail("Should have thrown exception");
            } 
			catch(MessageFormatException) 
			{
            }
	    }

		[Test]
	    public void TestReadObject() 
		{
	        ActiveMQStreamMessage msg = new ActiveMQStreamMessage();
            byte testByte = (byte)2;
            msg.WriteByte(testByte);
            msg.Reset();
            Assert.IsTrue((byte)msg.ReadObject() == testByte);
            msg.ClearBody();

            short testShort = 3;
            msg.WriteInt16(testShort);
            msg.Reset();
            Assert.IsTrue((short)msg.ReadObject() == testShort);
            msg.ClearBody();

            int testInt = 4;
            msg.WriteInt32(testInt);
            msg.Reset();
            Assert.IsTrue((int)msg.ReadObject() == testInt);
            msg.ClearBody();
	
            long testLong = 6L;
            msg.WriteInt64(testLong);
            msg.Reset();
            Assert.IsTrue((long)msg.ReadObject() == testLong);
            msg.ClearBody();

            float testFloat = 6.6f;
            msg.WriteSingle(testFloat);
            msg.Reset();
            Assert.IsTrue((float)msg.ReadObject() == testFloat);
            msg.ClearBody();

            double testDouble = 7.7d;
            msg.WriteDouble(testDouble);
            msg.Reset();
            Assert.IsTrue((double)msg.ReadObject() == testDouble);
            msg.ClearBody();

            char testChar = 'z';
            msg.WriteChar(testChar);
            msg.Reset();
            Assert.IsTrue((char)msg.ReadObject() == testChar);
            msg.ClearBody();

            byte[] data = new byte[50];
            for(int i = 0; i < data.Length; i++) 
			{
                data[i] = (byte)i;
            }
            msg.WriteBytes(data);
            msg.Reset();
            byte[] valid = (byte[])msg.ReadObject();
            Assert.IsTrue(valid.Length == data.Length);
            for(int i = 0; i < valid.Length; i++)
			{
                Assert.IsTrue(valid[i] == data[i]);
            }
            msg.ClearBody();
            msg.WriteBoolean(true);
            msg.Reset();
            Assert.IsTrue((bool)msg.ReadObject());
	    }
	
		[Test]
	    public void TestClearBody() 
		{
	        ActiveMQStreamMessage streamMessage = new ActiveMQStreamMessage();
	        try 
			{
	            streamMessage.WriteObject((Int64) 2);
	            streamMessage.ClearBody();
	            Assert.IsFalse(streamMessage.ReadOnlyBody);
	            streamMessage.WriteObject((Int64) 2);
	            streamMessage.ReadObject();
	            Assert.Fail("should throw exception");
	        } 
			catch(MessageNotReadableException) 
			{
	        } 
			catch(MessageNotWriteableException) 
			{
	            Assert.Fail("should be writeable");
	        }
	    }
	
		[Test]
	    public void TestReset() 
		{
	        ActiveMQStreamMessage streamMessage = new ActiveMQStreamMessage();
	        try 
			{
	            streamMessage.WriteDouble(24.5);
	            streamMessage.WriteInt64(311);
	        } 
			catch(MessageNotWriteableException) 
			{
	            Assert.Fail("should be writeable");
	        }
	        streamMessage.Reset();
	        try 
			{
	            Assert.IsTrue(streamMessage.ReadOnlyBody);
	            Assert.AreEqual(streamMessage.ReadDouble(), 24.5, 0);
	            Assert.AreEqual(streamMessage.ReadInt64(), 311);
	        } 
			catch(MessageNotReadableException)
			{
	            Assert.Fail("should be readable");
	        }
	        try 
			{
	            streamMessage.WriteInt32(33);
	            Assert.Fail("should throw exception");
	        } 
			catch(MessageNotWriteableException) 
			{
	        }
	    }
	
		[Test]
	    public void TestReadOnlyBody()
		{
	        ActiveMQStreamMessage message = new ActiveMQStreamMessage();
	        try 
			{
	            message.WriteBoolean(true);
	            message.WriteByte((byte)1);
	            message.WriteBytes(new byte[1]);
	            message.WriteBytes(new byte[3], 0, 2);
	            message.WriteChar('a');
	            message.WriteDouble(1.5);
	            message.WriteSingle((float)1.5);
	            message.WriteInt32(1);
	            message.WriteInt64(1);
	            message.WriteObject("stringobj");
	            message.WriteInt16((short)1);
	            message.WriteString("string");
	        }
			catch(MessageNotWriteableException) 
			{
	            Assert.Fail("Should be writeable");
	        }
	        message.Reset();
	        try 
			{
	            message.ReadBoolean();
	            message.ReadByte();
	            Assert.AreEqual(1, message.ReadBytes(new byte[10]));
	            Assert.AreEqual(-1, message.ReadBytes(new byte[10]));
	            Assert.AreEqual(2, message.ReadBytes(new byte[10]));
	            Assert.AreEqual(-1, message.ReadBytes(new byte[10]));
	            message.ReadChar();
	            message.ReadDouble();
	            message.ReadSingle();
	            message.ReadInt32();
	            message.ReadInt64();
	            message.ReadString();
	            message.ReadInt16();
	            message.ReadString();
	        }
			catch(MessageNotReadableException)
			{
	            Assert.Fail("Should be readable");
	        }
	        try
			{
	            message.WriteBoolean(true);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException)
			{
	        }
	        try 
			{
	            message.WriteByte((byte)1);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException) 
			{
	        }
	        try
			{
	            message.WriteBytes(new byte[1]);
	            Assert.Fail("Should have thrown exception");
	        }
			catch(MessageNotWriteableException)
			{
	        }
	        try 
			{
	            message.WriteBytes(new byte[3], 0, 2);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException)
			{
	        }
	        try {
	            message.WriteChar('a');
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException) 
			{
	        }
	        try 
			{
	            message.WriteDouble(1.5);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException)
			{
	        }
	        try 
			{
	            message.WriteSingle((float)1.5);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException) 
			{
	        }
	        try
			{
	            message.WriteInt32(1);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException) 
			{
	        }
	        try 
			{
	            message.WriteInt64(1);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException)
			{
	        }
	        try 
			{
	            message.WriteObject("stringobj");
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException)
			{
	        }
	        try 
			{
	            message.WriteSingle((short)1);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException) 
			{
	        }
	        try 
			{
	            message.WriteString("string");
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotWriteableException) 
			{
	        }
	    }
	
		[Test]
	    public void TestWriteOnlyBody()
		{
	        ActiveMQStreamMessage message = new ActiveMQStreamMessage();
	        message.ClearBody();
	        try 
			{
	            message.WriteBoolean(true);
	            message.WriteByte((byte)1);
	            message.WriteBytes(new byte[1]);
	            message.WriteBytes(new byte[3], 0, 2);
	            message.WriteChar('a');
	            message.WriteDouble(1.5);
	            message.WriteSingle((float)1.5);
	            message.WriteInt32(1);
	            message.WriteInt64(1);
	            message.WriteObject("stringobj");
	            message.WriteSingle((short)1);
	            message.WriteString("string");
	        } 
			catch(MessageNotWriteableException)
			{
	            Assert.Fail("Should be writeable");
	        }
	        try 
			{
	            message.ReadBoolean();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException)
			{
	        }
	        try 
			{
	            message.ReadByte();
	            Assert.Fail("Should have thrown exception");
	        } catch(MessageNotReadableException)
			{
	        }
	        try 
			{
	            message.ReadBytes(new byte[1]);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException)
			{
	        }
	        try 
			{
	            message.ReadBytes(new byte[2]);
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException)
			{
	        }
	        try 
			{
	            message.ReadChar();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException) 
			{
	        }
	        try 
			{
	            message.ReadDouble();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException) 
			{
	        }
	        try 
			{
	            message.ReadSingle();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException) {
	        }
			
	        try 
			{
	            message.ReadInt32();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException)
			{
	        }
	        try
			{
	            message.ReadInt64();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException) 
			{
	        }
	        try
			{
	            message.ReadString();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException) 
			{
	        }
	        try 
			{
	            message.ReadInt16();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException) 
			{
	        }
	        try 
			{
	            message.ReadString();
	            Assert.Fail("Should have thrown exception");
	        } 
			catch(MessageNotReadableException)
			{
	        }
	    }
	    
		[Test]
	    public void TestWriteObject() 
		{
	        try
			{
	            ActiveMQStreamMessage message = new ActiveMQStreamMessage();
	            message.ClearBody();
	            message.WriteObject("test");
	            message.WriteObject((Char) 'a');
	            message.WriteObject((Boolean) false);
	            message.WriteObject((Byte) ((byte) 2));
	            message.WriteObject((Int16) ((short) 2));
	            message.WriteObject((Int32) 2);
	            message.WriteObject((Int64) 2L);
	            message.WriteObject((Single) 2.0f);
	            message.WriteObject((Double) 2.0);
	        }
			catch(Exception e) 
			{
	            Assert.Fail(e.Message);
	        }
	        try 
			{
	            ActiveMQStreamMessage message = new ActiveMQStreamMessage();
	            message.ClearBody();
	            message.WriteObject(new Object());
	            Assert.Fail("should throw an exception");
	        }
			catch(MessageFormatException) 
			{
	        }
			catch(Exception e) 
			{
	            Assert.Fail(e.Message);
	        }
	    }
	}
}
