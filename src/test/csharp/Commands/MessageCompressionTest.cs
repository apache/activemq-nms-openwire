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
using Apache.NMS;
using Apache.NMS.Test;
using Apache.NMS.ActiveMQ.Commands;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class MessageCompressionTest : NMSTestSupport
    {
        protected static string TEST_CLIENT_ID = "MessageCompressionTestClientId";
        protected static string DESTINATION_NAME = "TEST.MessageCompressionTestDest";
        
        // The following text should compress well
        private const string TEXT = "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                  + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                  + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                  + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                  + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                  + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                  + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                  + "The quick red fox jumped over the lazy brown dog. " + "The quick red fox jumped over the lazy brown dog. "
                                  + "The quick red fox jumped over the lazy brown dog. ";

        protected bool a = true;
        protected byte b = 123;
        protected char c = 'c';
        protected short d = 0x1234;
        protected int e = 0x12345678;
        protected long f = 0x1234567812345678;
        protected string g = "Hello World!";
        protected bool h = false;
        protected byte i = 0xFF;
        protected short j = -0x1234;
        protected int k = -0x12345678;
        protected long l = -0x1234567812345678;
        protected float m = 2.1F;
        protected double n = 2.3;
        
        [Test]
        public void TestTextMessageCompression()
        {
            using(Connection connection = CreateConnection(TEST_CLIENT_ID) as Connection)
            {
                connection.UseCompression = true;
                connection.Start();

                Assert.IsTrue(connection.UseCompression);

                using(ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                {
                    ITextMessage message = session.CreateTextMessage(TEXT);
                    
                    IDestination destination = session.CreateTemporaryQueue();
                    
                    IMessageProducer producer = session.CreateProducer(destination);
                    IMessageConsumer consumer = session.CreateConsumer(destination);

                    producer.Send(message);

                    message = consumer.Receive(TimeSpan.FromMilliseconds(6000)) as ITextMessage;
                    
                    Assert.IsNotNull(message);
                    Assert.IsTrue(((ActiveMQMessage) message).Compressed);
                    Assert.AreEqual(TEXT, message.Text);
                }
            }
        }

        [Test]
        public void TestObjectMessageCompression()
        {
            using(Connection connection = CreateConnection(TEST_CLIENT_ID) as Connection)
            {
                connection.UseCompression = true;
                connection.Start();

                Assert.IsTrue(connection.UseCompression);

                using(ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                {
                    IObjectMessage message = session.CreateObjectMessage(TEXT);

                    IDestination destination = session.CreateTemporaryQueue();
                    
                    IMessageProducer producer = session.CreateProducer(destination);
                    IMessageConsumer consumer = session.CreateConsumer(destination);

                    producer.Send(message);

                    message = consumer.Receive(TimeSpan.FromMilliseconds(6000)) as IObjectMessage;
                    
                    Assert.IsNotNull(message);
                    Assert.IsTrue(((ActiveMQMessage) message).Compressed);
                    Assert.AreEqual(TEXT, message.Body);
                }
            }
        }

        [Test]
        public void TestStreamMessageCompression()
        {
            using(Connection connection = CreateConnection(TEST_CLIENT_ID) as Connection)
            {
                connection.UseCompression = true;
                connection.Start();

                Assert.IsTrue(connection.UseCompression);

                using(ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                {
                    IStreamMessage message = session.CreateStreamMessage();
                    
                    IDestination destination = session.CreateTemporaryQueue();
                    
                    IMessageProducer producer = session.CreateProducer(destination);
                    IMessageConsumer consumer = session.CreateConsumer(destination);

                    message.WriteBoolean(a);
                    message.WriteByte(b);
                    message.WriteChar(c);
                    message.WriteInt16(d);
                    message.WriteInt32(e);
                    message.WriteInt64(f);
                    message.WriteString(g);
                    message.WriteBoolean(h);
                    message.WriteByte(i);
                    message.WriteInt16(j);
                    message.WriteInt32(k);
                    message.WriteInt64(l);
                    message.WriteSingle(m);
                    message.WriteDouble(n);
                    
                    producer.Send(message);

                    message = consumer.Receive(TimeSpan.FromMilliseconds(4000)) as IStreamMessage;
                    
                    Assert.IsNotNull(message);
                    Assert.IsTrue(((ActiveMQMessage) message).Compressed);

                    // use generic API to access entries
                    Assert.AreEqual(a, message.ReadBoolean(), "Stream Boolean Value: a");
                    Assert.AreEqual(b, message.ReadByte(), "Stream Byte Value: b");
                    Assert.AreEqual(c, message.ReadChar(), "Stream Char Value: c");
                    Assert.AreEqual(d, message.ReadInt16(), "Stream Int16 Value: d");
                    Assert.AreEqual(e, message.ReadInt32(), "Stream Int32 Value: e");
                    Assert.AreEqual(f, message.ReadInt64(), "Stream Int64 Value: f");
                    Assert.AreEqual(g, message.ReadString(), "Stream String Value: g");
                    Assert.AreEqual(h, message.ReadBoolean(), "Stream Boolean Value: h");
                    Assert.AreEqual(i, message.ReadByte(), "Stream Byte Value: i");
                    Assert.AreEqual(j, message.ReadInt16(), "Stream Int16 Value: j");
                    Assert.AreEqual(k, message.ReadInt32(), "Stream Int32 Value: k");
                    Assert.AreEqual(l, message.ReadInt64(), "Stream Int64 Value: l");
                    Assert.AreEqual(m, message.ReadSingle(), "Stream Single Value: m");
                    Assert.AreEqual(n, message.ReadDouble(), "Stream Double Value: n");                    
                }
            }
        }

        [Test]
        public void TestMapMessageCompression()
        {
            using(Connection connection = CreateConnection(TEST_CLIENT_ID) as Connection)
            {
                connection.UseCompression = true;
                connection.Start();

                Assert.IsTrue(connection.UseCompression);

                using(ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                {
                    IMapMessage message = session.CreateMapMessage();
                    
                    IDestination destination = session.CreateTemporaryQueue();
                    
                    IMessageProducer producer = session.CreateProducer(destination);
                    IMessageConsumer consumer = session.CreateConsumer(destination);

                    message.Body["a"] = a;
                    message.Body["b"] = b;
                    message.Body["c"] = c;
                    message.Body["d"] = d;
                    message.Body["e"] = e;
                    message.Body["f"] = f;
                    message.Body["g"] = g;
                    message.Body["h"] = h;
                    message.Body["i"] = i;
                    message.Body["j"] = j;
                    message.Body["k"] = k;
                    message.Body["l"] = l;
                    message.Body["m"] = m;
                    message.Body["n"] = n;
                    
                    producer.Send(message);

                    message = consumer.Receive(TimeSpan.FromMilliseconds(4000)) as IMapMessage;
                    
                    Assert.IsNotNull(message);
                    Assert.IsTrue(((ActiveMQMessage) message).Compressed);

                    Assert.AreEqual(a, message.Body.GetBool("a"), "map entry: a");
                    Assert.AreEqual(b, message.Body.GetByte("b"), "map entry: b");
                    Assert.AreEqual(c, message.Body.GetChar("c"), "map entry: c");
                    Assert.AreEqual(d, message.Body.GetShort("d"), "map entry: d");
                    Assert.AreEqual(e, message.Body.GetInt("e"), "map entry: e");
                    Assert.AreEqual(f, message.Body.GetLong("f"), "map entry: f");
                    Assert.AreEqual(g, message.Body.GetString("g"), "map entry: g");
                    Assert.AreEqual(h, message.Body.GetBool("h"), "map entry: h");
                    Assert.AreEqual(i, message.Body.GetByte("i"), "map entry: i");
                    Assert.AreEqual(j, message.Body.GetShort("j"), "map entry: j");
                    Assert.AreEqual(k, message.Body.GetInt("k"), "map entry: k");
                    Assert.AreEqual(l, message.Body.GetLong("l"), "map entry: l");
                    Assert.AreEqual(m, message.Body.GetFloat("m"), "map entry: m");
                    Assert.AreEqual(n, message.Body.GetDouble("n"), "map entry: n");             
                }
            }
        }

        [Test]
        public void TestBytesMessageCompression()
        {
            using(Connection connection = CreateConnection(TEST_CLIENT_ID) as Connection)
            {
                connection.UseCompression = true;
                connection.Start();
                using(ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                {
                    IDestination destination = session.CreateTemporaryQueue();
                    using(IMessageConsumer consumer = session.CreateConsumer(destination))
                    using(IMessageProducer producer = session.CreateProducer(destination))
                    {
                        IBytesMessage message = session.CreateBytesMessage();
                        
                        message.WriteBoolean(a);
                        message.WriteByte(b);
                        message.WriteChar(c);
                        message.WriteInt16(d);
                        message.WriteInt32(e);
                        message.WriteInt64(f);
                        message.WriteString(g);
                        message.WriteBoolean(h);
                        message.WriteByte(i);
                        message.WriteInt16(j);
                        message.WriteInt32(k);
                        message.WriteInt64(l);
                        message.WriteSingle(m);
                        message.WriteDouble(n);
                        
                        producer.Send(message);

                        message = consumer.Receive(receiveTimeout) as IBytesMessage;

                        Assert.IsNotNull(message);
                        Assert.IsTrue(((ActiveMQMessage) message).Compressed);

                        Assert.AreEqual(a, message.ReadBoolean(), "Stream Boolean Value: a");
                        Assert.AreEqual(b, message.ReadByte(), "Stream Byte Value: b");
                        Assert.AreEqual(c, message.ReadChar(), "Stream Char Value: c");
                        Assert.AreEqual(d, message.ReadInt16(), "Stream Int16 Value: d");
                        Assert.AreEqual(e, message.ReadInt32(), "Stream Int32 Value: e");
                        Assert.AreEqual(f, message.ReadInt64(), "Stream Int64 Value: f");
                        Assert.AreEqual(g, message.ReadString(), "Stream String Value: g");
                        Assert.AreEqual(h, message.ReadBoolean(), "Stream Boolean Value: h");
                        Assert.AreEqual(i, message.ReadByte(), "Stream Byte Value: i");
                        Assert.AreEqual(j, message.ReadInt16(), "Stream Int16 Value: j");
                        Assert.AreEqual(k, message.ReadInt32(), "Stream Int32 Value: k");
                        Assert.AreEqual(l, message.ReadInt64(), "Stream Int64 Value: l");
                        Assert.AreEqual(m, message.ReadSingle(), "Stream Single Value: m");
                        Assert.AreEqual(n, message.ReadDouble(), "Stream Double Value: n");                           
                    }
                }
            }
        }
        
    }
}
