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
using Apache.NMS.ActiveMQ.OpenWire;
using Apache.NMS.ActiveMQ.Commands;
using Apache.NMS.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class BaseDataStreamMarshalerTest
    {
        OpenWireFormat wireFormat;
        TestDataStreamMarshaller testMarshaller;

        [SetUp]
        public void SetUp()
        {
            wireFormat = new OpenWireFormat();
            testMarshaller = new TestDataStreamMarshaller();
        }

        [Test]
        public void TestCompactedLongToIntegerTightUnmarshal()
        {
            long inputValue = 2147483667;

            BooleanStream stream = new BooleanStream();
            stream.WriteBoolean(true);
            stream.WriteBoolean(false);

            stream.Clear();

            MemoryStream buffer = new MemoryStream();
            BinaryWriter ds = new EndianBinaryWriter(buffer);

            ds.Write((int) inputValue);

            MemoryStream ins = new MemoryStream(buffer.ToArray());
            BinaryReader dis = new EndianBinaryReader(ins);

            long outputValue = testMarshaller.TightUnmarshalLong(wireFormat, dis, stream);
            Assert.AreEqual(inputValue, outputValue);
        }

        [Test]
        public void TestCompactedLongToShortTightUnmarshal()
        {
            long inputValue = 33000;

            BooleanStream stream = new BooleanStream();
            stream.WriteBoolean(false);
            stream.WriteBoolean(true);

            stream.Clear();

            MemoryStream buffer = new MemoryStream();
            BinaryWriter ds = new EndianBinaryWriter(buffer);

            ds.Write((short) inputValue);

            MemoryStream ins = new MemoryStream(buffer.ToArray());
            BinaryReader dis = new EndianBinaryReader(ins);

            long outputValue = testMarshaller.TightUnmarshalLong(wireFormat, dis, stream);
            Assert.AreEqual(inputValue, outputValue);
        }

        private static BinaryReader ReaderFor(params byte[] bytes)
        {
            return new EndianBinaryReader(new FrameStream(new MemoryStream(bytes), bytes.Length));
        }

        // ---- Finding 1: ReadAsciiString with a negative (high-bit) 16-bit length ----

        [Test]
        public void TestReadAsciiStringRejectsNegativeLength()
        {
            // 0x8000 decodes to -32768 as a signed short; must not throw OverflowException
            // or allocate, but a clean IOException instead.
            BinaryReader dis = ReaderFor(0x80, 0x00);
            Assert.Throws<IOException>(delegate { testMarshaller.ReadAsciiStringPublic(dis); });
        }

        [Test]
        public void TestReadAsciiStringReadsValidValue()
        {
            BinaryReader dis = ReaderFor(0x00, 0x03, (byte) 'a', (byte) 'b', (byte) 'c');
            Assert.AreEqual("abc", testMarshaller.ReadAsciiStringPublic(dis));
        }

        // ---- Finding 2: ReadBytes with an unvalidated 32-bit size ----

        [Test]
        public void TestReadBytesRejectsHugeSize()
        {
            // 0x7FFFFFFF used to drive a ~2 GB allocation / OutOfMemoryException.
            BinaryReader dis = ReaderFor(0x7F, 0xFF, 0xFF, 0xFF);
            Assert.Throws<IOException>(delegate { testMarshaller.ReadBytesPublic(dis); });
        }

        [Test]
        public void TestReadBytesRejectsNegativeSize()
        {
            BinaryReader dis = ReaderFor(0xFF, 0xFF, 0xFF, 0xFF);
            Assert.Throws<IOException>(delegate { testMarshaller.ReadBytesPublic(dis); });
        }

        [Test]
        public void TestReadBytesReadsValidPayload()
        {
            BinaryReader dis = ReaderFor(0x00, 0x00, 0x00, 0x03, 0x0A, 0x0B, 0x0C);
            byte[] result = testMarshaller.ReadBytesPublic(dis);
            Assert.AreEqual(new byte[] { 0x0A, 0x0B, 0x0C }, result);
        }

        // ---- Frame-size bound carried by the reader ----

        [Test]
        public void TestUnmarshalRejectsOversizedFrame()
        {
            wireFormat.MaxFrameSize = 1024;
            BinaryReader dis = ReaderFor(0x00, 0x00, 0x10, 0x00, 0x01);
            Assert.Throws<IOException>(delegate { wireFormat.Unmarshal(dis); });
        }

        [Test]
        public void TestUnmarshalIgnoresMaxFrameSizeWhenDisabled()
        {
            // Frame larger than MaxFrameSize but the check is disabled: the frame must still
            // be accepted and unmarshalled in full.
            wireFormat.MaxFrameSize = 1;
            wireFormat.MaxFrameSizeEnabled = false;
            ActiveMQMessage message = new ActiveMQMessage();
            MemoryStream ms = new MemoryStream();
            wireFormat.Marshal(message, new EndianBinaryWriter(ms));

            BinaryReader dis = new EndianBinaryReader(new MemoryStream(ms.ToArray()));
            Assert.IsNotNull(wireFormat.Unmarshal(dis));
        }

        [Test]
        public void TestRenegotiateDoesNotChangeMaxFrameSizeEnabled()
        {
            wireFormat.MaxFrameSizeEnabled = false;
            WireFormatInfo info = new WireFormatInfo();
            info.Version = 10;
            info.MaxFrameSizeEnabled = true;
            wireFormat.RenegotiateWireFormat(info);
            Assert.IsFalse(wireFormat.MaxFrameSizeEnabled);
        }

        [Test]
        public void TestUnmarshalRejectsNegativeFrame()
        {
            BinaryReader dis = ReaderFor(0xFF, 0xFF, 0xFF, 0xFF, 0x01);
            Assert.Throws<IOException>(delegate { wireFormat.Unmarshal(dis); });
        }

        [Test]
        public void TestUnmarshalRejectsLengthLargerThanFrame()
        {
            // A legitimate command whose Content length is patched to claim far more bytes
            // than the (small) frame holds must be rejected instead of allocated.
            ActiveMQMessage message = new ActiveMQMessage();
            message.Content = new byte[] { 1, 2, 3, 4 };
            MemoryStream ms = new MemoryStream();
            wireFormat.Marshal(message, new EndianBinaryWriter(ms));
            byte[] frame = ms.ToArray();

            int at = -1;
            for (int i = 0; i + 8 <= frame.Length && at < 0; i++)
            {
                if (frame[i + 3] == 4 && frame[i + 4] == 1 && frame[i + 5] == 2 && frame[i] == 0 && frame[i + 1] == 0 && frame[i + 2] == 0)
                {
                    at = i;
                }
            }
            Assert.GreaterOrEqual(at, 0);
            frame[at] = 0x7F; frame[at + 1] = 0xFF; frame[at + 2] = 0xFF; frame[at + 3] = 0xFF;

            BinaryReader dis = new EndianBinaryReader(new MemoryStream(frame));
            Assert.Throws<IOException>(delegate { wireFormat.Unmarshal(dis); });
        }

        [Test]
        public void TestUnmarshalRoundTripsValidFrame()
        {
            ActiveMQMessage message = new ActiveMQMessage();
            message.Content = new byte[] { 1, 2, 3, 4 };
            MemoryStream ms = new MemoryStream();
            wireFormat.Marshal(message, new EndianBinaryWriter(ms));

            BinaryReader dis = new EndianBinaryReader(new MemoryStream(ms.ToArray()));
            ActiveMQMessage result = (ActiveMQMessage) wireFormat.Unmarshal(dis);
            Assert.AreEqual(message.Content, result.Content);
        }

        [Test]
        public void TestRenegotiateKeepsDefaultMaxFrameSizeWhenPeerAdvertisesNone()
        {
            WireFormatInfo info = new WireFormatInfo();
            info.Version = 10;
            Assert.AreEqual(0, info.MaxFrameSize);
            wireFormat.RenegotiateWireFormat(info);
            Assert.AreEqual(OpenWireFormat.DefaultMaxFrameSize, wireFormat.MaxFrameSize);

            ActiveMQMessage message = new ActiveMQMessage();
            message.Content = new byte[] { 1, 2, 3 };
            MemoryStream ms = new MemoryStream();
            wireFormat.Marshal(message, new EndianBinaryWriter(ms));
            Assert.IsNotNull(wireFormat.Unmarshal(new EndianBinaryReader(new MemoryStream(ms.ToArray()))));
        }

        [Test]
        public void TestMaxFrameSizeSetterIsAdvertised()
        {
            wireFormat.MaxFrameSize = 1024 * 1024;
            Assert.AreEqual(1024 * 1024, wireFormat.PreferredWireFormatInfo.MaxFrameSize);
        }

        [Test]
        public void TestRenegotiateTakesSmallerMaxFrameSize()
        {
            WireFormatInfo info = new WireFormatInfo();
            info.Version = 10;
            info.MaxFrameSize = 2048;
            wireFormat.RenegotiateWireFormat(info);
            Assert.AreEqual(2048, wireFormat.MaxFrameSize);
            Assert.AreEqual(2048, info.MaxFrameSize);

            wireFormat.MaxFrameSize = 1024;
            info.MaxFrameSize = 4096;
            wireFormat.RenegotiateWireFormat(info);
            Assert.AreEqual(1024, wireFormat.MaxFrameSize);
            Assert.AreEqual(1024, info.MaxFrameSize);
        }

        [Test]
        public void TestUnmarshalHonoursMaxFrameSizeSetOnPreferredInfo()
        {
            wireFormat.PreferredWireFormatInfo.MaxFrameSize = 1024;
            BinaryReader dis = ReaderFor(0x00, 0x00, 0x10, 0x00, 0x01);
            Assert.Throws<IOException>(delegate { wireFormat.Unmarshal(dis); });
        }

        [Test]
        public void TestMaxFrameSizeIsUnlimitedByDefault()
        {
            Assert.AreEqual(long.MaxValue, new OpenWireFormat().MaxFrameSize);
        }

        [Test]
        public void TestMaxFrameSizeEnabledIsAdvertisedInPreferredInfo()
        {
            wireFormat.MaxFrameSizeEnabled = false;
            Assert.IsFalse(wireFormat.PreferredWireFormatInfo.MaxFrameSizeEnabled);
        }

        [Test]
        public void TestUnmarshalSkipsUnreadTrailingBytesOfFrame()
        {
            ActiveMQMessage message = new ActiveMQMessage();
            MemoryStream ms = new MemoryStream();
            wireFormat.Marshal(message, new EndianBinaryWriter(ms));
            byte[] frame = ms.ToArray();

            // Append two trailing bytes to the first frame and bump its size prefix, then a second frame.
            int size = (frame[0] << 24) | (frame[1] << 16) | (frame[2] << 8) | frame[3];
            size += 2;
            MemoryStream two = new MemoryStream();
            two.Write(new byte[] { (byte) (size >> 24), (byte) (size >> 16), (byte) (size >> 8), (byte) size }, 0, 4);
            two.Write(frame, 4, frame.Length - 4);
            two.Write(new byte[] { 9, 9 }, 0, 2);
            two.Write(frame, 0, frame.Length);

            BinaryReader dis = new EndianBinaryReader(new MemoryStream(two.ToArray()));
            Assert.IsNotNull(wireFormat.Unmarshal(dis));
            Assert.IsNotNull(wireFormat.Unmarshal(dis));
        }

        [Test]
        public void TestUnmarshalFailureStillConsumesWholeFrame()
        {
            // Unknown command type 0x7E in a 4-byte frame, followed by a valid null frame.
            BinaryReader dis = ReaderFor(0, 0, 0, 4, 0x7E, 1, 2, 3, 0, 0, 0, 1, 0);
            Assert.Throws<IOException>(delegate { wireFormat.Unmarshal(dis); });
            Assert.IsNull(wireFormat.Unmarshal(dis));
        }

        [Test]
        public void TestReadBytesThrowsOnTruncatedFrame()
        {
            BinaryReader dis = new EndianBinaryReader(new MemoryStream(new byte[] { 0, 0, 0, 5, 1, 2 }));
            Assert.Throws<EndOfStreamException>(delegate { testMarshaller.ReadBytesPublic(dis); });
        }

        private class TestDataStreamMarshaller : BaseDataStreamMarshaller
        {
            public override DataStructure CreateObject()
            {
                return null;
            }

            public override byte GetDataStructureType()
            {
                return 0;
            }

            public String ReadAsciiStringPublic(BinaryReader dataIn)
            {
                return ReadAsciiString(dataIn);
            }

            public byte[] ReadBytesPublic(BinaryReader dataIn)
            {
                return ReadBytes(dataIn);
            }
        }
    }
}

