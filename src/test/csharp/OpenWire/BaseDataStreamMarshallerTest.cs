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
        }
    }
}

