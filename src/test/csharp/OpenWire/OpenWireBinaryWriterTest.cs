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
using System.Collections.Generic;
using System.Text;
using System.IO;
using Apache.NMS.ActiveMQ.OpenWire;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test.OpenWire
{
    [TestFixture]
    public class OpenWireBinaryWriterTest
    {
        void writeString16TestHelper(char[] input, byte[] expect)
        {
            MemoryStream stream = new MemoryStream();
            OpenWireBinaryWriter writer = new OpenWireBinaryWriter(stream);

            String str = new String(input);

            writer.WriteString16(str);

            byte[] result = stream.GetBuffer();

            Assert.AreEqual(result[0], 0x00);
            Assert.AreEqual(result[1], expect.Length);

            for (int i = 4; i < expect.Length; ++i)
            {
                Assert.AreEqual(result[i], expect[i - 2]);
            }
        }
        [Test]
        public void testWriteString16()
        {
            // Test data with 1-byte UTF8 encoding.
            {
                char[] input = { '\u0000', '\u000B', '\u0048', '\u0065', '\u006C', '\u006C', '\u006F', '\u0020', '\u0057', '\u006F', '\u0072', '\u006C', '\u0064' };
                byte[] expect = { 0xC0, 0x80, 0x0B, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64 };

                writeString16TestHelper(input, expect);
            }

            // Test data with 2-byte UT8 encoding.
            {
                char[] input = { '\u0000', '\u00C2', '\u00A9', '\u00C3', '\u00A6' };
                byte[] expect = { 0xC0, 0x80, 0xC3, 0x82, 0xC2, 0xA9, 0xC3, 0x83, 0xC2, 0xA6 };

                writeString16TestHelper(input, expect);
            }

            // Test data with 1-byte and 2-byte encoding with embedded NULL's.
            {
                char[] input = { '\u0000', '\u0004', '\u00C2', '\u00A9', '\u00C3', '\u0000', '\u00A6' };
                byte[] expect = { 0xC0, 0x80, 0x04, 0xC3, 0x82, 0xC2, 0xA9, 0xC3, 0x83, 0xC0, 0x80, 0xC2, 0xA6 };

                writeString16TestHelper(input, expect);
            }

            // test that a null string writes no output.
            {
                MemoryStream stream = new MemoryStream();
                OpenWireBinaryWriter writer = new OpenWireBinaryWriter(stream);
                writer.WriteString16(null);
                Assert.AreEqual(0, stream.Length);
            }

            // test that a null string writes no output.
            {
                MemoryStream stream = new MemoryStream();
                OpenWireBinaryWriter writer = new OpenWireBinaryWriter(stream);
                writer.WriteString16("");

                stream.Seek(0, SeekOrigin.Begin);
                OpenWireBinaryReader reader = new OpenWireBinaryReader(stream);
                Assert.AreEqual(0, reader.ReadInt16());
            }

            // String of length 65536 of Null Characters.
            {
                MemoryStream stream = new MemoryStream();
                OpenWireBinaryWriter writer = new OpenWireBinaryWriter(stream);
                String testStr = new String( 'a', 65536 );
                try{
                    writer.Write(testStr);
                    Assert.Fail( "Should throw an Exception" );
                } 
                catch( Exception ) 
                {
                }
            }

            // String of length 65535 of non Null Characters since Null encodes as UTF-8.
            {
                MemoryStream stream = new MemoryStream();
                OpenWireBinaryWriter writer = new OpenWireBinaryWriter(stream);
                String testStr = new String( 'a', 65535 );
                try{
                    writer.Write(testStr);
                } 
                catch( Exception ) 
                {
                    Assert.Fail( "Should not throw an Exception" );
                }
            }

            // Set one of the 65535 bytes to a value that will result in a 2 byte UTF8 encoded sequence.
            // This will cause the string of length 65535 to have a utf length of 65536.
            {
                MemoryStream stream = new MemoryStream();
                OpenWireBinaryWriter writer = new OpenWireBinaryWriter(stream);
                String testStr = new String( 'a', 65535 );
                char[] array = testStr.ToCharArray();
                array[0] = '\u0000';
                testStr = new String(array);
                
                try{
                    writer.Write(testStr);
                    Assert.Fail( "Should throw an Exception" );
                } 
                catch( Exception ) 
                {
                }
            }
           
        }

        void writeString32TestHelper(char[] input, byte[] expect) {

            MemoryStream stream = new MemoryStream();
            OpenWireBinaryWriter writer = new OpenWireBinaryWriter(stream);

            String str = new String(input);
            
            writer.WriteString32( str );

            byte[] result = stream.GetBuffer();

            Assert.AreEqual( result[0], 0x00 );
            Assert.AreEqual( result[1], 0x00 );
            Assert.AreEqual( result[2], 0x00 );
            Assert.AreEqual( result[3], expect.Length );

            for (int i = 4; i < expect.Length; ++i)
            {
                Assert.AreEqual( result[i], expect[i-4] );
            }
        }

        [Test]
        public void testWriteString32()
        {
            // Test data with 1-byte UTF8 encoding.
            {
                char[] input = { '\u0000', '\u000B', '\u0048', '\u0065', '\u006C', '\u006C', '\u006F', '\u0020', '\u0057', '\u006F', '\u0072', '\u006C', '\u0064'};
                byte[] expect = {0xC0, 0x80, 0x0B, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64};

                writeString32TestHelper(input, expect);
            }

            // Test data with 2-byte UT8 encoding.
            {
                char[] input = { '\u0000', '\u00C2', '\u00A9', '\u00C3', '\u00A6' };
                byte[] expect = { 0xC0, 0x80, 0xC3, 0x82, 0xC2, 0xA9, 0xC3, 0x83, 0xC2, 0xA6 };

                writeString32TestHelper(input, expect);
            }

            // Test data with 1-byte and 2-byte encoding with embedded NULL's.
            {
                char[] input = { '\u0000', '\u0004', '\u00C2', '\u00A9', '\u00C3', '\u0000', '\u00A6' };
                byte[] expect = { 0xC0, 0x80, 0x04, 0xC3, 0x82, 0xC2, 0xA9, 0xC3, 0x83, 0xC0, 0x80, 0xC2, 0xA6 };

                writeString32TestHelper(input, expect);
            }

            // test that a null strings writes a -1
            {
                MemoryStream stream = new MemoryStream();
                OpenWireBinaryWriter writer = new OpenWireBinaryWriter(stream);
                writer.WriteString32(null);

                stream.Seek(0, SeekOrigin.Begin);
                OpenWireBinaryReader reader = new OpenWireBinaryReader(stream);
                Assert.AreEqual(-1, reader.ReadInt32());
            }
        }

    }
}
