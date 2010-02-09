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
using System.Text;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.Transport.Stomp
{
    /// <summary>
    /// A Stream for writing a <a href="http://stomp.codehaus.org/">STOMP</a> Frame
    /// </summary>
    public class StompFrameStream
    {
        /// Used to terminate a header line or end of a headers section of the Frame.
        public const String NEWLINE = "\n";
        /// Used to seperate the Key / Value pairing in Frame Headers
        public const String SEPARATOR = ":";
        /// Used to mark the End of the Frame.
        public const byte FRAME_TERMINUS = (byte) 0;

        private StringBuilder builder = new StringBuilder();
        private BinaryWriter ds;
        private byte[] content;
        private int contentLength = -1;
        private Encoding encoding;

        public StompFrameStream(BinaryWriter ds, Encoding encoding)
        {
            this.ds = ds;
            this.encoding = encoding;
        }

        public byte[] Content
        {
            get { return content; }
            set { content = value; }
        }

        public int ContentLength
        {
            get { return contentLength; }
            set
            {
                contentLength = value;
                WriteHeader("content-length", contentLength);
            }
        }

        public void WriteCommand(Command command, String name)
        {
            WriteCommand(command, name, false);
        }

        public void WriteCommand(Command command, String name, bool ignoreErrors)
        {
            builder.Append(name);
            builder.Append(NEWLINE);
            if(command.ResponseRequired)
            {
                if(ignoreErrors)
                {
                    WriteHeader("receipt", "ignore:" + command.CommandId);
                }
                else
                {
                    WriteHeader("receipt", command.CommandId);
                }
            }
        }

        public void WriteHeader(String name, Object value)
        {
            if (value != null)
            {
                builder.Append(name);
                builder.Append(SEPARATOR);
                builder.Append(value);
                builder.Append(NEWLINE);
            }
        }

        public void WriteHeader(String name, bool value)
        {
            if (value)
            {
                builder.Append(name);
                builder.Append(SEPARATOR);
                builder.Append("true");
                builder.Append(NEWLINE);
            }
        }

        public void Flush()
        {
            builder.Append(NEWLINE);
            ds.Write(encoding.GetBytes(builder.ToString()));

            if (content != null)
            {
                ds.Write(content);
            }

            // Always write a terminating NULL byte to end the content frame.
            ds.Write(FRAME_TERMINUS);
        }
    }
}
