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

namespace Apache.NMS.ActiveMQ.OpenWire
{
    /// <summary>
    /// A read-only view over the bytes of a single OpenWire frame.  It never hands out more
    /// than the frame size announced by the peer and reports how many bytes are still
    /// unread, so the marshallers can reject a length or count that claims more data than
    /// the frame can possibly hold before allocating anything for it.
    /// </summary>
    public sealed class FrameStream : Stream
    {
        private readonly Stream inner;
        private long remaining;

        public FrameStream(Stream inner, long frameSize)
        {
            this.inner = inner;
            this.remaining = frameSize;
        }

        /// <summary>
        /// Number of bytes of the frame that have not been consumed yet.
        /// </summary>
        public long Remaining
        {
            get { return remaining; }
        }

        /// <summary>
        /// Discards whatever is left of the frame so the underlying stream is positioned at
        /// the start of the next one, even if the unmarshaller consumed fewer bytes than the
        /// peer announced (for example a newer command with trailing fields).
        /// </summary>
        public void SkipRemaining()
        {
            if (remaining <= 0)
            {
                return;
            }

            byte[] scratch = new byte[(int) Math.Min(remaining, 4096)];
            while (remaining > 0)
            {
                int read = Read(scratch, 0, scratch.Length);
                if (read <= 0)
                {
                    throw new EndOfStreamException("Unexpected end of stream skipping the rest of a frame.");
                }
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (remaining <= 0 || count <= 0)
            {
                return 0;
            }

            int read = inner.Read(buffer, offset, (int) Math.Min(count, remaining));
            remaining -= read;
            return read;
        }

        public override int ReadByte()
        {
            if (remaining <= 0)
            {
                return -1;
            }

            int value = inner.ReadByte();
            if (value >= 0)
            {
                remaining--;
            }

            return value;
        }

        public override bool CanRead { get { return true; } }
        public override bool CanSeek { get { return false; } }
        public override bool CanWrite { get { return false; } }

        public override long Length { get { throw new NotSupportedException(); } }

        public override long Position
        {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }

        public override void Flush() { }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}
