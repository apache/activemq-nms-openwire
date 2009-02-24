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
using System.IO;

namespace Apache.NMS.ActiveMQ.OpenWire
{
	/// <summary>
	/// A BinaryWriter that switches the endian orientation of the write opperations so that they
	/// are compatible with marshalling used by OpenWire.
	/// </summary>
	[CLSCompliant(false)]
	public class OpenWireBinaryWriter : BinaryWriter
	{
		public const int MAXSTRINGLEN = short.MaxValue;

		public OpenWireBinaryWriter(Stream output)
			: base(output)
		{
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="value">A  long</param>
		public override void Write(long value)
		{
			base.Write(EndianSupport.SwitchEndian(value));
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="value">An ushort</param>
		public override void Write(ushort value)
		{
			base.Write(EndianSupport.SwitchEndian(value));
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="value">An int</param>
		public override void Write(int value)
		{
			int x = EndianSupport.SwitchEndian(value);
			base.Write(x);
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="chars">A  char[]</param>
		/// <param name="index">An int</param>
		/// <param name="count">An int</param>
		public override void Write(char[] chars, int index, int count)
		{
			char[] t = new char[count];
			for(int i = 0; i < count; i++)
			{
				t[index + i] = EndianSupport.SwitchEndian(t[index + i]);
			}
			base.Write(t);
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="chars">A  char[]</param>
		public override void Write(char[] chars)
		{
			Write(chars, 0, chars.Length);
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="value">An uint</param>
		public override void Write(uint value)
		{
			base.Write(EndianSupport.SwitchEndian(value));
		}


		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="ch">A  char</param>
		public override void Write(char ch)
		{
			base.Write((byte) ((ch >> 8) & 0xFF));
			base.Write((byte) (ch & 0xFF));
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="value">An ulong</param>
		public override void Write(ulong value)
		{
			base.Write(EndianSupport.SwitchEndian(value));
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="value">A  short</param>
		public override void Write(short value)
		{
			base.Write(EndianSupport.SwitchEndian(value));
		}

		public override void Write(String text)
		{
			foreach(string textPackage in new StringPackageSplitter(text))
			{
				WriteString(textPackage);
			}
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="text">A  string</param>
		private void WriteString(String text)
		{
			if(text != null)
			{
				if(text.Length > OpenWireBinaryWriter.MAXSTRINGLEN)
				{
					throw new IOException(String.Format("Cannot marshall string longer than: {0} characters, supplied string was: {1} characters", OpenWireBinaryWriter.MAXSTRINGLEN, text.Length));
				}

				int strlen = text.Length;
				short utflen = 0;
				int c = 0;
				int count = 0;

				char[] charr = text.ToCharArray();

				for(int i = 0; i < strlen; i++)
				{
					c = charr[i];
					if((c >= 0x0001) && (c <= 0x007F))
					{
						utflen++;
					}
					else if(c > 0x07FF)
					{
						utflen += 3;
					}
					else
					{
						utflen += 2;
					}
				}

				Write((short) utflen);

				byte[] bytearr = new byte[utflen];
				for(int i = 0; i < strlen; i++)
				{
					c = charr[i];
					if((c >= 0x0001) && (c <= 0x007F))
					{
						bytearr[count++] = (byte) c;
					}
					else if(c > 0x07FF)
					{
						bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
						bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
						bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
					}
					else
					{
						bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
						bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
					}
				}

				Write(bytearr);

			}
			else
			{
				Write((short) -1);
			}
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="value">A  double</param>
		public override void Write(float value)
		{
			base.Write(EndianSupport.SwitchEndian(value));
		}

		/// <summary>
		/// Method Write
		/// </summary>
		/// <param name="value">A  double</param>
		public override void Write(double value)
		{
			base.Write(EndianSupport.SwitchEndian(value));
		}
	}

	#region StringPackageSplitter

	/// <summary>
	/// StringPackageSplitter
	/// </summary>
	class StringPackageSplitter : IEnumerable
	{
		public StringPackageSplitter(string value)
		{
			this.value = value;
		}

		/// <summary>
		/// Emumerator class for StringPackageSplitter
		/// </summary>
		class StringPackageSplitterEnumerator : IEnumerator
		{
			/// <summary>
			/// </summary>
			/// <param name="parent"></param>
			public StringPackageSplitterEnumerator(StringPackageSplitter parent)
			{
				this.parent = parent;
			}

			private int Position = -1;
			private StringPackageSplitter parent;

			#region IEnumerator Members

			public string Current
			{
				get
				{
					int delta = parent.value.Length - Position;

					if(delta >= OpenWireBinaryWriter.MAXSTRINGLEN)
					{
						return parent.value.Substring(Position, OpenWireBinaryWriter.MAXSTRINGLEN);
					}
					else
					{
						return parent.value.Substring(Position, delta);
					}
				}
			}

			#endregion

			#region IDisposable Members

			public void Dispose()
			{
			}

			#endregion

			#region IEnumerator Members

			object IEnumerator.Current
			{
				get
                {
                    int delta;

                    delta = parent.value.Length - Position;

                    if (delta >= OpenWireBinaryWriter.MAXSTRINGLEN)
                    {
                        return parent.value.Substring(Position, OpenWireBinaryWriter.MAXSTRINGLEN);
                    }
                    else
                    {
                        return parent.value.Substring(Position, delta);
                    }
                }
			}

			public bool MoveNext()
			{
				if(parent.value == null)
				{
					return false;
				}

				if(Position == -1)
				{
					Position = 0;
					return true;
				}

				if((Position + OpenWireBinaryWriter.MAXSTRINGLEN) < parent.value.Length)
				{
					Position += OpenWireBinaryWriter.MAXSTRINGLEN;
					return true;
				}
				else
				{
					return false;
				}
			}

			public void Reset()
			{
				Position = -1;
			}

			#endregion
		}

		private String value;

		#region IEnumerable Members

		IEnumerator IEnumerable.GetEnumerator()
		{
			return new StringPackageSplitterEnumerator(this);
		}

		#endregion
	}

	#endregion // END StringPackageSplitter
}

