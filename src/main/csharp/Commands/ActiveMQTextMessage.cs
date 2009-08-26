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
using Apache.NMS.Util;

namespace Apache.NMS.ActiveMQ.Commands
{
	public class ActiveMQTextMessage : ActiveMQMessage, ITextMessage
	{
		public const byte ID_ACTIVEMQTEXTMESSAGE = 28;

		private String text;

		public ActiveMQTextMessage()
		{
		}

		public ActiveMQTextMessage(String text)
		{
			this.Text = text;
		}

		// TODO generate Equals method
		// TODO generate GetHashCode method
		// TODO generate ToString method

		public override string ToString()
		{
			return base.ToString() + " Text=" + Text;
		}

		public override byte GetDataStructureType()
		{
			return ID_ACTIVEMQTEXTMESSAGE;
		}

		// Properties

		public string Text
		{
			get
			{
				if(text == null)
				{
					// now lets read the content
					byte[] data = this.Content;
					if(data != null)
					{
						MemoryStream stream = new MemoryStream(data);
						EndianBinaryReader reader = new EndianBinaryReader(stream);
						text = reader.ReadString32();
					}
				}
				return text;
			}

			set
			{
				this.text = value;
				byte[] data = null;
				if(text != null)
				{
					// TODO lets make the evaluation of the Content lazy!

					// Set initial size to the size of the string the UTF-8 encode could
					// result in more if there are chars that encode to multibye values.
					MemoryStream stream = new MemoryStream(text.Length);
					EndianBinaryWriter writer = new EndianBinaryWriter(stream);
					writer.WriteString32(text);
					data = stream.GetBuffer();
				}
				this.Content = data;

			}
		}
	}
}

