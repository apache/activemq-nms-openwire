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
using Apache.NMS;

namespace Apache.NMS.ActiveMQ.Transport.Stomp
{
	/// <summary>
	/// Some <a href="http://stomp.codehaus.org/">STOMP</a> protocol conversion helper methods.
	/// </summary>
	public class StompHelper
	{
		private static int ParseInt(string text)
		{
			StringBuilder sbtext = new StringBuilder();

			for(int idx = 0; idx < text.Length; idx++)
			{
				if(char.IsNumber(text, idx))
				{
					sbtext.Append(text[idx]);
				}
				else
				{
					break;
				}
			}
			
			if(sbtext.Length > 0)
			{
				return Int32.Parse(sbtext.ToString());
			}
			
			return 0;
		}

		public static ActiveMQDestination ToDestination(string text)
		{
			if(text == null)
			{
				return null;
			}
			
			int type = ActiveMQDestination.ACTIVEMQ_QUEUE;
			string lowertext = text.ToLower();
			if(lowertext.StartsWith("/queue/"))
			{
				text = text.Substring("/queue/".Length);
			}
			else if(lowertext.StartsWith("/topic/"))
			{
				text = text.Substring("/topic/".Length);
				type = ActiveMQDestination.ACTIVEMQ_TOPIC;
			}
			else if(lowertext.StartsWith("/temp-topic/"))
			{
				text = text.Substring("/temp-topic/".Length);
				type = ActiveMQDestination.ACTIVEMQ_TEMPORARY_TOPIC;
			}
			else if(lowertext.StartsWith("/temp-queue/"))
			{
				text = text.Substring("/temp-queue/".Length);
				type = ActiveMQDestination.ACTIVEMQ_TEMPORARY_QUEUE;
			}
			else if(lowertext.StartsWith("/remote-temp-topic/"))
			{
				type = ActiveMQDestination.ACTIVEMQ_TEMPORARY_TOPIC;
			}
			else if(lowertext.StartsWith("/remote-temp-queue/"))
			{
				type = ActiveMQDestination.ACTIVEMQ_TEMPORARY_QUEUE;
			}
			
			return ActiveMQDestination.CreateDestination(type, text);
		}

		public static string ToStomp(ActiveMQDestination destination)
		{
			if(destination == null)
			{
				return null;
			}

			switch (destination.DestinationType)
			{
				case DestinationType.Topic:
					return "/topic/" + destination.PhysicalName;

				case DestinationType.TemporaryTopic:
					if (destination.PhysicalName.ToLower().StartsWith("/remote-temp-topic/"))
					{
						return destination.PhysicalName;
					}

					return "/temp-topic/" + destination.PhysicalName;

				case DestinationType.TemporaryQueue:
					if (destination.PhysicalName.ToLower().StartsWith("/remote-temp-queue/"))
					{
						return destination.PhysicalName;
					}

					return "/temp-queue/" + destination.PhysicalName;

				default:
					return "/queue/" + destination.PhysicalName;
			}
		}

		public static string ToStomp(ConsumerId id)
		{
			return id.ConnectionId + ":" + id.SessionId + ":" + id.Value;
		}

		public static ConsumerId ToConsumerId(string text)
		{
			if(text == null)
			{
				return null;
			}

			ConsumerId answer = new ConsumerId();
			int idx = text.LastIndexOf(':');
			if (idx >= 0)
			{
				try
				{
					answer.Value = ParseInt(text.Substring(idx + 1));
					text = text.Substring(0, idx);
					idx = text.LastIndexOf(':');
					if (idx >= 0)
					{
						try
						{
							answer.SessionId = ParseInt(text.Substring(idx + 1));
							text = text.Substring(0, idx);
						}
						catch(Exception ex)
						{
							Tracer.Debug(ex.Message);
						}
					}
				}
				catch(Exception ex)
				{
					Tracer.Debug(ex.Message);
				}
			}
			answer.ConnectionId = text;
			return answer;
		}

		public static string ToStomp(ProducerId id)
		{
			StringBuilder producerBuilder = new StringBuilder();

			producerBuilder.Append(id.ConnectionId);
			producerBuilder.Append(":");
			producerBuilder.Append(id.SessionId);
			producerBuilder.Append(":");
			producerBuilder.Append(id.Value);

			return producerBuilder.ToString();
		}
		
		public static ProducerId ToProducerId(string text)
		{
			if(text == null)
			{
				return null;
			}

			ProducerId answer = new ProducerId();
			int idx = text.LastIndexOf(':');
			if(idx >= 0)
			{
				try
				{
					answer.Value = ParseInt(text.Substring(idx + 1));
					text = text.Substring(0, idx);
					idx = text.LastIndexOf(':');
					if (idx >= 0)
					{
						answer.SessionId = ParseInt(text.Substring(idx + 1));
						text = text.Substring(0, idx);
					}
				}
				catch(Exception ex)
				{
					Tracer.Debug(ex.Message);
				}
			}
			answer.ConnectionId = text;
			return answer;
		}

		public static string ToStomp(MessageId id)
		{
			StringBuilder messageBuilder = new StringBuilder();

			messageBuilder.Append(ToStomp(id.ProducerId));
			messageBuilder.Append(":");
			messageBuilder.Append(id.ProducerSequenceId);

			return messageBuilder.ToString();
		}

		public static MessageId ToMessageId(string text)
		{
			if(text == null)
			{
				return null;
			}
			
			MessageId answer = new MessageId();
			int idx = text.LastIndexOf(':');
			if (idx >= 0)
			{
				try
				{
					answer.ProducerSequenceId = ParseInt(text.Substring(idx + 1));
					text = text.Substring(0, idx);
				}
				catch(Exception ex)
				{
					Tracer.Debug(ex.Message);
				}
			}
			answer.ProducerId = ToProducerId(text);
			return answer;
		}

		public static string ToStomp(TransactionId id)
		{
			if(id is LocalTransactionId)
			{
				return ToStomp(id as LocalTransactionId);
			}
			
			return id.ToString();
		}

		public static string ToStomp(LocalTransactionId transactionId)
		{
			return transactionId.ConnectionId.Value + ":" + transactionId.Value;
		}

		public static bool ToBool(string text, bool defaultValue)
		{
			if(text == null)
			{
				return defaultValue;
			}

			return (0 == string.Compare("true", text, true));
		}
	}
}
