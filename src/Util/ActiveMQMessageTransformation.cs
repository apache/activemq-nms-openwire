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

using System.Collections;
using Apache.NMS.Util;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.Util
{
    public class ActiveMQMessageTransformation : MessageTransformation
    {
        private readonly Connection connection;

        public ActiveMQMessageTransformation(Connection connection) : base()
        {
            this.connection = connection;
        }

        #region Creation Methods and Conversion Support Methods

        protected override IMessage DoCreateMessage()
        {
            ActiveMQMessage message = new ActiveMQMessage();
            message.Connection = this.connection;
            return message;
        }

        protected override IBytesMessage DoCreateBytesMessage()
        {
            ActiveMQBytesMessage message = new ActiveMQBytesMessage();
            message.Connection = this.connection;
            return message;
        }

        protected override ITextMessage DoCreateTextMessage()
        {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.Connection = this.connection;
            return message;
        }

        protected override IStreamMessage DoCreateStreamMessage()
        {
            ActiveMQStreamMessage message = new ActiveMQStreamMessage();
            message.Connection = this.connection;
            return message;
        }

        protected override IMapMessage DoCreateMapMessage()
        {
            ActiveMQMapMessage message = new ActiveMQMapMessage();
            message.Connection = this.connection;
            return message;
        }

        protected override IObjectMessage DoCreateObjectMessage()
        {
            ActiveMQObjectMessage message = new ActiveMQObjectMessage();
            message.Connection = this.connection;
            return message;
        }

        protected override IDestination DoTransformDestination(IDestination destination)
        {
            return ActiveMQDestination.Transform(destination);
        }

        protected override void DoPostProcessMessage(IMessage message)
        {
        }

        #endregion
        
        
        public static void CopyMap(IPrimitiveMap source, IPrimitiveMap target)
        {
            foreach (object key in source.Keys)
            {
                string name = key.ToString();
                object value = source[name];

                switch (value)
                {
                    case bool boolValue:
                        target.SetBool(name, boolValue);
                        break;
                    case char charValue:
                        target.SetChar(name, charValue);
                        break;
                    case string stringValue:
                        target.SetString(name, stringValue);
                        break;
                    case byte byteValue:
                        target.SetByte(name, byteValue);
                        break;
                    case short shortValue:
                        target.SetShort(name, shortValue);
                        break;
                    case int intValue:
                        target.SetInt(name, intValue);
                        break;
                    case long longValue:
                        target.SetLong(name, longValue);
                        break;
                    case float floatValue:
                        target.SetFloat(name, floatValue);
                        break;
                    case double doubleValue:
                        target.SetDouble(name, doubleValue);
                        break;
                    case byte[] bytesValue:
                        target.SetBytes(name, bytesValue);
                        break;
                    case IList listValue:
                        target.SetList(name, listValue);
                        break;
                    case IDictionary dictionaryValue:
                        target.SetDictionary(name, dictionaryValue);
                        break;
                    case object objectValue:
                        if (target is IPrimitiveMap primitiveMapBase)
                        {
                            primitiveMapBase[name] = objectValue;
                        }
                        break;
                }
            }
        }
    }
}

