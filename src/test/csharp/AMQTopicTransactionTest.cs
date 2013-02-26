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
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class AMQTopicTransactionTest : AMQTransactionTestSupport
    {
        public const String CLIENT_ID = "TopicTransactionTest";
        public const String DESTINATION_NAME = "TEST.AMQTopicTransactionTestDestination";
        public const String SUBSCRIPTION_NAME = "TopicTransactionTest";

        protected override bool Topic
        {
            get { return true; }
        }

        protected override String TestClientId
        {
            get { return CLIENT_ID; }
        }

        protected override String Subscription
        {
            get { return SUBSCRIPTION_NAME; }
        }

        protected override String DestinationName
        {
            get { return DESTINATION_NAME; }
        }

    }
}
