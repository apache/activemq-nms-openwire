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
using Apache.NMS.Policies;

namespace Apache.NMS.ActiveMQ
{
    public class RedeliveryPolicy : IRedeliveryPolicy
    {
        private int maximumRedeliveries = 6;
        private int initialRedeliveryDelay = 1000;
        private int backOffMultiplier = 5;
        private int collisionAvoidancePercent = 15;
        private bool useExponentialBackOff = false;
        private bool useCollisionAvoidance = false;

        public int MaximumRedeliveries
        {
            get { return maximumRedeliveries; }
            set { maximumRedeliveries = value; }
        }

        public int InitialRedeliveryDelay
        {
            get { return initialRedeliveryDelay; }
            set { initialRedeliveryDelay = value; }
        }

        public bool UseExponentialBackOff
        {
            get { return useExponentialBackOff; }
            set { useExponentialBackOff = value; }
        }

        public int BackOffMultiplier
        {
            get { return backOffMultiplier; }
            set { backOffMultiplier = value; }
        }

        public bool UseCollisionAvoidance
        {
            get { return useCollisionAvoidance; }
            set { useCollisionAvoidance = value; }
        }

        public int CollisionAvoidancePercent
        {
            get { return collisionAvoidancePercent; }
            set { collisionAvoidancePercent = value; }
        }

        public int RedeliveryDelay(int redeliveryCount)
        {
            int delay = redeliveryCount == 0 ? initialRedeliveryDelay : initialRedeliveryDelay;

            if (useExponentialBackOff && redeliveryCount > 0)
            {
                delay = (int)(delay * Math.Pow(backOffMultiplier, redeliveryCount - 1));
            }

            if (useCollisionAvoidance)
            {
                Random random = new Random();
                double variance = (random.NextDouble() * 2.0 - 1.0) * (collisionAvoidancePercent / 100.0);
                delay += (int)(delay * variance);
            }

            return delay;
        }

        public object Clone()
        {
            return new RedeliveryPolicy
            {
                maximumRedeliveries = this.maximumRedeliveries,
                initialRedeliveryDelay = this.initialRedeliveryDelay,
                backOffMultiplier = this.backOffMultiplier,
                collisionAvoidancePercent = this.collisionAvoidancePercent,
                useExponentialBackOff = this.useExponentialBackOff,
                useCollisionAvoidance = this.useCollisionAvoidance
            };
        }

        public int GetOutcome(IDestination destination)
        {
            // Default implementation always returns 0 (redeliver)
            return 0;
        }
    }
}