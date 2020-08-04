/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Util;
using NUnit.Framework;

namespace Apache.NMS.ActiveMQ.Test
{
    [TestFixture]
    public class LRUCacheTest
    {
        [Test]
        public void TestConstructor1()
        {
            LRUCache<Int64, Int64> underTest = new LRUCache<Int64, Int64>();
            Assert.AreEqual(LRUCache<Int64, Int64>.DEFAULT_MAX_CACHE_SIZE, underTest.MaxCacheSize);
        }

        [Test]
        public void TestConstructor2()
        {
            LRUCache<Int64, Int64> underTest = new LRUCache<Int64, Int64>(666);
            Assert.AreEqual(666, underTest.MaxCacheSize);
        }

        [Test]
        public void TestMaxCacheSize()
        {
            LRUCache<Int64, Int64> underTest = new LRUCache<Int64, Int64>(666);
            Assert.AreEqual(666, underTest.MaxCacheSize);
            underTest.MaxCacheSize = 512;
            Assert.AreEqual(512, underTest.MaxCacheSize);
        }

        [Test]
        public void TestAdd()
        {
            LRUCache<String, String> underTest = new LRUCache<String, String>(666);

            underTest.Add("key", "value");
            Assert.That(underTest.Count == 1);
        }

        [Test]
        public void TestRemove()
        {
            LRUCache<String, String> underTest = new LRUCache<String, String>(666);

            underTest.Add("key", "value");
            Assert.That(underTest.Count == 1);
            underTest.Remove("key");
            Assert.That(underTest.Count == 0);
        }

        [Test]
        public void TestContainsKey()
        {
            LRUCache<String, String> underTest = new LRUCache<String, String>(666);

            underTest.Add("key1", "value");
            underTest.Add("key2", "value");
            underTest.Add("key3", "value");
            Assert.That(underTest.Count == 3);
            Assert.That(underTest.ContainsKey("key2"));
            Assert.That(underTest.ContainsKey("key1"));
            Assert.That(underTest.ContainsKey("key3"));
            Assert.IsFalse(underTest.ContainsKey("Key4"));
        }

        [Test]
        public void TestContainsValue()
        {
            LRUCache<String, String> underTest = new LRUCache<String, String>(666);

            underTest.Add("key1", "value1");
            underTest.Add("key2", "value2");
            underTest.Add("key3", "value3");
            Assert.That(underTest.Count == 3);
            Assert.That(underTest.ContainsValue("value1"));
            Assert.That(underTest.ContainsValue("value2"));
            Assert.That(underTest.ContainsValue("value3"));
            Assert.IsFalse(underTest.ContainsValue("value4"));
        }

        [Test]
        public void TestCount()
        {
            LRUCache<String, String> underTest = new LRUCache<String, String>(666);

            Assert.That(underTest.Count == 0);
            underTest.Add("key1", "value");
            Assert.That(underTest.Count == 1);
            underTest.Add("key2", "value");
            Assert.That(underTest.Count == 2);
            underTest.Add("key3", "value");
            Assert.That(underTest.Count == 3);
            underTest.Clear();
            Assert.That(underTest.Count == 0);
        }

        [Test]
        public void TestClear()
        {
            LRUCache<String, String> underTest = new LRUCache<String, String>(666);

            underTest.Add("key1", "value");
            underTest.Add("key2", "value");
            underTest.Add("key3", "value");
            Assert.That(underTest.Count == 3);
            underTest.Clear();
            Assert.That(underTest.Count == 0);
        }

        [Test]
        public void TestResize()
        {
            LRUCache<Int64, Int64> underTest = new LRUCache<Int64, Int64>(1000);

            Int64 count = new Int64();
            long max = 0;
            for (; count < 27276827; count++)
            {
                long start = DateTime.Now.Ticks;
                if (!underTest.ContainsKey(count))
                {
                    underTest.Add(count, count);
                }
                long duration = DateTime.Now.Ticks - start;
                if (duration > max)
                {
                    Tracer.Debug("count: " + count + ", new max=" + duration);
                    max = duration;
                }
                if (count % 100000000 == 0)
                {
                    Tracer.Debug("count: " + count + ", max=" + max);
                }
            }

            Assert.AreEqual(1000, underTest.Count, "size is still in order");
        }
    }
}

