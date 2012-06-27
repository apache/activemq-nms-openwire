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
    public class BitArrayBinTest
    {
        [Test]
        public void TestSetAroundWindow()
        {
            DoTestSetAroundWindow(500, 2000);
            DoTestSetAroundWindow(512, 2000);
            DoTestSetAroundWindow(128, 512);
        }

        [Test]
        public void TestSetHiLo()
        {
            BitArrayBin toTest = new BitArrayBin(50);
            toTest.SetBit(0, true);
            toTest.SetBit(100, true);
            toTest.SetBit(150, true);
            Assert.IsTrue(toTest.GetBit(0));

            toTest.SetBit(0, true);
            Assert.IsTrue(toTest.GetBit(0));
        }
        
        private void DoTestSetAroundWindow(int window, int dataSize)
        {
            BitArrayBin toTest = new BitArrayBin(window);
            
            for (int ix=0; ix <= dataSize; ix++)
            {
                Assert.IsTrue(!toTest.SetBit(ix, true), "not already set");
                Assert.AreEqual(ix, toTest.GetLastSetIndex(), "current is max");
            }
    
            Assert.AreEqual(dataSize, toTest.GetLastSetIndex(), "last is max");
            
            int windowOfValidData = RoundWindow(dataSize, window);
            int i=dataSize;
            for (; i >= dataSize -windowOfValidData; i--)
            {
                Assert.IsTrue(toTest.SetBit(i, true), "was already set, id=" + i);
            }

            Assert.AreEqual(dataSize, toTest.GetLastSetIndex(), "last is still max");
            
            for (; i >= 0; i--)
            {
                Assert.IsTrue(!toTest.SetBit(i, true), "was not already set, id=" + i);
            }

            for (int j= dataSize +1; j<=(2*dataSize); j++)
            {
                Assert.IsTrue(!toTest.SetBit(j, true), "not already set: id=" + j);
            }
            
            Assert.AreEqual(2*dataSize, toTest.GetLastSetIndex(), "last still max*2");
        }

        [Test]
        public void TestSetUnsetAroundWindow()
        {
            DoTestSetUnSetAroundWindow(500, 2000);
            DoTestSetUnSetAroundWindow(512, 2000);
            DoTestSetUnSetAroundWindow(128, 512);
        }

        private void DoTestSetUnSetAroundWindow(int dataSize, int window)
        {
            BitArrayBin toTest = new BitArrayBin(window);
            
            for (int i=0; i <=dataSize; i++)
            {
                Assert.IsTrue(!toTest.SetBit(i, true), "not already set");
            }

            int windowOfValidData = RoundWindow(dataSize, window);
            for (int i=dataSize; i >= 0 && i >=dataSize -windowOfValidData; i--)
            {
                Assert.IsTrue(toTest.SetBit(i, false), "was already set, id=" + i);
            }
    
            for (int i=0; i <=dataSize; i++)
            {
                Assert.IsTrue(!toTest.SetBit(i, false), "not already set, id:" + i);
            }

            for (int j= 2*dataSize; j< 4*dataSize; j++)
            {
                Assert.IsTrue(!toTest.SetBit(j, true), "not already set: id=" + j);
            }
        }
        
        [Test]
        public void TestSetAroundLongSizeMultiplier()
        {
            int window = 512;
            int dataSize = 1000;
            for (int muliplier=1; muliplier < 8; muliplier++)
            {
                for (int i=0; i < dataSize; i++)
                {
                    BitArrayBin toTest = new BitArrayBin(window);

                    int instance = i + muliplier * BitArray.LONG_SIZE;
                    Assert.IsTrue(!toTest.SetBit(instance, true), "not already set: id=" + instance);
                    Assert.IsTrue(!toTest.SetBit(i, true), "not already set: id=" + i);
                    Assert.AreEqual(instance, toTest.GetLastSetIndex(), "max set correct");
                }
            }
        }
        
        [Test]
        public void TestLargeGapInData()
        {
            DoTestLargeGapInData(128);
            DoTestLargeGapInData(500);
        }
        
        private void DoTestLargeGapInData(int window)
        {
            BitArrayBin toTest = new BitArrayBin(window);
            
            int instance = BitArray.LONG_SIZE;
            Assert.IsTrue(!toTest.SetBit(instance, true), "not already set: id=" + instance);

            instance = 12 * BitArray.LONG_SIZE;
            Assert.IsTrue(!toTest.SetBit(instance, true), "not already set: id=" + instance);

            instance = 9 * BitArray.LONG_SIZE;
            Assert.IsTrue(!toTest.SetBit(instance, true), "not already set: id=" + instance);
        }

        [Test]
        public void TestLastSeq()
        {
            BitArrayBin toTest = new BitArrayBin(512);
            Assert.AreEqual(-1, toTest.GetLastSetIndex(), "last not set");

            toTest.SetBit(1, true);
            Assert.AreEqual(1, toTest.GetLastSetIndex(), "last not correct");

            toTest.SetBit(64, true);
            Assert.AreEqual(64, toTest.GetLastSetIndex(), "last not correct");

            toTest.SetBit(68, true);
            Assert.AreEqual(68, toTest.GetLastSetIndex(), "last not correct");
        }
    
        // window moves in increments of BitArrayBin.LONG_SIZE.
        // valid data window on low end can be larger than window
        private int RoundWindow(int dataSetEnd, int windowSize)
        {
            int validData = dataSetEnd - windowSize;
            int validDataBin = validData / BitArray.LONG_SIZE;
            validDataBin += (windowSize % BitArray.LONG_SIZE > 0? 1:0);
            int startOfValid = validDataBin * BitArray.LONG_SIZE;
    
            return dataSetEnd - startOfValid;
        }

    }
}

