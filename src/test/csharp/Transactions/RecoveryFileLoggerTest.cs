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
using System.Collections.Generic;

using NUnit.Framework;

using Apache.NMS.ActiveMQ.Transactions;
using Apache.NMS.ActiveMQ.Commands;

namespace Apache.NMS.ActiveMQ.Transactions.Test
{
    [TestFixture]
    public class RecoveryFileLoggerTest
    {
        private Guid rmId;
        private string nonExistantPath;
        private string autoCreatePath;
        private string nonDefaultLogLocation;

        [SetUp]
        public void SetUp()
        {
            this.rmId = Guid.NewGuid();
            this.nonExistantPath = Directory.GetCurrentDirectory() + Path.DirectorySeparatorChar + Guid.NewGuid();
            this.nonDefaultLogLocation = Directory.GetCurrentDirectory() + Path.DirectorySeparatorChar + Guid.NewGuid();
            this.autoCreatePath = Directory.GetCurrentDirectory() + Path.DirectorySeparatorChar + Guid.NewGuid();

            Directory.CreateDirectory(nonDefaultLogLocation);
        }

        [TearDown]
        public void TearDown()
        {
            if(Directory.Exists(autoCreatePath))
            {
                Directory.Delete(autoCreatePath);
            }

            Directory.Delete(nonDefaultLogLocation, true);
        }

        [Test]
        public void TestInitWithNoLocationSet()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Initialize(rmId.ToString());

            Assert.AreEqual(Directory.GetCurrentDirectory(), logger.Location);
        }

        [Test]
        public void TestInitWithNonDefaultLocationSet()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(rmId.ToString());

            Assert.AreEqual(nonDefaultLogLocation, logger.Location);
        }

        [Test]
        public void TestInitWithAutoCreateLocation()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            Assert.IsFalse(Directory.Exists(autoCreatePath));

            logger.AutoCreateLocation = true;
            logger.Location = autoCreatePath;
            logger.Initialize(rmId.ToString());

            Assert.IsTrue(Directory.Exists(autoCreatePath));
            Assert.AreEqual(autoCreatePath, logger.Location);
        }


        [Test]
        public void TestInitWithLocationSetToBadPath()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Location = this.nonExistantPath;

            try
            {
                logger.Initialize(rmId.ToString());
                Assert.Fail("Should have detected an invalid dir and thrown an exception");
            }
            catch
            {
            }
        }

        [Test]
        public void TestNothingToRecover()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(rmId.ToString());

            Assert.IsTrue(logger.GetRecoverables().Length == 0);
        }

        [Test]
        public void TestLogTransactionRecord()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            byte[] globalId = new byte[32];
            byte[] branchQ = new byte[32];
            byte[] recoveryData = new byte[256];

            Random gen = new Random();

            gen.NextBytes(globalId);
            gen.NextBytes(branchQ);
            gen.NextBytes(recoveryData);

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(rmId.ToString());

            XATransactionId xid = new XATransactionId();
            xid.GlobalTransactionId = globalId;
            xid.BranchQualifier = branchQ;

            logger.LogRecoveryInfo(xid, recoveryData);

            Assert.IsTrue(File.Exists(logger.Location + Path.DirectorySeparatorChar + rmId.ToString() + ".bin"),
                          "Recovery File was not created");
        }

        [Test]
        public void TestRecoverLoggedRecord()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            byte[] globalId = new byte[32];
            byte[] branchQ = new byte[32];
            byte[] recoveryData = new byte[256];

            Random gen = new Random();

            gen.NextBytes(globalId);
            gen.NextBytes(branchQ);
            gen.NextBytes(recoveryData);

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(rmId.ToString());

            XATransactionId xid = new XATransactionId();
            xid.GlobalTransactionId = globalId;
            xid.BranchQualifier = branchQ;

            logger.LogRecoveryInfo(xid, recoveryData);

            Assert.IsTrue(File.Exists(logger.Location + Path.DirectorySeparatorChar + rmId.ToString() + ".bin"),
                          "Recovery File was not created");
            Assert.IsTrue(logger.GetRecoverables().Length == 1,
                          "Did not recover the logged record.");

            KeyValuePair<XATransactionId, byte[]>[] records = logger.GetRecoverables();
            Assert.AreEqual(1, records.Length);

            Assert.AreEqual(globalId, records[0].Key.GlobalTransactionId, "Incorrect Global TX Id returned");
            Assert.AreEqual(branchQ, records[0].Key.BranchQualifier, "Incorrect Branch Qualifier returned");
            Assert.AreEqual(recoveryData, records[0].Value, "Incorrect Recovery Information returned");
        }

        [Test]
        public void TestPurgeTransactionRecord()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            byte[] globalId = new byte[32];
            byte[] branchQ = new byte[32];
            byte[] recoveryData = new byte[256];

            Random gen = new Random();

            gen.NextBytes(globalId);
            gen.NextBytes(branchQ);
            gen.NextBytes(recoveryData);

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(rmId.ToString());

            XATransactionId xid = new XATransactionId();
            xid.GlobalTransactionId = globalId;
            xid.BranchQualifier = branchQ;

            logger.LogRecoveryInfo(xid, recoveryData);

            Assert.IsTrue(File.Exists(logger.Location + Path.DirectorySeparatorChar + rmId.ToString() + ".bin"),
                          "Recovery File was not created");

            logger.Purge();

            Assert.IsFalse(File.Exists(logger.Location + Path.DirectorySeparatorChar + rmId.ToString() + ".bin"),
                          "Recovery File was not created");
        }

    }
}

