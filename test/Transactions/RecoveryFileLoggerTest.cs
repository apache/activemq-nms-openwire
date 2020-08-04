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

namespace Apache.NMS.ActiveMQ.Test.Transactions
{
    using System.Threading;

    [TestFixture]
    public class RecoveryFileLoggerTest
    {
        private string resourceManagerId;
        private string nonExistantPath;
        private string autoCreatePath;
        private string nonDefaultLogLocation;

        [SetUp]
        public void SetUp()
        {
            this.resourceManagerId = Guid.NewGuid().ToString();
            this.nonExistantPath = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString());
			this.nonDefaultLogLocation = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString());
			this.autoCreatePath = Path.Combine(Directory.GetCurrentDirectory(), Guid.NewGuid().ToString());

            Directory.CreateDirectory(nonDefaultLogLocation);
        }

        [TearDown]
        public void TearDown()
        {
            SafeDeleteDirectory(autoCreatePath, 1000);
            SafeDeleteDirectory(nonDefaultLogLocation, 1000);
        }

        [Test]
        public void TestInitWithNoLocationSet()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Initialize(this.resourceManagerId);

            Assert.AreEqual(Directory.GetCurrentDirectory(), logger.Location);
        }

        [Test]
        public void TestInitWithNonDefaultLocationSet()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(this.resourceManagerId);

            Assert.AreEqual(nonDefaultLogLocation, logger.Location);
        }

        [Test]
        public void TestInitWithAutoCreateLocation()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            Assert.IsFalse(Directory.Exists(autoCreatePath));

            logger.AutoCreateLocation = true;
            logger.Location = autoCreatePath;
            logger.Initialize(this.resourceManagerId);

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
                logger.Initialize(this.resourceManagerId);
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
            logger.Initialize(this.resourceManagerId);

            Assert.IsTrue(logger.GetRecoverables().Length == 0);
        }

        [Test]
        public void TestLogTransactionRecord()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(this.resourceManagerId);

            TransactionData transactionData = new TransactionData();
            logger.LogRecoveryInfo(transactionData.Transaction, transactionData.RecoveryData);

            Assert.IsTrue(File.Exists(this.GetFilename(logger, transactionData)),
                "Recovery File was not created");
        }

        [Test]
        public void TestRecoverLoggedRecord()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Location = this.nonDefaultLogLocation;
            logger.Initialize(this.resourceManagerId);

            TransactionData transactionData01 = new TransactionData();
            logger.LogRecoveryInfo(transactionData01.Transaction, transactionData01.RecoveryData);
            TransactionData transactionData02 = new TransactionData();
            logger.LogRecoveryInfo(transactionData02.Transaction, transactionData02.RecoveryData);

            Assert.IsTrue(File.Exists(this.GetFilename(logger, transactionData01)), "Recovery File was not created");
            Assert.IsTrue(File.Exists(this.GetFilename(logger, transactionData02)), "Recovery File was not created");
            Assert.AreEqual(2, logger.GetRecoverables().Length, "Did not recover the logged record.");

            KeyValuePair<XATransactionId, byte[]>[] records = logger.GetRecoverables();
            Assert.AreEqual(2, records.Length);

            foreach (var keyValuePair in records)
            {
                if (BitConverter.ToString(keyValuePair.Key.GlobalTransactionId) == BitConverter.ToString(transactionData01.Transaction.GlobalTransactionId))
                {
                    Assert.AreEqual(transactionData01.GlobalId, keyValuePair.Key.GlobalTransactionId, "Incorrect Global TX Id returned");
                    Assert.AreEqual(transactionData01.BranchQ, keyValuePair.Key.BranchQualifier, "Incorrect Branch Qualifier returned");
                    Assert.AreEqual(transactionData01.RecoveryData, keyValuePair.Value, "Incorrect Recovery Information returned");
                }
                else if (BitConverter.ToString(keyValuePair.Key.GlobalTransactionId) == BitConverter.ToString(transactionData02.Transaction.GlobalTransactionId))
                {
                    Assert.AreEqual(transactionData02.GlobalId, keyValuePair.Key.GlobalTransactionId, "Incorrect Global TX Id returned");
                    Assert.AreEqual(transactionData02.BranchQ, keyValuePair.Key.BranchQualifier, "Incorrect Branch Qualifier returned");
                    Assert.AreEqual(transactionData02.RecoveryData, keyValuePair.Value, "Incorrect Recovery Information returned");
                }
                else
                {
                    Assert.Fail("Transaction not found.");
                }
            }
        }

        [Test]
        public void TestLogRecovered()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(this.resourceManagerId);

            TransactionData transactionData = new TransactionData();
            logger.LogRecoveryInfo(transactionData.Transaction, transactionData.RecoveryData);

            Assert.IsTrue(File.Exists(this.GetFilename(logger, transactionData)), "Recovery File was not created");

            logger.LogRecovered(transactionData.Transaction);

            this.AssertFileIsDeleted(this.GetFilename(logger, transactionData), 1000);
        }

        [Test]
        public void TestPurgeTransactionRecord()
        {
            RecoveryFileLogger logger = new RecoveryFileLogger();

            logger.Location = nonDefaultLogLocation;
            logger.Initialize(this.resourceManagerId.ToString());

            TransactionData transactionData01 = new TransactionData();
            logger.LogRecoveryInfo(transactionData01.Transaction, transactionData01.RecoveryData);
            TransactionData transactionData02 = new TransactionData();
            logger.LogRecoveryInfo(transactionData02.Transaction, transactionData02.RecoveryData);

            Assert.IsTrue(File.Exists(this.GetFilename(logger, transactionData01)), "Recovery File was not created");
            Assert.IsTrue(File.Exists(this.GetFilename(logger, transactionData02)), "Recovery File was not created");

            logger.Purge();

            this.AssertFileIsDeleted(this.GetFilename(logger, transactionData01), 1000);
            this.AssertFileIsDeleted(this.GetFilename(logger, transactionData02), 1000);
        }

        private string GetFilename(RecoveryFileLogger logger, TransactionData transactionData)
        {
            return string.Format(
                "{0}{1}{2}_{3}.bin",
                logger.Location,
                Path.DirectorySeparatorChar,
                this.resourceManagerId.ToString(),
                BitConverter.ToString(transactionData.Transaction.GlobalTransactionId).Replace("-", string.Empty));
        }

        private void AssertFileIsDeleted(string filename, int timeout)
        {
            var expiration = DateTime.Now.Add(TimeSpan.FromMilliseconds(timeout));
            while (File.Exists(filename))
            {
                if (expiration < DateTime.Now)
                {
                    Assert.Fail("Recovery File was not removed");
                }

                Thread.Sleep(5);
            }
        }

        private void SafeDeleteDirectory(string directory, int timeout)
        {
            var expiration = DateTime.Now.Add(TimeSpan.FromMilliseconds(timeout));
            while (true)
            {
                if (!Directory.Exists(directory))
                {
                    return;
                }

                try
                {
                    Directory.Delete(directory, true);
                    return;
                }
                catch (Exception)
                {
                }

                if (expiration < DateTime.Now)
                {
                    return;
                }

                Thread.Sleep(5);
            }
        }

        private class TransactionData
        {
            private static readonly Random Random = new Random();

            private readonly XATransactionId xid;

            private readonly byte[] recoveryData = new byte[256];
            private readonly byte[] globalId = new byte[32];
            private readonly byte[] branchQ = new byte[32];

            public TransactionData()
            {
                Random.NextBytes(this.globalId);
                Random.NextBytes(this.branchQ);
                Random.NextBytes(this.recoveryData);

                this.xid = new XATransactionId();
                this.xid.GlobalTransactionId = this.globalId;
                this.xid.BranchQualifier = this.branchQ;
            }

            public XATransactionId Transaction
            {
                get { return this.xid; }
            }

            public byte[] RecoveryData
            {
                get { return this.recoveryData; }
            }

            public byte[] GlobalId
            {
                get { return this.globalId; }
            }

            public byte[] BranchQ
            {
                get { return this.branchQ; }
            }
        }
    }
}

