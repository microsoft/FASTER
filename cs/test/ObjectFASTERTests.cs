// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using System.IO;
using NUnit.Framework;

namespace FASTER.test
{

    [TestFixture]
    internal class ObjectFASTERTests
    {
        private IManagedFasterKV<MyKey, MyValue, MyInput, MyOutput, MyContext> fht;
        private IDevice log, objlog;
        
        [SetUp]
        public void Setup()
        {
            log = FasterFactory.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);
            objlog = FasterFactory.CreateObjectLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);

            fht = FasterFactory.Create
                <MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions>
                (indexSizeBuckets: 128, logDevice: log, objectLogDevice: objlog, functions: new MyFunctions(), 
                LogMutableFraction: 0.1, LogPageSizeBits: 9, LogTotalSizeBytes: 512*16
                );
            fht.StartSession();
        }

        [TearDown]
        public void TearDown()
        {
            fht.StopSession();
            fht = null;
            log.Close();
        }



        [Test]
        public void ObjectInMemWriteRead()
        {
            var key1 = new MyKey { key = 9999999 };
            var value = new MyValue { value = 23 };

            MyOutput output = new MyOutput();
            fht.Upsert(key1, value, null, 0);
            fht.Read(key1, null, ref output, null, 0);

            Assert.IsTrue(output.value.value == value.value);
        }

        [Test]
        public void ObjectInMemWriteRead2()
        {
            var key1 = new MyKey { key = 8999998 };
            var input1 = new MyInput { value = 23 };

            fht.RMW(key1, input1, null, 0);

            var key2 = new MyKey { key = 8999999 };
            var input2 = new MyInput { value = 24 };
            fht.RMW(key2, input2, null, 0);

            MyOutput output = new MyOutput();
            fht.Read(key1, null, ref output, null, 0);

            Assert.IsTrue(output.value.value == input1.value);

            fht.Read(key2, null, ref output, null, 0);
            Assert.IsTrue(output.value.value == input2.value);

        }


        [Test]
        public void ObjectDiskWriteRead()
        {
            for (int i = 0; i < 2000; i++)
                fht.Upsert(new MyKey { key = i }, new MyValue { value = i }, default(MyContext), 0);

            MyOutput g1 = new MyOutput();
            var status = fht.Read(new MyKey { key = 23 }, new MyInput(), ref g1, new MyContext(), 0);

            if (status == Status.PENDING)
            {
                fht.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
            }

            Assert.IsTrue(g1.value.value == 23);

            status = fht.Read(new MyKey { key = 99999 }, new MyInput(), ref g1, new MyContext(), 0);

            if (status == Status.PENDING)
            {
                fht.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.NOTFOUND);
            }


        }
    }
}
